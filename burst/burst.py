"""Concurrent correctness check for a running seat reservation service.

Example:
    python burst/burst.py http://localhost:8000
    python burst/burst.py https://example.onrender.com --hot-seat 100 --limit-attempts 20

ADMIN_TOKEN must match the target service. It defaults to the local Compose
dev token and is never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from uuid import uuid4

import httpx

_DEFAULT_ADMIN_TOKEN = "dev-admin-token"
_DEFAULT_HOT_SEAT = 20
_DEFAULT_LIMIT_ATTEMPTS = 10
_PER_USER_LIMIT = 4


@dataclass(frozen=True)
class _Attempt:
    status_code: int
    error: str | None
    reservation_id: str | None
    replay: bool


@dataclass(frozen=True)
class Report:
    hot_confirmed: int
    hot_seat_taken: int
    hot_5xx: int
    hot_other: int
    hot_requests: int
    limit_confirmed: int
    limit_declined: int
    limit_5xx: int
    limit_other: int
    limit_attempts: int
    per_user_limit: int
    replay_ok: bool
    mismatch_ok: bool
    total: int
    available: int
    held: int
    confirmed: int
    invariant_ok: bool
    counts_match: bool

    @property
    def ok(self) -> bool:
        expected_limit_confirmed = min(self.limit_attempts, self.per_user_limit)
        expected_limit_declined = max(self.limit_attempts - self.per_user_limit, 0)
        return (
            self.hot_requests >= 1
            and self.hot_confirmed == 1
            and self.hot_seat_taken == self.hot_requests - 1
            and self.hot_5xx == 0
            and self.hot_other == 0
            and self.limit_confirmed == expected_limit_confirmed
            and self.limit_confirmed <= self.per_user_limit
            and self.limit_declined == expected_limit_declined
            and self.limit_5xx == 0
            and self.limit_other == 0
            and self.replay_ok
            and self.mismatch_ok
            and self.invariant_ok
            and self.counts_match
        )


class BurstError(Exception):
    """The target service could not be prepared for the burst."""


def render(report: Report) -> str:
    return "\n".join(
        [
            "Hot seat:",
            f"  confirmed: {report.hot_confirmed}",
            f"  seat_taken: {report.hot_seat_taken}",
            f"  5xx: {report.hot_5xx}",
            "",
            "Per-user limit:",
            f"  confirmed: {report.limit_confirmed}",
            f"  per_user_limit: {report.limit_declined}",
            f"  5xx: {report.limit_5xx}",
            "",
            "Idempotency:",
            f"  replay_ok: {_flag(report.replay_ok)}",
            f"  mismatch_ok: {_flag(report.mismatch_ok)}",
            "",
            "Reconciliation:",
            f"  total: {report.total}",
            f"  available: {report.available}",
            f"  held: {report.held}",
            f"  confirmed: {report.confirmed}",
            f"  invariant_ok: {_flag(report.invariant_ok)}",
        ]
    )


async def run_burst(
    client: httpx.AsyncClient,
    *,
    admin_token: str,
    hot_seat: int,
    limit_attempts: int,
    per_user_limit: int = _PER_USER_LIMIT,
) -> Report:
    if hot_seat < 1 or limit_attempts < 1 or per_user_limit < 1:
        raise BurstError("hot-seat, limit-attempts, and per-user-limit must be positive")

    run_id = uuid4().hex[:8]
    hot_label = "H1"
    limit_labels = [f"P{index}" for index in range(limit_attempts)]
    replay_label = "R1"
    mismatch_label = "R2"
    seats = [hot_label, *limit_labels, replay_label, mismatch_label]
    show_id = await _create_show(client, admin_token, run_id, seats)

    hot_users = [f"hot{run_id}{index}" for index in range(hot_seat)]
    limit_user = f"limit{run_id}"
    replay_user = f"replay{run_id}"
    tokens = await asyncio.gather(
        *[_token(client, user_id) for user_id in [*hot_users, limit_user, replay_user]]
    )
    hot_tokens = tokens[:hot_seat]
    limit_token = tokens[hot_seat]
    replay_token = tokens[hot_seat + 1]

    hot_attempts = await asyncio.gather(
        *[
            _reserve(client, show_id, token, [hot_label], f"hot-{run_id}-{index}")
            for index, token in enumerate(hot_tokens)
        ]
    )
    limit_results = await asyncio.gather(
        *[
            _reserve(client, show_id, limit_token, [label], f"limit-{run_id}-{label}")
            for label in limit_labels
        ]
    )

    replay_key = f"replay-{run_id}"
    first = await _reserve(client, show_id, replay_token, [replay_label], replay_key)
    replay = await _reserve(client, show_id, replay_token, [replay_label], replay_key)
    mismatch = await _reserve(
        client,
        show_id,
        replay_token,
        [mismatch_label],
        replay_key,
    )
    show = await _show(client, show_id)
    seat_status = {
        seat["seat_label"]: seat["status"]
        for seat in show["seats"]
        if isinstance(seat, dict) and isinstance(seat.get("seat_label"), str)
    }
    counts = show["counts"]
    total = int(counts["total"])
    available = int(counts["available"])
    held = int(counts["held"])
    confirmed = int(counts["confirmed"])

    hot_confirmed, hot_taken, hot_5xx, hot_other = _tally(hot_attempts, "seat_taken")
    limit_confirmed, limit_declined, limit_5xx, limit_other = _tally(
        limit_results,
        "per_user_limit",
    )
    idem_confirmed = 1 if first.status_code == 201 else 0
    replay_ok = (
        first.status_code == 201
        and replay.status_code == 200
        and replay.replay
        and replay.reservation_id is not None
        and replay.reservation_id == first.reservation_id
        and seat_status.get(replay_label) == "confirmed"
    )
    mismatch_ok = (
        mismatch.status_code == 409
        and mismatch.error == "idempotency_mismatch"
        and seat_status.get(mismatch_label) == "available"
    )
    return Report(
        hot_confirmed=hot_confirmed,
        hot_seat_taken=hot_taken,
        hot_5xx=hot_5xx,
        hot_other=hot_other,
        hot_requests=len(hot_attempts),
        limit_confirmed=limit_confirmed,
        limit_declined=limit_declined,
        limit_5xx=limit_5xx,
        limit_other=limit_other,
        limit_attempts=len(limit_results),
        per_user_limit=per_user_limit,
        replay_ok=replay_ok,
        mismatch_ok=mismatch_ok,
        total=total,
        available=available,
        held=held,
        confirmed=confirmed,
        invariant_ok=available + held + confirmed == total,
        counts_match=(
            held == 0
            and total == len(seats)
            and confirmed == hot_confirmed + limit_confirmed + idem_confirmed
        ),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    base_url = args.base_url.rstrip("/")
    admin_token = os.environ.get("ADMIN_TOKEN", _DEFAULT_ADMIN_TOKEN)
    timeout = httpx.Timeout(args.timeout)
    limits = httpx.Limits(
        max_connections=max(args.hot_seat, args.limit_attempts) + 10,
        max_keepalive_connections=20,
    )

    async def _execute() -> Report:
        async with httpx.AsyncClient(base_url=base_url, timeout=timeout, limits=limits) as client:
            return await run_burst(
                client,
                admin_token=admin_token,
                hot_seat=args.hot_seat,
                limit_attempts=args.limit_attempts,
                per_user_limit=args.per_user_limit,
            )

    try:
        report = asyncio.run(_execute())
    except BurstError as exc:
        print(f"setup failed: {exc}", file=sys.stderr)
        return 1
    print(render(report))
    return 0 if report.ok else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Burst-check a seat reservation service.")
    parser.add_argument("base_url", help="Service origin, for example http://localhost:8000")
    parser.add_argument("--hot-seat", type=int, default=_DEFAULT_HOT_SEAT)
    parser.add_argument("--limit-attempts", type=int, default=_DEFAULT_LIMIT_ATTEMPTS)
    parser.add_argument("--per-user-limit", type=int, default=_PER_USER_LIMIT)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _tally(attempts: list[_Attempt], decline: str) -> tuple[int, int, int, int]:
    confirmed = sum(1 for attempt in attempts if attempt.status_code == 201)
    declined = sum(
        1 for attempt in attempts if attempt.status_code == 409 and attempt.error == decline
    )
    server_errors = sum(1 for attempt in attempts if attempt.status_code >= 500)
    other = len(attempts) - confirmed - declined - server_errors
    return confirmed, declined, server_errors, other


async def _create_show(
    client: httpx.AsyncClient,
    admin_token: str,
    run_id: str,
    seats: list[str],
) -> str:
    response = await client.post(
        "/shows",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"name": f"burst-{run_id}", "seats": seats, "price_paise": 1000},
    )
    body = _object(response)
    if response.status_code != 201 or not isinstance(body.get("id"), str):
        reason = body.get("error") if isinstance(body.get("error"), str) else "unexpected response"
        raise BurstError(f"create show returned {response.status_code} {reason}")
    return body["id"]


async def _token(client: httpx.AsyncClient, user_id: str) -> str:
    response = await client.post("/auth/token", json={"user_id": user_id})
    body = _object(response)
    token = body.get("token")
    if response.status_code != 201 or not isinstance(token, str):
        reason = body.get("error") if isinstance(body.get("error"), str) else "unexpected response"
        raise BurstError(f"create token returned {response.status_code} {reason}")
    return token


async def _reserve(
    client: httpx.AsyncClient,
    show_id: str,
    token: str,
    seats: list[str],
    key: str,
) -> _Attempt:
    try:
        response = await client.post(
            f"/shows/{show_id}/reserve",
            headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key},
            json={"seats": seats},
        )
    except httpx.HTTPError:
        return _Attempt(status_code=503, error=None, reservation_id=None, replay=False)
    body = _object(response)
    error = body.get("error")
    reservation_id = body.get("reservation_id")
    return _Attempt(
        status_code=response.status_code,
        error=error if isinstance(error, str) else None,
        reservation_id=reservation_id if isinstance(reservation_id, str) else None,
        replay=response.headers.get("idempotent-replay") == "true",
    )


async def _show(client: httpx.AsyncClient, show_id: str) -> dict:
    response = await client.get(f"/shows/{show_id}")
    body = _object(response)
    counts = body.get("counts")
    seats = body.get("seats")
    if response.status_code != 200 or not isinstance(counts, dict) or not isinstance(seats, list):
        raise BurstError(f"read show returned {response.status_code}")
    return body


def _object(response: httpx.Response) -> dict:
    try:
        body = response.json()
    except ValueError:
        return {}
    if not isinstance(body, dict):
        return {}
    return body


if __name__ == "__main__":
    sys.exit(main())
