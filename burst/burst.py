"""Concurrent correctness check for a running seat reservation service.

The default command stays a small local check:

    python burst/burst.py http://localhost:8000

Larger checks raise the counters without opening one socket per request:

    python burst/burst.py http://localhost:8000 --hot-seat 500 --concurrency 100
    python burst/burst.py http://localhost:8000 --requests 2000 --concurrency 200
    python burst/burst.py http://localhost:8000 --requests 5000 --concurrency 300
    python burst/burst.py http://localhost:8000 --requests 20000 --concurrency 500 --setup-concurrency 25

ADMIN_TOKEN must match the target service. It defaults to the local Compose
dev token and is never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from uuid import uuid4

import httpx

_DEFAULT_ADMIN_TOKEN = "dev-admin-token"
_DEFAULT_HOT_SEAT = 20
_DEFAULT_LIMIT_ATTEMPTS = 10
_DEFAULT_CONCURRENCY = 50
_DEFAULT_SETUP_CONCURRENCY = 25
_PER_USER_LIMIT = 4
_MIXED_MINIMUM = 12


@dataclass(frozen=True)
class Attempt:
    kind: str
    status_code: int
    error: str | None
    reservation_id: str | None
    replay: bool
    seats: tuple[str, ...] = ()
    user_id: str = ""
    key: str = ""
    exception_type: str = ""


@dataclass(frozen=True)
class Job:
    user_id: str
    seats: tuple[str, ...]
    key: str
    kind: str
    hot_seat: str | None = None
    untouched_seat: str | None = None


@dataclass(frozen=True)
class Report:
    hot_attempts: int
    hot_confirmed: int
    hot_seat_taken: int
    hot_5xx: int
    hot_transport_errors: int
    hot_timeouts: int
    hot_other: int
    limit_attempts: int
    limit_confirmed: int
    limit_declined: int
    limit_5xx: int
    limit_transport_errors: int
    limit_timeouts: int
    limit_other: int
    per_user_limit: int
    replay_ok: bool
    mismatch_ok: bool
    mixed_requests: int
    mixed_concurrency: int
    mixed_confirmed: int
    mixed_replay: int
    mixed_seat_taken: int
    mixed_per_user_limit: int
    mixed_mismatch: int
    mixed_other_4xx: int
    mixed_5xx: int
    mixed_timeouts: int
    mixed_transport_errors: int
    mixed_checks_ok: bool
    elapsed_seconds: float
    requests_per_second: float
    during_samples: int
    reconciliation_failures: int
    total: int
    available: int
    held: int
    confirmed: int
    invariant_ok: bool
    counts_match: bool
    metrics_available: bool
    observed_confirmed: int
    observed_seat_taken: int
    observed_per_user_limit: int
    observed_replay: int
    metric_confirmed_delta: float
    metric_seat_taken_delta: float
    metric_per_user_limit_delta: float
    metric_replay_delta: float
    transport_error_types: dict[str, int]

    @property
    def ok(self) -> bool:
        expected_confirmed = min(self.limit_attempts, self.per_user_limit)
        expected_declined = max(self.limit_attempts - self.per_user_limit, 0)
        hot_ok = (
            self.hot_attempts >= 1
            and self.hot_confirmed == 1
            and self.hot_seat_taken == self.hot_attempts - 1
            and self.hot_5xx == 0
            and self.hot_transport_errors == 0
            and self.hot_timeouts == 0
            and self.hot_other == 0
        )
        limit_ok = (
            self.limit_attempts >= 1
            and self.limit_confirmed == expected_confirmed
            and self.limit_confirmed <= self.per_user_limit
            and self.limit_declined == expected_declined
            and self.limit_5xx == 0
            and self.limit_transport_errors == 0
            and self.limit_timeouts == 0
            and self.limit_other == 0
        )
        mixed_ok = self.mixed_requests == 0 or (
            self.mixed_checks_ok
            and self.mixed_5xx == 0
            and self.mixed_timeouts == 0
            and self.mixed_transport_errors == 0
            and self.mixed_other_4xx == 0
        )
        return (
            hot_ok
            and limit_ok
            and self.replay_ok
            and self.mismatch_ok
            and self.counts_match
            and self.invariant_ok
            and self.reconciliation_failures == 0
            and self.during_samples >= 1
            and mixed_ok
        )


class BurstError(Exception):
    """The target service could not be prepared for the burst."""


def classify(
    status_code: int,
    error: str | None,
    replay: bool,
    *,
    timeout: bool = False,
    transport: bool = False,
) -> str:
    if timeout:
        return "timeout"
    if transport:
        return "transport"
    if status_code == 201:
        return "confirmed"
    if status_code == 200 and replay:
        return "replay"
    if status_code == 409 and error == "seat_taken":
        return "seat_taken"
    if status_code == 409 and error == "per_user_limit":
        return "per_user_limit"
    if status_code == 409 and error == "idempotency_mismatch":
        return "idempotency_mismatch"
    if status_code >= 500:
        return "server_error"
    return "other_4xx"


def invariant(counts: dict) -> bool:
    try:
        total = int(counts["total"])
        available = int(counts["available"])
        held = int(counts["held"])
        confirmed = int(counts["confirmed"])
    except (KeyError, TypeError, ValueError):
        return False
    return available + held + confirmed == total


def plan_mixed(requests: int, run_id: str) -> tuple[list[str], list[Job], list[Job]]:
    if requests < _MIXED_MINIMUM:
        raise BurstError(f"--requests must be at least {_MIXED_MINIMUM}")
    replay_n = max(1, requests // 20)
    mismatch_n = max(1, requests // 20)
    if requests >= 80:
        limit_n = 10
    elif requests >= 20:
        limit_n = 6
    else:
        limit_n = 4
    multi_pairs = 1 if requests >= 30 else 0
    hot_n = max(4, requests // 5)
    normal_n = requests - (
        hot_n + limit_n + replay_n + mismatch_n + replay_n + mismatch_n + (2 * multi_pairs)
    )
    while normal_n < 0 and hot_n > 4:
        hot_n -= 1
        normal_n += 1
    if normal_n < 0:
        raise BurstError("--requests is too small for the mixed workload")

    hot_labels = [f"H{index}" for index in range(5)]
    wave1: list[Job] = []
    for index in range(hot_n):
        label = hot_labels[index % len(hot_labels)]
        wave1.append(
            Job(
                user_id=_user(run_id, "hot", index),
                seats=(label,),
                key=f"mix-hot-{run_id}-{index}",
                kind="hot",
                hot_seat=label,
            )
        )
    limit_user = _user(run_id, "cap", 0)
    for index in range(limit_n):
        wave1.append(
            Job(
                user_id=limit_user,
                seats=(f"L{index}",),
                key=f"mix-limit-{run_id}-{index}",
                kind="limit",
            )
        )
    for index in range(normal_n):
        wave1.append(
            Job(
                user_id=_user(run_id, "norm", index),
                seats=(f"N{index}",),
                key=f"mix-norm-{run_id}-{index}",
                kind="normal",
            )
        )
    for index in range(multi_pairs):
        left, shared, right = f"M{index}A", f"M{index}S", f"M{index}B"
        wave1.append(
            Job(
                user_id=_user(run_id, "multi", index * 2),
                seats=(left, shared),
                key=f"mix-multi-{run_id}-{index}-a",
                kind="multi",
            )
        )
        wave1.append(
            Job(
                user_id=_user(run_id, "multi", index * 2 + 1),
                seats=(shared, right),
                key=f"mix-multi-{run_id}-{index}-b",
                kind="multi",
            )
        )
    wave2: list[Job] = []
    for index in range(replay_n):
        label = f"R{index}"
        key = f"mix-replay-{run_id}-{index}"
        user_id = _user(run_id, "replay", index)
        wave1.append(Job(user_id=user_id, seats=(label,), key=key, kind="seed"))
        wave2.append(Job(user_id=user_id, seats=(label,), key=key, kind="replay"))
    for index in range(mismatch_n):
        booked = f"A{index}"
        other = f"B{index}"
        key = f"mix-mismatch-{run_id}-{index}"
        user_id = _user(run_id, "mismatch", index)
        wave1.append(Job(user_id=user_id, seats=(booked,), key=key, kind="seed"))
        wave2.append(
            Job(
                user_id=user_id,
                seats=(other,),
                key=key,
                kind="mismatch",
                untouched_seat=other,
            )
        )
    labels: list[str] = []
    seen: set[str] = set()
    for job in [*wave1, *wave2]:
        for label in job.seats:
            if label not in seen:
                seen.add(label)
                labels.append(label)
    if len(wave1) + len(wave2) != requests:
        raise BurstError("mixed plan did not use the requested attempt count")
    return labels, wave1, wave2


def render(report: Report) -> str:
    lines = [
        "Hot seat:",
        f"  attempts: {report.hot_attempts}",
        f"  confirmed: {report.hot_confirmed}",
        f"  seat_taken: {report.hot_seat_taken}",
        f"  5xx: {report.hot_5xx}",
        f"  transport_errors: {report.hot_transport_errors}",
        f"  passed: {_flag(_hot_passed(report))}",
        "",
        "Per-user limit:",
        f"  attempts: {report.limit_attempts}",
        f"  confirmed: {report.limit_confirmed}",
        f"  per_user_limit: {report.limit_declined}",
        f"  5xx: {report.limit_5xx}",
        f"  passed: {_flag(_limit_passed(report))}",
        "",
        "Idempotency:",
        f"  replay_ok: {_flag(report.replay_ok)}",
        f"  mismatch_ok: {_flag(report.mismatch_ok)}",
        "",
        "Mixed burst:",
        f"  requests: {report.mixed_requests}",
        f"  concurrency: {report.mixed_concurrency}",
        f"  confirmed: {report.mixed_confirmed}",
        f"  replay: {report.mixed_replay}",
        f"  seat_taken: {report.mixed_seat_taken}",
        f"  per_user_limit: {report.mixed_per_user_limit}",
        f"  idempotency_mismatch: {report.mixed_mismatch}",
        f"  other_4xx: {report.mixed_other_4xx}",
        f"  5xx: {report.mixed_5xx}",
        f"  timeouts: {report.mixed_timeouts}",
        f"  transport_errors: {report.mixed_transport_errors}",
        f"  elapsed_seconds: {report.elapsed_seconds:.3f}",
        f"  requests_per_second: {report.requests_per_second:.1f}",
        "",
        "Reconciliation:",
        f"  during_samples: {report.during_samples}",
        f"  failures: {report.reconciliation_failures}",
        f"  total: {report.total}",
        f"  available: {report.available}",
        f"  held: {report.held}",
        f"  confirmed: {report.confirmed}",
        f"  invariant_ok: {_flag(report.invariant_ok)}",
        "",
        "Metrics:",
        *_metric_lines(report),
        *_transport_type_lines(report.transport_error_types),
        "",
        "Overall:",
        "  PASS" if report.ok else "  FAIL",
    ]
    return "\n".join(lines)


async def map_limited(limit: int, items: Sequence, worker):
    """Run worker on items with a fixed pool of `limit` tasks, preserving order."""
    if limit < 1:
        raise BurstError("concurrency must be positive")
    indexed = list(enumerate(items))
    if not indexed:
        return []

    results: list = [None] * len(indexed)
    queue: asyncio.Queue = asyncio.Queue()
    for pair in indexed:
        queue.put_nowait(pair)

    async def run() -> None:
        while True:
            try:
                index, item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            results[index] = await worker(item)

    tasks = [asyncio.create_task(run()) for _ in range(min(limit, len(indexed)))]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        error = _first_task_error(done)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if error is not None:
            wrapped = _as_setup_error(error)
            if wrapped is error:
                raise error
            raise wrapped from error
        return results
    finally:
        leftover = [task for task in tasks if not task.done()]
        for task in leftover:
            task.cancel()
        if leftover:
            await asyncio.gather(*leftover, return_exceptions=True)


async def run_burst(
    client: httpx.AsyncClient,
    *,
    admin_token: str,
    hot_seat: int,
    limit_attempts: int,
    per_user_limit: int = _PER_USER_LIMIT,
    requests: int = 0,
    concurrency: int = _DEFAULT_CONCURRENCY,
    setup_concurrency: int = _DEFAULT_SETUP_CONCURRENCY,
) -> Report:
    if (
        hot_seat < 1
        or limit_attempts < 1
        or per_user_limit < 1
        or concurrency < 1
        or setup_concurrency < 1
    ):
        raise BurstError(
            "hot-seat, limit-attempts, per-user-limit, concurrency, and setup-concurrency must be positive"
        )
    if requests < 0:
        raise BurstError("--requests cannot be negative")

    before = await _metrics(client)
    quick = await _run_quick(
        client,
        admin_token=admin_token,
        hot_seat=hot_seat,
        limit_attempts=limit_attempts,
        per_user_limit=per_user_limit,
        concurrency=concurrency,
        setup_concurrency=setup_concurrency,
    )
    mixed = await _run_mixed(
        client,
        admin_token=admin_token,
        requests=requests,
        concurrency=concurrency,
        setup_concurrency=setup_concurrency,
        per_user_limit=per_user_limit,
    )
    after = await _metrics(client)
    observed = _count_kinds([*quick.attempts, *mixed.attempts])
    deltas = _deltas(before, after)
    watched = mixed if requests else quick
    return Report(
        hot_attempts=quick.hot_attempts,
        hot_confirmed=quick.hot_confirmed,
        hot_seat_taken=quick.hot_seat_taken,
        hot_5xx=quick.hot_5xx,
        hot_transport_errors=quick.hot_transport_errors,
        hot_timeouts=quick.hot_timeouts,
        hot_other=quick.hot_other,
        limit_attempts=quick.limit_attempts,
        limit_confirmed=quick.limit_confirmed,
        limit_declined=quick.limit_declined,
        limit_5xx=quick.limit_5xx,
        limit_transport_errors=quick.limit_transport_errors,
        limit_timeouts=quick.limit_timeouts,
        limit_other=quick.limit_other,
        per_user_limit=per_user_limit,
        replay_ok=quick.replay_ok,
        mismatch_ok=quick.mismatch_ok,
        mixed_requests=mixed.requests,
        mixed_concurrency=concurrency,
        mixed_confirmed=mixed.confirmed_requests,
        mixed_replay=mixed.replay,
        mixed_seat_taken=mixed.seat_taken,
        mixed_per_user_limit=mixed.per_user_limit,
        mixed_mismatch=mixed.mismatch,
        mixed_other_4xx=mixed.other_4xx,
        mixed_5xx=mixed.server_errors,
        mixed_timeouts=mixed.timeouts,
        mixed_transport_errors=mixed.transport_errors,
        mixed_checks_ok=mixed.checks_ok,
        elapsed_seconds=mixed.elapsed_seconds,
        requests_per_second=mixed.requests_per_second,
        during_samples=watched.during_samples,
        reconciliation_failures=watched.reconciliation_failures,
        total=watched.total,
        available=watched.available,
        held=watched.held,
        confirmed=watched.confirmed,
        invariant_ok=watched.invariant_ok,
        counts_match=quick.counts_match,
        metrics_available=before is not None and after is not None,
        observed_confirmed=observed["confirmed"],
        observed_seat_taken=observed["seat_taken"],
        observed_per_user_limit=observed["per_user_limit"],
        observed_replay=observed["replay"],
        metric_confirmed_delta=_delta(deltas, "confirmed"),
        metric_seat_taken_delta=_delta(deltas, "seat_taken"),
        metric_per_user_limit_delta=_delta(deltas, "per_user_limit"),
        metric_replay_delta=_delta(deltas, "replay"),
        transport_error_types=_transport_error_types([*quick.attempts, *mixed.attempts]),
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    base_url = args.base_url.rstrip("/")
    admin_token = os.environ.get("ADMIN_TOKEN", _DEFAULT_ADMIN_TOKEN)
    timeout = httpx.Timeout(connect=5.0, read=args.timeout, write=10.0, pool=args.timeout)
    limits = httpx.Limits(
        max_connections=max(args.concurrency, args.limit_attempts, args.setup_concurrency) + 8,
        max_keepalive_connections=min(max(args.concurrency, args.setup_concurrency), 100),
    )

    async def _execute() -> Report:
        async with httpx.AsyncClient(base_url=base_url, timeout=timeout, limits=limits) as client:
            return await run_burst(
                client,
                admin_token=admin_token,
                hot_seat=args.hot_seat,
                limit_attempts=args.limit_attempts,
                per_user_limit=args.per_user_limit,
                requests=args.requests,
                concurrency=args.concurrency,
                setup_concurrency=args.setup_concurrency,
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
    parser.add_argument("--requests", type=int, default=0, help="Mixed reservation attempts. Default 0.")
    parser.add_argument("--concurrency", type=int, default=_DEFAULT_CONCURRENCY)
    parser.add_argument("--setup-concurrency", type=int, default=_DEFAULT_SETUP_CONCURRENCY)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser


@dataclass
class _Quick:
    attempts: list[Attempt]
    hot_attempts: int
    hot_confirmed: int
    hot_seat_taken: int
    hot_5xx: int
    hot_transport_errors: int
    hot_timeouts: int
    hot_other: int
    limit_attempts: int
    limit_confirmed: int
    limit_declined: int
    limit_5xx: int
    limit_transport_errors: int
    limit_timeouts: int
    limit_other: int
    replay_ok: bool
    mismatch_ok: bool
    counts_match: bool
    during_samples: int
    reconciliation_failures: int
    total: int
    available: int
    held: int
    confirmed: int
    invariant_ok: bool


@dataclass
class _Mixed:
    attempts: list[Attempt]
    requests: int
    confirmed_requests: int
    replay: int
    seat_taken: int
    per_user_limit: int
    mismatch: int
    other_4xx: int
    server_errors: int
    timeouts: int
    transport_errors: int
    checks_ok: bool
    elapsed_seconds: float
    requests_per_second: float
    during_samples: int
    reconciliation_failures: int
    total: int
    available: int
    held: int
    confirmed: int
    invariant_ok: bool


async def _run_quick(
    client: httpx.AsyncClient,
    *,
    admin_token: str,
    hot_seat: int,
    limit_attempts: int,
    per_user_limit: int,
    concurrency: int,
    setup_concurrency: int,
) -> _Quick:
    run_id = uuid4().hex[:8]
    hot_label = "H1"
    limit_labels = [f"P{index}" for index in range(limit_attempts)]
    replay_label = "R1"
    mismatch_label = "R2"
    seats = [hot_label, *limit_labels, replay_label, mismatch_label]
    show_id = await _create_show(client, admin_token, f"quick-{run_id}", seats)
    user_ids = [f"hot{run_id}{index}" for index in range(hot_seat)]
    limit_user = f"limit{run_id}"
    replay_user = f"replay{run_id}"
    tokens = await _tokens(client, [*user_ids, limit_user, replay_user], setup_concurrency)
    hot_tokens = dict(zip(user_ids, tokens[:hot_seat], strict=True))
    limit_token = tokens[hot_seat]
    replay_token = tokens[hot_seat + 1]
    sampler = _Sampler(client, show_id)
    hot_jobs = [
        Job(user_id=user_id, seats=(hot_label,), key=f"hot-{run_id}-{index}", kind="hot")
        for index, user_id in enumerate(user_ids)
    ]
    hot_attempts = await _perform(client, show_id, hot_jobs, hot_tokens, concurrency, sampler)
    limit_jobs = [
        Job(user_id=limit_user, seats=(label,), key=f"limit-{run_id}-{label}", kind="limit")
        for label in limit_labels
    ]
    limit_attempts_done = await _perform(
        client,
        show_id,
        limit_jobs,
        {limit_user: limit_token},
        max(concurrency, len(limit_jobs)),
        sampler,
    )
    replay_key = f"replay-{run_id}"
    first = await _reserve(client, show_id, replay_user, replay_token, [replay_label], replay_key)
    replay = await _reserve(client, show_id, replay_user, replay_token, [replay_label], replay_key)
    mismatch = await _reserve(
        client,
        show_id,
        replay_user,
        replay_token,
        [mismatch_label],
        replay_key,
    )
    await sampler.probe()
    show = await _show(client, show_id)
    status = _seat_status(show)
    counts = _counts(show)
    hot_counts = _count_kinds(hot_attempts)
    limit_counts = _count_kinds(limit_attempts_done)
    idem_confirmed = 1 if first.kind == "confirmed" else 0
    return _Quick(
        attempts=[*hot_attempts, *limit_attempts_done, first, replay, mismatch],
        hot_attempts=len(hot_attempts),
        hot_confirmed=hot_counts["confirmed"],
        hot_seat_taken=hot_counts["seat_taken"],
        hot_5xx=hot_counts["server_error"],
        hot_transport_errors=hot_counts["transport"],
        hot_timeouts=hot_counts["timeout"],
        hot_other=hot_counts["other_4xx"] + hot_counts["replay"] + hot_counts["per_user_limit"] + hot_counts["idempotency_mismatch"],
        limit_attempts=len(limit_attempts_done),
        limit_confirmed=limit_counts["confirmed"],
        limit_declined=limit_counts["per_user_limit"],
        limit_5xx=limit_counts["server_error"],
        limit_transport_errors=limit_counts["transport"],
        limit_timeouts=limit_counts["timeout"],
        limit_other=(
            len(limit_attempts_done)
            - limit_counts["confirmed"]
            - limit_counts["per_user_limit"]
            - limit_counts["server_error"]
            - limit_counts["transport"]
            - limit_counts["timeout"]
        ),
        replay_ok=(
            first.kind == "confirmed"
            and replay.kind == "replay"
            and replay.reservation_id is not None
            and replay.reservation_id == first.reservation_id
            and status.get(replay_label) == "confirmed"
        ),
        mismatch_ok=(
            mismatch.kind == "idempotency_mismatch"
            and status.get(mismatch_label) == "available"
        ),
        counts_match=(
            invariant(counts)
            and counts["held"] == 0
            and counts["total"] == len(seats)
            and counts["confirmed"] == hot_counts["confirmed"] + limit_counts["confirmed"] + idem_confirmed
        ),
        during_samples=sampler.samples,
        reconciliation_failures=sampler.failures,
        total=counts["total"],
        available=counts["available"],
        held=counts["held"],
        confirmed=counts["confirmed"],
        invariant_ok=invariant(counts),
    )


async def _run_mixed(
    client: httpx.AsyncClient,
    *,
    admin_token: str,
    requests: int,
    concurrency: int,
    setup_concurrency: int,
    per_user_limit: int,
) -> _Mixed:
    if requests == 0:
        return _Mixed(
            attempts=[],
            requests=0,
            confirmed_requests=0,
            replay=0,
            seat_taken=0,
            per_user_limit=0,
            mismatch=0,
            other_4xx=0,
            server_errors=0,
            timeouts=0,
            transport_errors=0,
            checks_ok=True,
            elapsed_seconds=0.0,
            requests_per_second=0.0,
            during_samples=0,
            reconciliation_failures=0,
            total=0,
            available=0,
            held=0,
            confirmed=0,
            invariant_ok=True,
        )
    run_id = uuid4().hex[:8]
    labels, wave1, wave2 = plan_mixed(requests, run_id)
    show_id = await _create_show(client, admin_token, f"mix-{run_id}", labels)
    user_ids = list(dict.fromkeys(job.user_id for job in [*wave1, *wave2]))
    minted = await _tokens(client, user_ids, setup_concurrency)
    tokens = dict(zip(user_ids, minted, strict=True))
    sampler = _Sampler(client, show_id)
    started = time.perf_counter()
    first_wave = await _perform(client, show_id, wave1, tokens, concurrency, sampler)
    second_wave = await _perform(client, show_id, wave2, tokens, concurrency, sampler)
    elapsed = time.perf_counter() - started
    await sampler.probe()
    show = await _show(client, show_id)
    counts = _counts(show)
    attempts = [*first_wave, *second_wave]
    counts_by_kind = _count_kinds(attempts)
    by_key = {attempt.key: attempt for attempt in first_wave if attempt.kind == "confirmed"}
    return _Mixed(
        attempts=attempts,
        requests=len(attempts),
        confirmed_requests=counts_by_kind["confirmed"],
        replay=counts_by_kind["replay"],
        seat_taken=counts_by_kind["seat_taken"],
        per_user_limit=counts_by_kind["per_user_limit"],
        mismatch=counts_by_kind["idempotency_mismatch"],
        other_4xx=counts_by_kind["other_4xx"],
        server_errors=counts_by_kind["server_error"],
        timeouts=counts_by_kind["timeout"],
        transport_errors=counts_by_kind["transport"],
        checks_ok=_mixed_checks_ok(
            wave1,
            wave2,
            attempts,
            _seat_status(show),
            by_key,
            per_user_limit,
        ),
        elapsed_seconds=elapsed,
        requests_per_second=(len(attempts) / elapsed) if elapsed > 0 else 0.0,
        during_samples=sampler.samples,
        reconciliation_failures=sampler.failures,
        total=counts["total"],
        available=counts["available"],
        held=counts["held"],
        confirmed=counts["confirmed"],
        invariant_ok=invariant(counts),
    )


def _mixed_checks_ok(
    wave1: list[Job],
    wave2: list[Job],
    attempts: list[Attempt],
    seat_status: dict[str, str],
    confirmed_by_key: dict[str, Attempt],
    per_user_limit: int,
) -> bool:
    by_job = list(zip([*wave1, *wave2], attempts, strict=True))
    confirmed_seats: dict[str, int] = {}
    hot_winners: dict[str, int] = {}
    for job, attempt in by_job:
        if attempt.kind == "confirmed":
            confirmed_seats[job.user_id] = confirmed_seats.get(job.user_id, 0) + len(job.seats)
            if job.hot_seat is not None:
                hot_winners[job.hot_seat] = hot_winners.get(job.hot_seat, 0) + 1
        if job.kind == "normal" and attempt.kind != "confirmed":
            return False
        if job.kind == "limit" and attempt.kind not in {"confirmed", "per_user_limit"}:
            return False
        if job.kind == "seed" and attempt.kind != "confirmed":
            return False
        if job.kind == "hot" and attempt.kind not in {"confirmed", "seat_taken"}:
            return False
        if job.kind == "multi" and attempt.kind not in {"confirmed", "seat_taken"}:
            return False
    if any(count > per_user_limit for count in confirmed_seats.values()):
        return False
    hot_seats = {job.hot_seat for job in wave1 if job.hot_seat is not None}
    if any(hot_winners.get(label, 0) != 1 for label in hot_seats):
        return False
    limit_user_ids = {job.user_id for job in wave1 if job.kind == "limit"}
    limit_jobs = [job for job in wave1 if job.kind == "limit"]
    for user_id in limit_user_ids:
        won = confirmed_seats.get(user_id, 0)
        attempts_for_user = sum(1 for job in limit_jobs if job.user_id == user_id)
        if attempts_for_user >= per_user_limit and won != per_user_limit:
            return False
    for job in wave2:
        seed = confirmed_by_key.get(job.key)
        attempt = next(item for paired, item in by_job if paired is job)
        if job.kind == "replay":
            if (
                seed is None
                or attempt.kind != "replay"
                or attempt.reservation_id != seed.reservation_id
            ):
                return False
        if job.kind == "mismatch":
            if attempt.kind != "idempotency_mismatch":
                return False
            if job.untouched_seat is not None and seat_status.get(job.untouched_seat) != "available":
                return False
    winners: dict[str, int] = {}
    groups: dict[str, list[str]] = {}
    for job, attempt in by_job:
        if job.kind != "multi":
            continue
        group = job.key.rsplit("-", 1)[0]
        groups.setdefault(group, []).append(attempt.kind)
        if attempt.kind == "confirmed":
            for label in job.seats:
                winners[label] = winners.get(label, 0) + 1
    if any(count > 1 for count in winners.values()):
        return False
    return all(kinds.count("confirmed") == 1 for kinds in groups.values())


async def _perform(
    client: httpx.AsyncClient,
    show_id: str,
    jobs: Sequence[Job],
    tokens: dict[str, str],
    concurrency: int,
    sampler: _Sampler,
) -> list[Attempt]:
    completed = 0
    sample_every = max(1, len(jobs) // 8)

    async def worker(job: Job) -> Attempt:
        nonlocal completed
        attempt = await _reserve(
            client,
            show_id,
            job.user_id,
            tokens[job.user_id],
            list(job.seats),
            job.key,
        )
        completed += 1
        if completed % sample_every == 0:
            await sampler.probe()
        return attempt

    return await map_limited(concurrency, jobs, worker)


async def _tokens(
    client: httpx.AsyncClient,
    user_ids: Sequence[str],
    concurrency: int,
) -> list[str]:
    return await map_limited(concurrency, user_ids, lambda user_id: _token(client, user_id))


class _Sampler:
    def __init__(self, client: httpx.AsyncClient, show_id: str) -> None:
        self._client = client
        self._show_id = show_id
        self.samples = 0
        self.failures = 0
        self._lock = asyncio.Lock()

    async def probe(self) -> None:
        async with self._lock:
            try:
                response = await self._client.get(f"/shows/{self._show_id}")
            except httpx.HTTPError:
                return
            body = _object(response)
            counts = body.get("counts")
            if response.status_code != 200 or not isinstance(counts, dict):
                return
            self.samples += 1
            if not invariant(counts):
                self.failures += 1


def _hot_passed(report: Report) -> bool:
    return (
        report.hot_confirmed == 1
        and report.hot_seat_taken == report.hot_attempts - 1
        and report.hot_5xx == 0
        and report.hot_transport_errors == 0
        and report.hot_timeouts == 0
        and report.hot_other == 0
    )


def _limit_passed(report: Report) -> bool:
    expected_confirmed = min(report.limit_attempts, report.per_user_limit)
    expected_declined = max(report.limit_attempts - report.per_user_limit, 0)
    return (
        report.limit_confirmed == expected_confirmed
        and report.limit_confirmed <= report.per_user_limit
        and report.limit_declined == expected_declined
        and report.limit_5xx == 0
        and report.limit_transport_errors == 0
        and report.limit_timeouts == 0
        and report.limit_other == 0
    )


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _metric_lines(report: Report) -> list[str]:
    if not report.metrics_available:
        return ["  available: false"]
    return [
        f"  confirmed: observed {report.observed_confirmed} delta {report.metric_confirmed_delta:.0f}",
        f"  seat_taken: observed {report.observed_seat_taken} delta {report.metric_seat_taken_delta:.0f}",
        f"  per_user_limit: observed {report.observed_per_user_limit} delta {report.metric_per_user_limit_delta:.0f}",
        f"  idempotent_replay: observed {report.observed_replay} delta {report.metric_replay_delta:.0f}",
    ]


def _count_kinds(attempts: Sequence[Attempt]) -> dict[str, int]:
    counts = {
        "confirmed": 0,
        "replay": 0,
        "seat_taken": 0,
        "per_user_limit": 0,
        "idempotency_mismatch": 0,
        "other_4xx": 0,
        "server_error": 0,
        "timeout": 0,
        "transport": 0,
    }
    for attempt in attempts:
        counts[attempt.kind] = counts.get(attempt.kind, 0) + 1
    return counts


def _user(run_id: str, role: str, index: int) -> str:
    return f"{role}{run_id}{index}"


async def _create_show(
    client: httpx.AsyncClient,
    admin_token: str,
    name: str,
    seats: list[str],
) -> str:
    try:
        response = await client.post(
            "/shows",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"name": name, "seats": seats, "price_paise": 1000},
        )
    except httpx.HTTPError as exc:
        raise _setup_failure("create show", exc) from exc
    body = _object(response)
    if response.status_code != 201 or not isinstance(body.get("id"), str):
        reason = body.get("error") if isinstance(body.get("error"), str) else "unexpected response"
        raise BurstError(f"create show returned {response.status_code} {reason}")
    return body["id"]


async def _token(client: httpx.AsyncClient, user_id: str) -> str:
    try:
        response = await client.post("/auth/token", json={"user_id": user_id})
    except httpx.HTTPError as exc:
        raise _setup_failure("create token", exc) from exc
    body = _object(response)
    token = body.get("token")
    if response.status_code != 201 or not isinstance(token, str):
        reason = body.get("error") if isinstance(body.get("error"), str) else "unexpected response"
        raise BurstError(f"create token returned {response.status_code} {reason}")
    return token


async def _reserve(
    client: httpx.AsyncClient,
    show_id: str,
    user_id: str,
    token: str,
    seats: list[str],
    key: str,
) -> Attempt:
    try:
        response = await client.post(
            f"/shows/{show_id}/reserve",
            headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key},
            json={"seats": seats},
        )
    except httpx.TimeoutException:
        return _failed("timeout", user_id, seats, key)
    except httpx.HTTPError as exc:
        return _failed("transport", user_id, seats, key, type(exc).__name__)
    body = _object(response)
    error = body.get("error") if isinstance(body.get("error"), str) else None
    reservation_id = body.get("reservation_id")
    replay = response.headers.get("idempotent-replay") == "true"
    return Attempt(
        kind=classify(response.status_code, error, replay),
        status_code=response.status_code,
        error=error,
        reservation_id=reservation_id if isinstance(reservation_id, str) else None,
        replay=replay,
        seats=tuple(seats),
        user_id=user_id,
        key=key,
    )


def _first_task_error(done: set[asyncio.Task]) -> BaseException | None:
    found: BaseException | None = None
    for task in done:
        if task.cancelled():
            continue
        exc = task.exception()
        if exc is not None and found is None:
            found = exc
    return found


def _as_setup_error(exc: BaseException) -> BaseException:
    if isinstance(exc, BurstError):
        return exc
    if isinstance(exc, httpx.TimeoutException):
        return BurstError(f"request timed out ({exc.__class__.__name__})")
    if isinstance(exc, httpx.HTTPError):
        return BurstError(f"request failed ({exc.__class__.__name__})")
    return exc


def _setup_failure(action: str, exc: httpx.HTTPError) -> BurstError:
    if isinstance(exc, httpx.TimeoutException):
        return BurstError(f"{action} timed out ({exc.__class__.__name__})")
    return BurstError(f"{action} failed ({exc.__class__.__name__})")


def _transport_error_types(attempts: Sequence[Attempt]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for attempt in attempts:
        if attempt.kind != "transport":
            continue
        name = attempt.exception_type or "HTTPError"
        counts[name] = counts.get(name, 0) + 1
    return counts


def _transport_type_lines(types: dict[str, int]) -> list[str]:
    if not types:
        return []
    return [
        "",
        "Transport errors:",
        *[f"  {name}: {types[name]}" for name in sorted(types)],
    ]


def _failed(
    kind: str,
    user_id: str,
    seats: list[str],
    key: str,
    exception_type: str = "",
) -> Attempt:
    return Attempt(
        kind=kind,
        status_code=0,
        error=None,
        reservation_id=None,
        replay=False,
        seats=tuple(seats),
        user_id=user_id,
        key=key,
        exception_type=exception_type,
    )


async def _show(client: httpx.AsyncClient, show_id: str) -> dict:
    try:
        response = await client.get(f"/shows/{show_id}")
    except httpx.HTTPError as exc:
        raise _setup_failure("read show", exc) from exc
    body = _object(response)
    counts = body.get("counts")
    seats = body.get("seats")
    if response.status_code != 200 or not isinstance(counts, dict) or not isinstance(seats, list):
        raise BurstError(f"read show returned {response.status_code}")
    return body


def _counts(show: dict) -> dict[str, int]:
    counts = show["counts"]
    return {
        "total": int(counts["total"]),
        "available": int(counts["available"]),
        "held": int(counts["held"]),
        "confirmed": int(counts["confirmed"]),
    }


def _seat_status(show: dict) -> dict[str, str]:
    status: dict[str, str] = {}
    for seat in show["seats"]:
        if isinstance(seat, dict) and isinstance(seat.get("seat_label"), str):
            value = seat.get("status")
            if isinstance(value, str):
                status[seat["seat_label"]] = value
    return status


async def _metrics(client: httpx.AsyncClient) -> dict[str, float] | None:
    try:
        response = await client.get("/metrics")
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    return {
        "confirmed": _prometheus(response.text, "reservations_confirmed_total"),
        "seat_taken": _prometheus(
            response.text,
            "reservations_declined_total",
            {"reason": "seat_taken"},
        ),
        "per_user_limit": _prometheus(
            response.text,
            "reservations_declined_total",
            {"reason": "per_user_limit"},
        ),
        "replay": _prometheus(
            response.text,
            "reservations_declined_total",
            {"reason": "idempotent_replay"},
        ),
    }


def _deltas(
    before: dict[str, float] | None,
    after: dict[str, float] | None,
) -> dict[str, float] | None:
    if before is None or after is None:
        return None
    return {key: after[key] - before[key] for key in after}


def _delta(deltas: dict[str, float] | None, key: str) -> float:
    if deltas is None:
        return 0.0
    return deltas[key]


def _prometheus(body: str, name: str, labels: dict[str, str] | None = None) -> float:
    prefix = name if not labels else name + "{"
    for line in body.splitlines():
        if line.startswith("#") or not line.startswith(prefix):
            continue
        head, _, value = line.partition(" ")
        if labels is None:
            if head == name:
                return float(value)
            continue
        if not head.startswith(name + "{") or not head.endswith("}"):
            continue
        found = dict(
            item.split("=", 1)
            for item in head[len(name) + 1 : -1].split(",")
            if "=" in item
        )
        found = {key: item.strip('"') for key, item in found.items()}
        if all(found.get(key) == expected for key, expected in labels.items()):
            return float(value)
    return 0.0


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
