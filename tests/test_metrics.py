import asyncio
import os
from uuid import UUID, uuid4

import httpx

from app.db import get_pool
from app.main import app

_ADMIN = {"Authorization": f"Bearer {os.environ.get('ADMIN_TOKEN', 'dev-admin-token')}"}


async def _request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json: dict | None = None,
) -> httpx.Response:
    return await client.request(method, path, headers=headers, json=json)


async def _token(client: httpx.AsyncClient, user_id: str) -> dict[str, str]:
    response = await _request(client, "POST", "/auth/token", json={"user_id": user_id})
    assert response.status_code == 201
    return {"Authorization": f"Bearer {response.json()['token']}"}


async def _show(client: httpx.AsyncClient, name: str, seats: list[str]) -> str:
    response = await _request(
        client,
        "POST",
        "/shows",
        headers=_ADMIN,
        json={"name": name, "seats": seats, "price_paise": 1000},
    )
    assert response.status_code == 201
    return response.json()["id"]


async def _reserve(
    client: httpx.AsyncClient,
    show_id: str,
    headers: dict[str, str],
    seats: list[str],
    key: str,
) -> httpx.Response:
    return await _request(
        client,
        "POST",
        f"/shows/{show_id}/reserve",
        headers={**headers, "Idempotency-Key": key},
        json={"seats": seats},
    )


def _sample(body: str, name: str, labels: dict[str, str] | None = None) -> float:
    if labels:
        rendered = ",".join(f'{key}="{value}"' for key, value in labels.items())
        needle = f"{name}{{{rendered}}}"
    else:
        needle = name
    for line in body.splitlines():
        if line.startswith("#"):
            continue
        head, _, value = line.partition(" ")
        if head == needle:
            return float(value)
    return 0.0


async def _metrics(client: httpx.AsyncClient) -> str:
    response = await _request(client, "GET", "/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    return response.text


async def _available(show_id: str) -> int:
    return await get_pool().fetchval(
        """
        SELECT COUNT(*) FILTER (WHERE status = 'available')
        FROM seats
        WHERE show_id = $1
        """,
        UUID(show_id),
    )


async def _fresh_confirmation_increments_once() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            before = _sample(await _metrics(client), "reservations_confirmed_total")
            show_id = await _show(client, f"metrics-fresh-{uuid4().hex[:8]}", ["A1"])
            user = await _token(client, f"fresh-{uuid4().hex[:8]}")
            reserved = await _reserve(client, show_id, user, ["A1"], "once")
            after = _sample(await _metrics(client), "reservations_confirmed_total")

    assert reserved.status_code == 201
    assert "idempotent-replay" not in reserved.headers
    assert after - before == 1


async def _replay_counts_once_and_does_not_confirm_again() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            before_confirmed = _sample(await _metrics(client), "reservations_confirmed_total")
            before_replay = _sample(
                await _metrics(client),
                "reservations_declined_total",
                {"reason": "idempotent_replay"},
            )
            show_id = await _show(client, f"metrics-replay-{uuid4().hex[:8]}", ["A1"])
            user = await _token(client, f"replay-{uuid4().hex[:8]}")
            first = await _reserve(client, show_id, user, ["A1"], "same")
            second = await _reserve(client, show_id, user, ["A1"], "same")
            text = await _metrics(client)
            after_confirmed = _sample(text, "reservations_confirmed_total")
            after_replay = _sample(
                text,
                "reservations_declined_total",
                {"reason": "idempotent_replay"},
            )

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replay"] == "true"
    assert second.json() == first.json()
    assert after_confirmed - before_confirmed == 1
    assert after_replay - before_replay == 1


async def _seat_taken_increments_decline() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"metrics-taken-{uuid4().hex[:8]}", ["A1"])
            winner = await _token(client, f"winner-{uuid4().hex[:8]}")
            loser = await _token(client, f"loser-{uuid4().hex[:8]}")
            won = await _reserve(client, show_id, winner, ["A1"], "winner")
            before = _sample(
                await _metrics(client),
                "reservations_declined_total",
                {"reason": "seat_taken"},
            )
            lost = await _reserve(client, show_id, loser, ["A1"], "loser")
            after = _sample(
                await _metrics(client),
                "reservations_declined_total",
                {"reason": "seat_taken"},
            )

    assert won.status_code == 201
    assert lost.status_code == 409
    assert lost.json()["error"] == "seat_taken"
    assert after - before == 1


async def _per_user_limit_increments_decline() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(
                client,
                f"metrics-limit-{uuid4().hex[:8]}",
                ["A1", "A2", "A3", "A4", "A5"],
            )
            user = await _token(client, f"limit-{uuid4().hex[:8]}")
            filled = await _reserve(client, show_id, user, ["A1", "A2", "A3", "A4"], "fill")
            before = _sample(
                await _metrics(client),
                "reservations_declined_total",
                {"reason": "per_user_limit"},
            )
            declined = await _reserve(client, show_id, user, ["A5"], "over")
            after = _sample(
                await _metrics(client),
                "reservations_declined_total",
                {"reason": "per_user_limit"},
            )

    assert filled.status_code == 201
    assert declined.status_code == 409
    assert declined.json()["error"] == "per_user_limit"
    assert after - before == 1


async def _available_gauge_matches_database() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(
                client,
                f"metrics-gauge-{uuid4().hex[:8]}",
                ["A1", "A2", "A3"],
            )
            user = await _token(client, f"gauge-{uuid4().hex[:8]}")
            reserved = await _reserve(client, show_id, user, ["A1"], "one")
            text = await _metrics(client)
            stored = await _available(show_id)

    assert reserved.status_code == 201
    assert stored == 2
    assert _sample(text, "seats_available", {"show_id": show_id}) == stored


async def _cancel_increases_available_gauge() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(
                client,
                f"metrics-cancel-{uuid4().hex[:8]}",
                ["A1", "A2"],
            )
            user = await _token(client, f"cancel-{uuid4().hex[:8]}")
            reserved = await _reserve(client, show_id, user, ["A1"], "hold")
            before_text = await _metrics(client)
            before = _sample(before_text, "seats_available", {"show_id": show_id})
            cancelled = await _request(
                client,
                "POST",
                f"/reservations/{reserved.json()['reservation_id']}/cancel",
                headers=user,
            )
            after_text = await _metrics(client)
            after = _sample(after_text, "seats_available", {"show_id": show_id})
            stored = await _available(show_id)

    assert cancelled.status_code == 200
    assert before == 1
    assert after == 2
    assert after == stored
    assert after > before


def test_fresh_confirmation_increments_once() -> None:
    asyncio.run(_fresh_confirmation_increments_once())


def test_replay_counts_once_and_does_not_confirm_again() -> None:
    asyncio.run(_replay_counts_once_and_does_not_confirm_again())


def test_seat_taken_increments_decline() -> None:
    asyncio.run(_seat_taken_increments_decline())


def test_per_user_limit_increments_decline() -> None:
    asyncio.run(_per_user_limit_increments_decline())


def test_available_gauge_matches_database() -> None:
    asyncio.run(_available_gauge_matches_database())


def test_cancel_increases_available_gauge() -> None:
    asyncio.run(_cancel_increases_available_gauge())
