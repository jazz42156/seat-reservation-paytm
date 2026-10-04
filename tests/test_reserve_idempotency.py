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


async def _reservation_count(show_id: str, user_id: str | None = None) -> int:
    if user_id is None:
        return await get_pool().fetchval(
            "SELECT count(*) FROM reservations WHERE show_id = $1",
            UUID(show_id),
        )
    return await get_pool().fetchval(
        "SELECT count(*) FROM reservations WHERE show_id = $1 AND user_id = $2",
        UUID(show_id),
        user_id,
    )


async def _same_key_same_request() -> None:
    user_id = f"same-{uuid4().hex[:8]}"
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"idem-same-{uuid4().hex[:8]}", ["A1"])
            user = await _token(client, user_id)
            first = await _reserve(client, show_id, user, ["A1"], "K")
            second = await _reserve(client, show_id, user, ["A1"], "K")
            show = await _request(client, "GET", f"/shows/{show_id}")
            reservations = await _reservation_count(show_id)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replay"] == "true"
    assert first.json()["reservation_id"] == second.json()["reservation_id"]
    assert show.status_code == 200
    assert show.json()["counts"] == {"total": 1, "available": 0, "held": 0, "confirmed": 1}
    assert reservations == 1


async def _same_key_different_request() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"idem-diff-{uuid4().hex[:8]}", ["A1", "A2"])
            user = await _token(client, f"diff-{uuid4().hex[:8]}")
            first = await _reserve(client, show_id, user, ["A1"], "K")
            second = await _reserve(client, show_id, user, ["A2"], "K")
            show = await _request(client, "GET", f"/shows/{show_id}")

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json()["error"] == "idempotency_mismatch"
    assert show.status_code == 200
    statuses = {seat["seat_label"]: seat["status"] for seat in show.json()["seats"]}
    assert statuses == {"A1": "confirmed", "A2": "available"}


async def _stored_decline_replay() -> None:
    loser_id = f"loser-{uuid4().hex[:8]}"
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"idem-decline-{uuid4().hex[:8]}", ["A1"])
            winner = await _token(client, f"winner-{uuid4().hex[:8]}")
            loser = await _token(client, loser_id)
            won = await _reserve(client, show_id, winner, ["A1"], "winner")
            first = await _reserve(client, show_id, loser, ["A1"], "K")
            second = await _reserve(client, show_id, loser, ["A1"], "K")
            loser_reservations = await _reservation_count(show_id, loser_id)

    assert won.status_code == 201
    assert first.status_code == 409
    assert second.status_code == 409
    assert first.json()["error"] == "seat_taken"
    assert second.json()["error"] == "seat_taken"
    assert loser_reservations == 0


def test_same_key_same_request_replays_one_reservation() -> None:
    asyncio.run(_same_key_same_request())


def test_same_key_different_request_is_rejected() -> None:
    asyncio.run(_same_key_different_request())


def test_stored_decline_replay_does_not_create_a_reservation() -> None:
    asyncio.run(_stored_decline_replay())
