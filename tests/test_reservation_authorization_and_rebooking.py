import asyncio
import os
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import httpx

from app.db import get_pool
from app.main import app

_ADMIN = {"Authorization": f"Bearer {os.environ.get('ADMIN_TOKEN', 'dev-admin-token')}"}


@asynccontextmanager
async def _client():
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


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


async def _show(client: httpx.AsyncClient) -> str:
    response = await _request(
        client,
        "POST",
        "/shows",
        headers=_ADMIN,
        json={"name": f"auth-{uuid4().hex[:8]}", "seats": ["A1"], "price_paise": 1000},
    )
    assert response.status_code == 201
    return response.json()["id"]


async def _reserve(
    client: httpx.AsyncClient,
    show_id: str,
    headers: dict[str, str],
    key: str,
    body: dict | None = None,
) -> httpx.Response:
    payload = {"seats": ["A1"]}
    if body:
        payload.update(body)
    return await _request(
        client,
        "POST",
        f"/shows/{show_id}/reserve",
        headers={**headers, "Idempotency-Key": key},
        json=payload,
    )


async def _cancel(
    client: httpx.AsyncClient,
    reservation_id: str,
    headers: dict[str, str],
    body: dict | None = None,
) -> httpx.Response:
    return await _request(
        client,
        "POST",
        f"/reservations/{reservation_id}/cancel",
        headers=headers,
        json=body,
    )


def _assert_reconciled(body: dict) -> None:
    counts = body["counts"]
    seats = body["seats"]
    assert counts["available"] + counts["held"] + counts["confirmed"] == counts["total"]
    assert counts["total"] == len(seats)
    assert counts["available"] == sum(1 for seat in seats if seat["status"] == "available")
    assert counts["held"] == sum(1 for seat in seats if seat["status"] == "held")
    assert counts["confirmed"] == sum(1 for seat in seats if seat["status"] == "confirmed")


async def _seat(show_id: str) -> dict:
    row = await get_pool().fetchrow(
        """
        SELECT
            seats.status,
            seats.reservation_id::text AS reservation_id,
            reservations.user_id,
            reservations.status AS reservation_status
        FROM seats
        LEFT JOIN reservations ON reservations.id = seats.reservation_id
        WHERE seats.show_id = $1 AND seats.seat_label = 'A1'
        """,
        UUID(show_id),
    )
    assert row is not None
    return dict(row)


async def _owner_can_cancel_and_seat_can_be_rebooked() -> None:
    user1_id = f"user1-{uuid4().hex[:8]}"
    user2_id = f"user2-{uuid4().hex[:8]}"
    async with _client() as client:
        show_id = await _show(client)
        user1 = await _token(client, user1_id)
        user2 = await _token(client, user2_id)
        reserved = await _reserve(client, show_id, user1, "user1-book")
        assert reserved.status_code == 201
        cancelled = await _cancel(client, reserved.json()["reservation_id"], user1)
        assert cancelled.status_code == 200
        after_cancel = await _request(client, "GET", f"/shows/{show_id}")
        assert after_cancel.status_code == 200
        assert after_cancel.json()["seats"] == [{"seat_label": "A1", "status": "available"}]
        _assert_reconciled(after_cancel.json())

        rebooked = await _reserve(client, show_id, user2, "user2-book")
        assert rebooked.status_code == 201
        show = await _request(client, "GET", f"/shows/{show_id}")
        owner = await _seat(show_id)

    assert show.status_code == 200
    assert show.json()["seats"] == [{"seat_label": "A1", "status": "confirmed"}]
    _assert_reconciled(show.json())
    assert rebooked.json()["user_id"] == user2_id
    assert owner["reservation_id"] == rebooked.json()["reservation_id"]
    assert owner["user_id"] == user2_id
    assert owner["reservation_status"] == "confirmed"


async def _non_owner_cannot_cancel() -> None:
    user1_id = f"user1-{uuid4().hex[:8]}"
    async with _client() as client:
        show_id = await _show(client)
        user1 = await _token(client, user1_id)
        user2 = await _token(client, f"user2-{uuid4().hex[:8]}")
        reserved = await _reserve(client, show_id, user1, "user1-book")
        assert reserved.status_code == 201
        reservation_id = reserved.json()["reservation_id"]
        denied = await _cancel(client, reservation_id, user2, {"user_id": user1_id})
        show = await _request(client, "GET", f"/shows/{show_id}")
        owner = await _seat(show_id)
        original = await get_pool().fetchrow(
            "SELECT status, user_id FROM reservations WHERE id = $1",
            UUID(reservation_id),
        )

    assert denied.status_code == 403
    assert denied.json()["error"] == "forbidden"
    assert show.json()["seats"] == [{"seat_label": "A1", "status": "confirmed"}]
    _assert_reconciled(show.json())
    assert owner["reservation_id"] == reservation_id
    assert owner["user_id"] == user1_id
    assert original["status"] == "confirmed"
    assert original["user_id"] == user1_id


async def _body_user_id_cannot_spoof_token() -> None:
    user1_id = f"user1-{uuid4().hex[:8]}"
    user2_id = f"user2-{uuid4().hex[:8]}"
    async with _client() as client:
        show_id = await _show(client)
        user1 = await _token(client, user1_id)
        reserved = await _reserve(
            client,
            show_id,
            user1,
            "spoof-book",
            {"user_id": user2_id},
        )
        assert reserved.status_code == 201
        reservation_id = reserved.json()["reservation_id"]
        cancelled = await _cancel(
            client,
            reservation_id,
            user1,
            {"user_id": user2_id},
        )
        owner = await _seat(show_id)
        stored = await get_pool().fetchrow(
            "SELECT user_id, status FROM reservations WHERE id = $1",
            UUID(reservation_id),
        )

    assert reserved.json()["user_id"] == user1_id
    assert cancelled.status_code == 200
    assert stored["user_id"] == user1_id
    assert stored["status"] == "cancelled"
    assert owner["status"] == "available"
    assert owner["user_id"] is None


async def _double_cancel() -> None:
    user1_id = f"user1-{uuid4().hex[:8]}"
    async with _client() as client:
        show_id = await _show(client)
        user1 = await _token(client, user1_id)
        reserved = await _reserve(client, show_id, user1, "once")
        assert reserved.status_code == 201
        reservation_id = reserved.json()["reservation_id"]
        first = await _cancel(client, reservation_id, user1)
        second = await _cancel(client, reservation_id, user1)
        show = await _request(client, "GET", f"/shows/{show_id}")
        count = await get_pool().fetchval(
            """
            SELECT confirmed_seat_count
            FROM user_show_counts
            WHERE user_id = $1 AND show_id = $2
            """,
            user1_id,
            UUID(show_id),
        )

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["error"] == "already_cancelled"
    assert show.json()["seats"] == [{"seat_label": "A1", "status": "available"}]
    _assert_reconciled(show.json())
    assert count == 0


async def _old_cancel_does_not_release_the_new_owner() -> None:
    user1_id = f"user1-{uuid4().hex[:8]}"
    user2_id = f"user2-{uuid4().hex[:8]}"
    async with _client() as client:
        show_id = await _show(client)
        user1 = await _token(client, user1_id)
        user2 = await _token(client, user2_id)
        first = await _reserve(client, show_id, user1, "user1-book")
        assert first.status_code == 201
        old_reservation_id = first.json()["reservation_id"]
        cancelled = await _cancel(client, old_reservation_id, user1)
        assert cancelled.status_code == 200
        rebooked = await _reserve(client, show_id, user2, "user2-book")
        assert rebooked.status_code == 201
        late = await _cancel(client, old_reservation_id, user1)
        show = await _request(client, "GET", f"/shows/{show_id}")
        owner = await _seat(show_id)
        old = await get_pool().fetchrow(
            "SELECT status FROM reservations WHERE id = $1",
            UUID(old_reservation_id),
        )

    assert late.status_code == 409
    assert late.json()["error"] == "already_cancelled"
    assert show.json()["seats"] == [{"seat_label": "A1", "status": "confirmed"}]
    _assert_reconciled(show.json())
    assert owner["reservation_id"] == rebooked.json()["reservation_id"]
    assert owner["user_id"] == user2_id
    assert owner["reservation_status"] == "confirmed"
    assert old["status"] == "cancelled"


def test_owner_can_cancel_and_seat_can_be_rebooked() -> None:
    asyncio.run(_owner_can_cancel_and_seat_can_be_rebooked())


def test_non_owner_cannot_cancel() -> None:
    asyncio.run(_non_owner_cannot_cancel())


def test_body_user_id_cannot_spoof_token() -> None:
    asyncio.run(_body_user_id_cannot_spoof_token())


def test_double_cancel() -> None:
    asyncio.run(_double_cancel())


def test_old_cancel_does_not_release_the_new_owner() -> None:
    asyncio.run(_old_cancel_does_not_release_the_new_owner())
