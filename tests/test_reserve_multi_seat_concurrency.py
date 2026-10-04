import asyncio
import os
from uuid import UUID, uuid4

import httpx

from app.db import get_pool
from app.main import app

_ADMIN = {"Authorization": f"Bearer {os.environ.get('ADMIN_TOKEN', 'dev-admin-token')}"}
_SEATS = ["A1", "A2", "A3", "A4"]
_REQUESTS = [
    ["A1", "A2"],
    ["A2", "A3"],
    ["A1", "A3"],
    ["A3", "A4"],
]


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


def _assert_reconciled(body: dict) -> None:
    counts = body["counts"]
    seats = body["seats"]
    assert counts["available"] + counts["held"] + counts["confirmed"] == counts["total"]
    assert counts["total"] == len(seats)
    assert counts["available"] == sum(1 for seat in seats if seat["status"] == "available")
    assert counts["held"] == sum(1 for seat in seats if seat["status"] == "held")
    assert counts["confirmed"] == sum(1 for seat in seats if seat["status"] == "confirmed")


async def _seat_owners(show_id: str) -> list[dict]:
    return await get_pool().fetch(
        """
        SELECT
            seats.seat_label,
            seats.status,
            seats.reservation_id::text AS reservation_id,
            reservations.user_id
        FROM seats
        LEFT JOIN reservations ON reservations.id = seats.reservation_id
        WHERE seats.show_id = $1
        ORDER BY seats.seat_label
        """,
        UUID(show_id),
    )


async def _overlapping_multi_seat_reservations() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=15.0) as client:
            show_id = await _show(client, f"overlap-{uuid4().hex[:8]}", _SEATS)
            users = [
                await _token(client, f"overlap-{index}-{uuid4().hex[:8]}")
                for index in range(len(_REQUESTS))
            ]
            responses = await asyncio.wait_for(
                asyncio.gather(
                    *[
                        _request(
                            client,
                            "POST",
                            f"/shows/{show_id}/reserve",
                            headers={**users[index], "Idempotency-Key": f"overlap-{index}"},
                            json={"seats": seats},
                        )
                        for index, seats in enumerate(_REQUESTS)
                    ]
                ),
                timeout=10,
            )
            show = await _request(client, "GET", f"/shows/{show_id}")
            owners = await _seat_owners(show_id)

    won_seats: dict[str, str] = {}
    for requested, response in zip(_REQUESTS, responses, strict=True):
        assert response.status_code < 500
        if response.status_code == 201:
            body = response.json()
            assert sorted(body["seats"]) == sorted(requested)
            for seat in body["seats"]:
                assert seat not in won_seats
                won_seats[seat] = body["reservation_id"]
        else:
            assert response.status_code == 409
            assert response.json()["error"] == "seat_taken"

    assert show.status_code == 200
    body = show.json()
    _assert_reconciled(body)
    statuses = {seat["seat_label"]: seat["status"] for seat in body["seats"]}
    assert set(statuses) == set(_SEATS)
    for seat in _SEATS:
        expected = "confirmed" if seat in won_seats else "available"
        assert statuses[seat] == expected

    assert len(owners) == len(_SEATS)
    for row in owners:
        if row["status"] == "confirmed":
            assert row["reservation_id"] == won_seats[row["seat_label"]]
            assert row["user_id"]
        else:
            assert row["status"] == "available"
            assert row["reservation_id"] is None
            assert row["seat_label"] not in won_seats


def test_overlapping_multi_seat_reservations() -> None:
    asyncio.run(_overlapping_multi_seat_reservations())
