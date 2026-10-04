import asyncio
import os
from uuid import uuid4

import httpx

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


def _assert_reconciled(body: dict) -> None:
    counts = body["counts"]
    seats = body["seats"]
    assert counts["available"] + counts["held"] + counts["confirmed"] == counts["total"]
    assert counts["total"] == len(seats)
    assert counts["available"] == sum(1 for seat in seats if seat["status"] == "available")
    assert counts["held"] == sum(1 for seat in seats if seat["status"] == "held")
    assert counts["confirmed"] == sum(1 for seat in seats if seat["status"] == "confirmed")


async def _hot_seat_storm() -> None:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"hot-{uuid4().hex[:8]}", ["A1"])
            users = [await _token(client, f"hot-{index}-{uuid4().hex[:8]}") for index in range(12)]
            responses = await asyncio.gather(
                *[
                    _request(
                        client,
                        "POST",
                        f"/shows/{show_id}/reserve",
                        headers={**users[index], "Idempotency-Key": f"hot-{index}"},
                        json={"seats": ["A1"]},
                    )
                    for index in range(12)
                ]
            )
            show = await _request(client, "GET", f"/shows/{show_id}")

    statuses = [response.status_code for response in responses]
    assert statuses.count(201) == 1
    assert statuses.count(409) == 11
    assert all(status < 500 for status in statuses)
    assert [response.json()["error"] for response in responses if response.status_code == 409] == [
        "seat_taken"
    ] * 11

    assert show.status_code == 200
    body = show.json()
    assert body["counts"] == {"total": 1, "available": 0, "held": 0, "confirmed": 1}
    _assert_reconciled(body)


async def _concurrent_per_user_limit() -> None:
    seats = ["S1", "S2", "S3", "S4", "S5", "S6"]
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            show_id = await _show(client, f"limit-{uuid4().hex[:8]}", seats)
            before = await _request(client, "GET", f"/shows/{show_id}")
            assert before.status_code == 200
            assert before.json()["per_user_limit"] == 4

            user = await _token(client, f"limit-{uuid4().hex[:8]}")
            responses = await asyncio.gather(
                *[
                    _request(
                        client,
                        "POST",
                        f"/shows/{show_id}/reserve",
                        headers={**user, "Idempotency-Key": f"limit-{seat}"},
                        json={"seats": [seat]},
                    )
                    for seat in seats
                ]
            )
            show = await _request(client, "GET", f"/shows/{show_id}")

    assert all(response.status_code < 500 for response in responses)
    confirmed_seats = 0
    for response in responses:
        if response.status_code == 201:
            confirmed_seats += len(response.json()["seats"])
        else:
            assert response.status_code == 409
            assert response.json()["error"] == "per_user_limit"
    assert confirmed_seats <= 4
    assert any(response.status_code == 409 for response in responses)

    assert show.status_code == 200
    body = show.json()
    _assert_reconciled(body)
    assert body["counts"]["confirmed"] == confirmed_seats
    assert body["counts"]["confirmed"] <= 4
    assert body["counts"]["held"] == 0
    assert body["counts"]["total"] == len(seats)


def test_hot_seat_storm() -> None:
    asyncio.run(_hot_seat_storm())


def test_concurrent_per_user_limit() -> None:
    asyncio.run(_concurrent_per_user_limit())
