import os
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app

_ADMIN = {"Authorization": f"Bearer {os.environ.get('ADMIN_TOKEN', 'dev-admin-token')}"}


def _counts_match_seats(body: dict) -> None:
    seats = body["seats"]
    counts = body["counts"]
    assert counts["available"] == sum(1 for seat in seats if seat["status"] == "available")
    assert counts["held"] == sum(1 for seat in seats if seat["status"] == "held")
    assert counts["confirmed"] == sum(1 for seat in seats if seat["status"] == "confirmed")
    assert counts["available"] + counts["held"] + counts["confirmed"] == counts["total"]
    assert counts["total"] == len(seats)


def test_new_show_lists_every_seat_available() -> None:
    with TestClient(app) as client:
        created = client.post(
            "/shows",
            headers=_ADMIN,
            json={"name": "get-new", "seats": ["B2", "A1", "B1"], "price_paise": 25000},
        )
        assert created.status_code == 201
        show_id = created.json()["id"]

        response = client.get(f"/shows/{show_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == show_id
    assert body["name"] == "get-new"
    assert body["price_paise"] == 25000
    assert body["per_user_limit"] == 4
    assert body["seats"] == [
        {"seat_label": "A1", "status": "available"},
        {"seat_label": "B1", "status": "available"},
        {"seat_label": "B2", "status": "available"},
    ]
    assert body["counts"] == {"total": 3, "available": 3, "held": 0, "confirmed": 0}
    _counts_match_seats(body)


def test_reservation_moves_a_seat_from_available_to_confirmed() -> None:
    with TestClient(app) as client:
        created = client.post(
            "/shows",
            headers=_ADMIN,
            json={"name": "get-reserved", "seats": ["A1", "A2"], "price_paise": 1000},
        )
        assert created.status_code == 201
        show_id = created.json()["id"]
        token = client.post("/auth/token", json={"user_id": f"getter-{uuid4().hex[:8]}"})
        assert token.status_code == 201
        reserved = client.post(
            f"/shows/{show_id}/reserve",
            headers={
                "Authorization": f"Bearer {token.json()['token']}",
                "Idempotency-Key": "get-show-reserve",
            },
            json={"seats": ["A2"]},
        )
        assert reserved.status_code == 201

        response = client.get(f"/shows/{show_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["available"] == 1
    assert body["counts"]["confirmed"] == 1
    assert body["counts"]["held"] == 0
    statuses = {seat["seat_label"]: seat["status"] for seat in body["seats"]}
    assert statuses == {"A1": "available", "A2": "confirmed"}
    _counts_match_seats(body)


def test_missing_show_returns_404() -> None:
    with TestClient(app) as client:
        missing = client.get(f"/shows/{uuid4()}")
        invalid = client.get("/shows/not-a-show")

    assert missing.status_code == 404
    assert missing.json()["error"] == "not_found"
    assert invalid.status_code == 404
    assert invalid.json()["error"] == "not_found"
