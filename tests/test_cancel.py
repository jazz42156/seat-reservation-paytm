import os
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app

_ADMIN = {"Authorization": f"Bearer {os.environ.get('ADMIN_TOKEN', 'dev-admin-token')}"}


def _user(client: TestClient, user_id: str) -> dict[str, str]:
    token = client.post("/auth/token", json={"user_id": user_id})
    assert token.status_code == 201
    return {"Authorization": f"Bearer {token.json()['token']}"}


def _show(client: TestClient, name: str, seats: list[str]) -> str:
    created = client.post(
        "/shows",
        headers=_ADMIN,
        json={"name": name, "seats": seats, "price_paise": 1000},
    )
    assert created.status_code == 201
    return created.json()["id"]


def _reserve(client: TestClient, show_id: str, headers: dict[str, str], seats: list[str], key: str) -> dict:
    response = client.post(
        f"/shows/{show_id}/reserve",
        headers={**headers, "Idempotency-Key": key},
        json={"seats": seats},
    )
    assert response.status_code == 201
    return response.json()


def test_owner_cancel_frees_seats_and_show_counts() -> None:
    with TestClient(app) as client:
        show_id = _show(client, "cancel-owner", ["A1", "A2", "A3", "A4"])
        owner = _user(client, f"owner-{uuid4().hex[:8]}")
        first = _reserve(client, show_id, owner, ["A1", "A2", "A3", "A4"], "fill")

        cancelled = client.post(f"/reservations/{first['reservation_id']}/cancel", headers=owner)
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "cancelled"
        assert cancelled.json()["seats"] == ["A1", "A2", "A3", "A4"]

        show = client.get(f"/shows/{show_id}")
        assert show.status_code == 200
        body = show.json()
        assert {seat["status"] for seat in body["seats"]} == {"available"}
        assert body["counts"] == {"total": 4, "available": 4, "held": 0, "confirmed": 0}

        again = _reserve(client, show_id, owner, ["A1", "A2", "A3", "A4"], "after-cancel")
        assert again["status"] == "confirmed"


def test_non_owner_cannot_cancel() -> None:
    with TestClient(app) as client:
        show_id = _show(client, "cancel-other", ["B1"])
        owner = _user(client, f"owner-{uuid4().hex[:8]}")
        other = _user(client, f"other-{uuid4().hex[:8]}")
        reservation = _reserve(client, show_id, owner, ["B1"], "owner-seat")

        denied = client.post(
            f"/reservations/{reservation['reservation_id']}/cancel",
            headers=other,
            json={"user_id": owner["Authorization"]},
        )
        assert denied.status_code == 403
        assert denied.json()["error"] == "forbidden"

        show = client.get(f"/shows/{show_id}")
        assert show.json()["seats"] == [{"seat_label": "B1", "status": "confirmed"}]


def test_cancelling_again_returns_409() -> None:
    with TestClient(app) as client:
        show_id = _show(client, "cancel-twice", ["C1"])
        owner = _user(client, f"owner-{uuid4().hex[:8]}")
        reservation = _reserve(client, show_id, owner, ["C1"], "once")
        reservation_id = reservation["reservation_id"]

        first = client.post(f"/reservations/{reservation_id}/cancel", headers=owner)
        second = client.post(f"/reservations/{reservation_id}/cancel", headers=owner)

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["error"] == "already_cancelled"


def test_missing_reservation_returns_404() -> None:
    with TestClient(app) as client:
        owner = _user(client, f"owner-{uuid4().hex[:8]}")
        missing = client.post(f"/reservations/{uuid4()}/cancel", headers=owner)
        invalid = client.post("/reservations/not-a-reservation/cancel", headers=owner)

    assert missing.status_code == 404
    assert missing.json()["error"] == "not_found"
    assert invalid.status_code == 404
    assert invalid.json()["error"] == "not_found"
