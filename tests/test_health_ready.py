from fastapi.testclient import TestClient

from app.main import app

_SECRET = "postgresql://seat:super-secret-password@db.internal:5432/seat"


class _QueryFailure:
    def acquire(self) -> "_QueryFailure":
        return self

    async def __aenter__(self) -> "_QueryFailure":
        return self

    async def __aexit__(self, *_args: object) -> bool:
        return False

    async def fetchval(self, _query: str) -> int:
        raise ConnectionError(_SECRET)


def test_ready_when_database_is_reachable() -> None:
    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_ready_when_database_query_fails(monkeypatch) -> None:
    monkeypatch.setattr("app.api.get_pool", lambda: _QueryFailure())

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert _SECRET not in response.text


def test_live_stays_up_when_database_is_not_ready(monkeypatch) -> None:
    monkeypatch.setattr("app.api.get_pool", lambda: _QueryFailure())

    with TestClient(app) as client:
        ready = client.get("/health/ready")
        live = client.get("/health/live")

    assert ready.status_code == 503
    assert ready.json() == {"status": "not_ready"}
    assert live.status_code == 200
    assert live.json() == {"status": "alive"}
