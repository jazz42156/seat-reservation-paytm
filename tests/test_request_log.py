import asyncio
import json
from uuid import UUID

import httpx
from fastapi import FastAPI

from app.main import app
from app.request_log import RequestLogMiddleware

_TOKEN = "super-secret-admin-token-xyz"
_IDEMPOTENCY_KEY = "super-secret-idempotency-key-xyz"


async def _send(
    application: FastAPI,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, headers=headers, content=content)


def _request_logs(captured: str) -> list[dict]:
    found: list[dict] = []
    for line in captured.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and str(payload.get("event", "")).startswith("request."):
            found.append(payload)
    return found


def test_generated_request_id_is_returned(capfd) -> None:
    response = asyncio.run(_send(app, "GET", "/health/live"))
    captured = capfd.readouterr()

    assert response.status_code == 200
    request_id = response.headers["x-request-id"]
    UUID(request_id)
    logs = _request_logs(captured.out + captured.err)
    assert len(logs) == 1
    assert logs[0]["request_id"] == request_id


def test_caller_request_id_is_propagated(capfd) -> None:
    response = asyncio.run(
        _send(app, "GET", "/health/live", headers={"X-Request-ID": "caller-req-1"})
    )
    captured = capfd.readouterr()

    assert response.headers["x-request-id"] == "caller-req-1"
    logs = _request_logs(captured.out + captured.err)
    assert logs[0]["request_id"] == "caller-req-1"
    assert logs[0]["method"] == "GET"
    assert logs[0]["path"] == "/health/live"
    assert logs[0]["status_code"] == 200
    assert logs[0]["event"] == "request.completed"
    assert logs[0]["level"] == "info"
    assert "timestamp" in logs[0]
    assert isinstance(logs[0]["duration_ms"], (int, float))


def test_invalid_request_id_is_replaced(capfd) -> None:
    response = asyncio.run(
        _send(app, "GET", "/health/live", headers={"X-Request-ID": "bad id"})
    )

    request_id = response.headers["x-request-id"]
    UUID(request_id)
    assert request_id != "bad id"
    captured = capfd.readouterr()
    assert "bad id" not in captured.out + captured.err


def test_completion_log_omits_secrets(capfd) -> None:
    response = asyncio.run(
        _send(
            app,
            "POST",
            "/auth/token",
            headers={
                "Authorization": f"Bearer {_TOKEN}",
                "Idempotency-Key": _IDEMPOTENCY_KEY,
                "X-Request-ID": "token-req-1",
            },
            content=b"",
        )
    )
    captured = capfd.readouterr()
    text = captured.out + captured.err
    logs = _request_logs(text)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"
    assert len(logs) == 1
    assert logs[0]["event"] == "request.completed"
    assert logs[0]["level"] == "info"
    assert logs[0]["request_id"] == "token-req-1"
    assert logs[0]["method"] == "POST"
    assert logs[0]["path"] == "/auth/token"
    assert logs[0]["status_code"] == 400
    assert logs[0]["outcome"] == "invalid_request"
    assert _TOKEN not in text
    assert _IDEMPOTENCY_KEY not in text
    assert "Authorization" not in text
    assert "Idempotency-Key" not in text


def test_unexpected_exception_log_uses_the_request_id(capfd) -> None:
    mini = FastAPI()
    mini.add_middleware(RequestLogMiddleware)

    @mini.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("db-password-should-not-leak")

    response = asyncio.run(
        _send(mini, "GET", "/boom", headers={"X-Request-ID": "err-req-1"})
    )
    captured = capfd.readouterr()
    text = captured.out + captured.err
    logs = _request_logs(text)

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "err-req-1"
    assert response.text == "Internal Server Error"
    assert len(logs) == 1
    assert logs[0]["event"] == "request.failed"
    assert logs[0]["level"] == "error"
    assert logs[0]["request_id"] == "err-req-1"
    assert logs[0]["exception_type"] == "RuntimeError"
    assert "db-password-should-not-leak" not in text
