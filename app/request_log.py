"""One JSON completion log and a correlation id for every HTTP request."""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CONTEXT_FIELDS = ("show_id", "reservation_id", "user_id", "outcome")
_logger = logging.getLogger("app.request")


class _StdoutHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            sys.stdout.write(self.format(record) + "\n")
            sys.stdout.flush()
        except Exception:
            self.handleError(record)


def configure_request_logger() -> None:
    if _logger.handlers:
        return
    handler = _StdoutHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False


def request_id_from_header(value: str | None) -> str:
    if value is None:
        return str(uuid4())
    candidate = value.strip()
    if _REQUEST_ID.fullmatch(candidate):
        return candidate
    return str(uuid4())


class RequestLogMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = request_id_from_header(_header(scope, b"x-request-id"))
        state = scope.get("state")
        if not isinstance(state, dict):
            state = {}
            scope["state"] = state
        state["request_id"] = request_id
        started = time.perf_counter()
        status_code = 500
        response_started = False

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = int(message["status"])
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception as exc:
            if not response_started:
                failed = PlainTextResponse(
                    "Internal Server Error",
                    status_code=500,
                    headers={"X-Request-ID": request_id},
                )
                await failed(scope, receive, send)
            _emit(
                level="error",
                event="request.failed",
                scope=scope,
                request_id=request_id,
                status_code=500 if not response_started else status_code,
                duration_ms=_elapsed_ms(started),
                exception_type=type(exc).__name__,
            )
            if response_started:
                raise
            return

        level = "error" if status_code >= 500 else "info"
        _emit(
            level=level,
            event="request.completed",
            scope=scope,
            request_id=request_id,
            status_code=status_code,
            duration_ms=_elapsed_ms(started),
        )


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key == name:
            return value.decode("latin-1")
    return None


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)


def _safe_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned or len(cleaned) > 128 or any(character.isspace() for character in cleaned):
        return None
    return cleaned


def _context(scope: Scope) -> dict[str, str]:
    state = scope.get("state")
    path_params = scope.get("path_params")
    state_values = state if isinstance(state, dict) else {}
    path_values = path_params if isinstance(path_params, dict) else {}
    context: dict[str, str] = {}
    for key in _CONTEXT_FIELDS:
        value = _safe_value(state_values.get(key))
        if value is None and key in {"show_id", "reservation_id"}:
            value = _safe_value(path_values.get(key))
        if value is not None:
            context[key] = value
    return context


def _emit(
    *,
    level: str,
    event: str,
    scope: Scope,
    request_id: str,
    status_code: int,
    duration_ms: float,
    exception_type: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        "request_id": request_id,
        "method": scope.get("method", ""),
        "path": scope.get("path", ""),
        "status_code": status_code,
        "duration_ms": duration_ms,
    }
    payload.update(_context(scope))
    if exception_type is not None:
        payload["exception_type"] = exception_type
    _logger.log(logging.ERROR if level == "error" else logging.INFO, json.dumps(payload))


configure_request_logger()
