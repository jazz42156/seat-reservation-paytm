from app.api import CreateShowRequest, ReserveRequest, TokenRequest
from app.main import app


def _json_schema(path: str) -> dict:
    operation = app.openapi()["paths"][path]["post"]
    body = operation["requestBody"]
    assert body["required"] is True
    content = body["content"]
    assert list(content) == ["application/json"]
    return content["application/json"]["schema"]


def test_create_show_documents_json_body() -> None:
    assert _json_schema("/shows") == CreateShowRequest.model_json_schema()


def test_create_token_documents_json_body() -> None:
    assert _json_schema("/auth/token") == TokenRequest.model_json_schema()


def test_reserve_documents_json_body() -> None:
    assert _json_schema("/shows/{show_id}/reserve") == ReserveRequest.model_json_schema()
    parameters = app.openapi()["paths"]["/shows/{show_id}/reserve"]["post"]["parameters"]
    names = {parameter["name"] for parameter in parameters}
    assert "show_id" in names
    assert "idempotency-key" in names


def test_cancel_has_no_json_body() -> None:
    operation = app.openapi()["paths"]["/reservations/{reservation_id}/cancel"]["post"]
    assert "requestBody" not in operation
