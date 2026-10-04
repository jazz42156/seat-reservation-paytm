"""Request identity."""

import hmac
from typing import Annotated

from fastapi import Header, HTTPException

from app.settings import load_settings


async def require_admin(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    settings = load_settings()
    presented = _bearer_token(authorization)
    if not settings.admin_token or not hmac.compare_digest(presented, settings.admin_token):
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "message": "admin token required"},
        )


def _bearer_token(authorization: str | None) -> str:
    if authorization is None:
        return ""
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return token.strip()
