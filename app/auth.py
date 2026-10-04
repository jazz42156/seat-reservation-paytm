"""Request identity."""

import hashlib
import hmac
import re
from typing import Annotated

from fastapi import Header, HTTPException

from app.settings import load_settings

_USER_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


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


async def require_user(
    authorization: Annotated[str | None, Header()] = None,
) -> str:
    settings = load_settings()
    user_id = user_id_from_token(_bearer_token(authorization), settings.auth_secret)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "message": "user token required"},
        )
    return user_id


def issue_user_token(user_id: str) -> str:
    if not _USER_ID.fullmatch(user_id):
        raise ValueError("user_id must be 1-64 characters of letters, digits, _ or -")
    settings = load_settings()
    if not settings.auth_secret:
        raise ValueError("AUTH_SECRET is not configured")
    signature = hmac.new(
        settings.auth_secret.encode(),
        user_id.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"v1.{user_id}.{signature}"


def user_id_from_token(token: str, secret: str) -> str | None:
    if not secret:
        return None
    version, separator, rest = token.partition(".")
    user_id, separator, signature = rest.partition(".")
    if version != "v1" or not separator or not _USER_ID.fullmatch(user_id):
        return None
    expected = hmac.new(secret.encode(), user_id.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    return user_id


def _bearer_token(authorization: str | None) -> str:
    if authorization is None:
        return ""
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return token.strip()
