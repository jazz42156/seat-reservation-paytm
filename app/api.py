import json
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from app.auth import require_admin
from app.db import get_pool

router = APIRouter()

_MAX_PAISE = 2**63 - 1


class CreateShowRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    seats: list[str]
    price_paise: int

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("price_paise", mode="before")
    @classmethod
    def price_must_be_non_negative_int(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("must be a non-negative integer")
        if value < 0 or value > _MAX_PAISE:
            raise ValueError("must be a non-negative integer")
        return value

    @field_validator("seats")
    @classmethod
    def seats_must_be_unique_names(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for seat in value:
            label = seat.strip()
            if not label:
                raise ValueError("must not contain an empty seat name")
            if label in seen:
                raise ValueError("must not contain duplicate seat names")
            seen.add(label)
            cleaned.append(label)
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned


class SeatResponse(BaseModel):
    seat_label: str
    status: str


class ShowResponse(BaseModel):
    id: UUID
    name: str
    price_paise: int
    seats: list[SeatResponse]


@router.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "alive"}


@router.post("/shows", status_code=201, response_model=ShowResponse)
async def create_show(
    request: Request,
    _: Annotated[None, Depends(require_admin)],
) -> ShowResponse:
    payload = await _read_show(request)
    pool = get_pool()
    async with pool.acquire() as connection:
        async with connection.transaction():
            show = await connection.fetchrow(
                """
                INSERT INTO shows (name, price_paise)
                VALUES ($1, $2)
                RETURNING id, name, price_paise
                """,
                payload.name,
                payload.price_paise,
            )
            if show is None:
                raise RuntimeError("show insert did not return a row")
            await connection.executemany(
                """
                INSERT INTO seats (show_id, seat_label, status, reservation_id)
                VALUES ($1, $2, 'available', NULL)
                """,
                [(show["id"], label) for label in payload.seats],
            )
    return ShowResponse(
        id=show["id"],
        name=show["name"],
        price_paise=show["price_paise"],
        seats=[
            SeatResponse(seat_label=label, status="available") for label in payload.seats
        ],
    )


async def _read_show(request: Request) -> CreateShowRequest:
    raw = await request.body()
    if not raw:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "request body must be JSON"},
        )
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "request body must be JSON"},
        ) from None
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "request body must be a JSON object"},
        )
    try:
        return CreateShowRequest.model_validate(body)
    except ValidationError as exc:
        error = exc.errors()[0]
        field = ".".join(str(part) for part in error["loc"])
        message = error["msg"].removeprefix("Value error, ")
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": f"{field}: {message}"},
        ) from None
