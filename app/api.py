import json
from typing import Annotated, TypeVar
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from app.auth import issue_user_token, require_admin, require_user
from app.cancel import cancel_seats
from app.db import get_pool
from app.metrics import (
    refresh_seats_available,
    reservations_confirmed_total,
    reservations_declined_total,
)
from app.reserve import ReserveOutcome, reserve_seats

router = APIRouter()

_MAX_PAISE = 2**63 - 1
T = TypeVar("T", bound=BaseModel)


def _json_request_body(model: type[BaseModel]) -> dict[str, object]:
    return {
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": model.model_json_schema(),
                }
            },
        }
    }


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


class ShowCounts(BaseModel):
    total: int
    available: int
    held: int
    confirmed: int


class ShowStateResponse(BaseModel):
    id: UUID
    name: str
    price_paise: int
    per_user_limit: int
    seats: list[SeatResponse]
    counts: ShowCounts


@router.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/health/ready")
async def health_ready() -> JSONResponse:
    try:
        pool = get_pool()
        async with pool.acquire() as connection:
            answer = await connection.fetchval("SELECT 1")
    except Exception:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    if answer != 1:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    return JSONResponse(status_code=200, content={"status": "ready"})


@router.get("/metrics")
async def metrics() -> Response:
    await refresh_seats_available()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post(
    "/shows",
    status_code=201,
    response_model=ShowResponse,
    openapi_extra=_json_request_body(CreateShowRequest),
)
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


@router.get("/shows/{show_id}", response_model=ShowStateResponse)
async def get_show(show_id: str) -> ShowStateResponse:
    try:
        parsed_show_id = UUID(show_id)
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": "show not found"},
        ) from None

    pool = get_pool()
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                shows.id,
                shows.name,
                shows.price_paise,
                shows.per_user_limit,
                seats.seat_label,
                seats.status
            FROM shows
            LEFT JOIN seats ON seats.show_id = shows.id
            WHERE shows.id = $1
            ORDER BY seats.seat_label
            """,
            parsed_show_id,
        )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": "show not found"},
        )

    seats = [
        SeatResponse(seat_label=row["seat_label"], status=row["status"])
        for row in rows
        if row["seat_label"] is not None
    ]
    available = sum(1 for seat in seats if seat.status == "available")
    held = sum(1 for seat in seats if seat.status == "held")
    confirmed = sum(1 for seat in seats if seat.status == "confirmed")
    total = available + held + confirmed
    show = rows[0]
    return ShowStateResponse(
        id=show["id"],
        name=show["name"],
        price_paise=show["price_paise"],
        per_user_limit=show["per_user_limit"],
        seats=seats,
        counts=ShowCounts(
            total=total,
            available=available,
            held=held,
            confirmed=confirmed,
        ),
    )


class TokenRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    user_id: str


class TokenResponse(BaseModel):
    token: str
    token_type: str
    user_id: str


class ReserveRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    seats: list[str]

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


@router.post(
    "/auth/token",
    status_code=201,
    response_model=TokenResponse,
    openapi_extra=_json_request_body(TokenRequest),
)
async def create_token(request: Request) -> TokenResponse:
    payload = await _read_model(request, TokenRequest)
    try:
        token = issue_user_token(payload.user_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": str(exc)},
        ) from None
    return TokenResponse(token=token, token_type="Bearer", user_id=payload.user_id)


@router.post(
    "/shows/{show_id}/reserve",
    openapi_extra=_json_request_body(ReserveRequest),
)
async def reserve_show(
    show_id: str,
    request: Request,
    user_id: Annotated[str, Depends(require_user)],
    idempotency_key: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    request.state.user_id = user_id
    key = (idempotency_key or "").strip()
    if not key or len(key) > 200:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_request",
                "message": "Idempotency-Key header is required",
            },
        )
    try:
        parsed_show_id = UUID(show_id)
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": "show not found"},
        ) from None
    payload = await _read_model(request, ReserveRequest)
    outcome = await reserve_seats(
        show_id=parsed_show_id,
        user_id=user_id,
        seats=payload.seats,
        idempotency_key=key,
    )
    _record_reserve_metrics(outcome)
    _remember_result(request, outcome.body)
    headers = {"Idempotent-Replay": "true"} if outcome.replay else None
    return JSONResponse(status_code=outcome.status_code, content=outcome.body, headers=headers)


@router.post("/reservations/{reservation_id}/cancel")
async def cancel_reservation(
    reservation_id: str,
    request: Request,
    user_id: Annotated[str, Depends(require_user)],
) -> JSONResponse:
    request.state.user_id = user_id
    try:
        parsed_reservation_id = UUID(reservation_id)
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": "reservation not found"},
        ) from None
    outcome = await cancel_seats(reservation_id=parsed_reservation_id, user_id=user_id)
    _remember_result(request, outcome.body)
    return JSONResponse(status_code=outcome.status_code, content=outcome.body)


def _remember_result(request: Request, body: dict) -> None:
    reservation_id = body.get("reservation_id")
    if isinstance(reservation_id, str) and reservation_id:
        request.state.reservation_id = reservation_id
    show_id = body.get("show_id")
    if isinstance(show_id, str) and show_id:
        request.state.show_id = show_id
    outcome = body.get("error") if isinstance(body.get("error"), str) else body.get("status")
    if isinstance(outcome, str) and outcome:
        request.state.outcome = outcome


def _record_reserve_metrics(outcome: ReserveOutcome) -> None:
    if outcome.replay:
        reservations_declined_total.labels(reason="idempotent_replay").inc()
        return
    if outcome.status_code == 201:
        reservations_confirmed_total.inc()
        return
    error = outcome.body.get("error")
    if error == "seat_taken":
        reservations_declined_total.labels(reason="seat_taken").inc()
    elif error == "per_user_limit":
        reservations_declined_total.labels(reason="per_user_limit").inc()


async def _read_show(request: Request) -> CreateShowRequest:
    return await _read_model(request, CreateShowRequest)


async def _read_model(request: Request, model: type[T]) -> T:
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
        return model.model_validate(body)
    except ValidationError as exc:
        error = exc.errors()[0]
        field = ".".join(str(part) for part in error["loc"])
        message = error["msg"].removeprefix("Value error, ")
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": f"{field}: {message}"},
        ) from None
