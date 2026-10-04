"""Seat claim.

Lock order in the transaction is always:
1. the idempotency row for this user, show, and key
2. the user_show_counts row
3. the requested seat rows, sorted by seat_label
"""

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from app.db import get_pool

_MAX_PAISE = 2**63 - 1
_ATTEMPTS = 3


@dataclass(frozen=True)
class ReserveOutcome:
    status_code: int
    body: dict
    replay: bool


class _Decline(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message


async def reserve_seats(
    *,
    show_id: UUID,
    user_id: str,
    seats: list[str],
    idempotency_key: str,
) -> ReserveOutcome:
    ordered = sorted(seats)
    request_hash = hashlib.sha256("\n".join(ordered).encode()).hexdigest()
    for attempt in range(_ATTEMPTS):
        try:
            return await _reserve_once(
                show_id=show_id,
                user_id=user_id,
                seats=ordered,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.SerializationError,
        ):
            if attempt == _ATTEMPTS - 1:
                raise
    raise RuntimeError("reserve retry loop ended without a result")


async def _reserve_once(
    *,
    show_id: UUID,
    user_id: str,
    seats: list[str],
    idempotency_key: str,
    request_hash: str,
) -> ReserveOutcome:
    async with get_pool().acquire() as connection:
        async with connection.transaction():
            show = await connection.fetchrow(
                """
                SELECT price_paise, per_user_limit
                FROM shows
                WHERE id = $1
                """,
                show_id,
            )
            if show is None:
                return ReserveOutcome(
                    404,
                    {"error": "not_found", "message": "show not found"},
                    False,
                )

            found = await connection.fetch(
                """
                SELECT seat_label
                FROM seats
                WHERE show_id = $1 AND seat_label = ANY($2::text[])
                """,
                show_id,
                seats,
            )
            if len(found) != len(seats):
                return ReserveOutcome(
                    400,
                    {"error": "invalid_request", "message": "unknown seat"},
                    False,
                )

            amount_paise = show["price_paise"] * len(seats)
            if amount_paise > _MAX_PAISE:
                return ReserveOutcome(
                    400,
                    {"error": "invalid_request", "message": "amount_paise is too large"},
                    False,
                )

            replay = await _claim_idempotency(
                connection,
                show_id=show_id,
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay

            try:
                async with connection.transaction():
                    await _increment_user_count(
                        connection,
                        show_id=show_id,
                        user_id=user_id,
                        seat_count=len(seats),
                        per_user_limit=show["per_user_limit"],
                    )
                    await _lock_available_seats(connection, show_id=show_id, seats=seats)
                    body = await _confirm(
                        connection,
                        show_id=show_id,
                        user_id=user_id,
                        seats=seats,
                        amount_paise=amount_paise,
                    )
            except _Decline as decline:
                error_body = {"error": decline.code, "message": decline.message}
                await _complete_idempotency(
                    connection,
                    show_id=show_id,
                    user_id=user_id,
                    idempotency_key=idempotency_key,
                    outcome=decline.code,
                    http_status=409,
                    body=error_body,
                )
                return ReserveOutcome(409, error_body, False)

            await _complete_idempotency(
                connection,
                show_id=show_id,
                user_id=user_id,
                idempotency_key=idempotency_key,
                outcome="confirmed",
                http_status=201,
                body=body,
            )
            return ReserveOutcome(201, body, False)


async def _claim_idempotency(
    connection: asyncpg.Connection,
    *,
    show_id: UUID,
    user_id: str,
    idempotency_key: str,
    request_hash: str,
) -> ReserveOutcome | None:
    inserted = await connection.fetchrow(
        """
        INSERT INTO idempotency_keys (
            user_id, show_id, idempotency_key, request_hash, outcome, response
        )
        VALUES ($1, $2, $3, $4, 'in_progress', NULL)
        ON CONFLICT (user_id, show_id, idempotency_key) DO NOTHING
        RETURNING outcome
        """,
        user_id,
        show_id,
        idempotency_key,
        request_hash,
    )
    if inserted is not None:
        return None

    existing = await connection.fetchrow(
        """
        SELECT request_hash, outcome, response
        FROM idempotency_keys
        WHERE user_id = $1 AND show_id = $2 AND idempotency_key = $3
        FOR UPDATE
        """,
        user_id,
        show_id,
        idempotency_key,
    )
    if existing is None:
        raise RuntimeError("idempotency row disappeared")
    if existing["request_hash"] != request_hash:
        return ReserveOutcome(
            409,
            {
                "error": "idempotency_mismatch",
                "message": "idempotency key was already used with different seats",
            },
            False,
        )
    if existing["outcome"] == "in_progress" or existing["response"] is None:
        return ReserveOutcome(
            409,
            {
                "error": "idempotency_in_progress",
                "message": "idempotency key is still in progress",
            },
            False,
        )

    stored = _load_json(existing["response"])
    body = stored["body"]
    if existing["outcome"] == "confirmed":
        return ReserveOutcome(200, body, True)
    return ReserveOutcome(int(stored["http_status"]), body, True)


async def _increment_user_count(
    connection: asyncpg.Connection,
    *,
    show_id: UUID,
    user_id: str,
    seat_count: int,
    per_user_limit: int,
) -> None:
    await connection.execute(
        """
        INSERT INTO user_show_counts (user_id, show_id, confirmed_seat_count)
        VALUES ($1, $2, 0)
        ON CONFLICT (user_id, show_id)
        DO UPDATE SET confirmed_seat_count = user_show_counts.confirmed_seat_count
        """,
        user_id,
        show_id,
    )
    updated = await connection.fetchrow(
        """
        UPDATE user_show_counts
        SET confirmed_seat_count = confirmed_seat_count + $3
        WHERE user_id = $1
          AND show_id = $2
          AND confirmed_seat_count + $3 <= $4
        RETURNING confirmed_seat_count
        """,
        user_id,
        show_id,
        seat_count,
        per_user_limit,
    )
    if updated is None:
        raise _Decline("per_user_limit", "seat request exceeds the per-user limit")


async def _lock_available_seats(
    connection: asyncpg.Connection,
    *,
    show_id: UUID,
    seats: list[str],
) -> None:
    for label in seats:
        row = await connection.fetchrow(
            """
            SELECT status
            FROM seats
            WHERE show_id = $1 AND seat_label = $2
            FOR UPDATE
            """,
            show_id,
            label,
        )
        if row is None or row["status"] != "available":
            raise _Decline("seat_taken", "one or more seats are no longer available")


async def _confirm(
    connection: asyncpg.Connection,
    *,
    show_id: UUID,
    user_id: str,
    seats: list[str],
    amount_paise: int,
) -> dict:
    reservation = await connection.fetchrow(
        """
        INSERT INTO reservations (show_id, user_id, status)
        VALUES ($1, $2, 'confirmed')
        RETURNING id
        """,
        show_id,
        user_id,
    )
    if reservation is None:
        raise RuntimeError("reservation insert did not return a row")
    reservation_id = reservation["id"]
    await connection.executemany(
        """
        INSERT INTO reservation_seats (reservation_id, show_id, seat_label)
        VALUES ($1, $2, $3)
        """,
        [(reservation_id, show_id, label) for label in seats],
    )
    for label in seats:
        updated = await connection.fetchrow(
            """
            UPDATE seats
            SET status = 'confirmed', reservation_id = $3
            WHERE show_id = $1
              AND seat_label = $2
              AND status = 'available'
            RETURNING seat_label
            """,
            show_id,
            label,
            reservation_id,
        )
        if updated is None:
            raise _Decline("seat_taken", "one or more seats are no longer available")
    return {
        "reservation_id": str(reservation_id),
        "show_id": str(show_id),
        "user_id": user_id,
        "seats": seats,
        "amount_paise": amount_paise,
        "status": "confirmed",
    }


async def _complete_idempotency(
    connection: asyncpg.Connection,
    *,
    show_id: UUID,
    user_id: str,
    idempotency_key: str,
    outcome: str,
    http_status: int,
    body: dict,
) -> None:
    await connection.execute(
        """
        UPDATE idempotency_keys
        SET outcome = $4, response = $5::jsonb
        WHERE user_id = $1 AND show_id = $2 AND idempotency_key = $3
        """,
        user_id,
        show_id,
        idempotency_key,
        outcome,
        json.dumps({"http_status": http_status, "body": body}),
    )


def _load_json(value: object) -> dict:
    if isinstance(value, str):
        loaded = json.loads(value)
    else:
        loaded = value
    if not isinstance(loaded, dict):
        raise RuntimeError("stored idempotency response is not an object")
    return loaded
