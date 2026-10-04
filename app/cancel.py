"""Reservation release.

Lock order in the transaction is always:
1. the reservation row
2. that reservation's seat rows, sorted by seat_label
3. the user_show_counts row, when the released seats are decremented
"""

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from app.db import get_pool

_ATTEMPTS = 3


@dataclass(frozen=True)
class CancelOutcome:
    status_code: int
    body: dict


async def cancel_seats(*, reservation_id: UUID, user_id: str) -> CancelOutcome:
    for attempt in range(_ATTEMPTS):
        try:
            return await _cancel_once(reservation_id=reservation_id, user_id=user_id)
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.SerializationError,
        ):
            if attempt == _ATTEMPTS - 1:
                raise
    raise RuntimeError("cancel retry loop ended without a result")


async def _cancel_once(*, reservation_id: UUID, user_id: str) -> CancelOutcome:
    async with get_pool().acquire() as connection:
        async with connection.transaction():
            reservation = await connection.fetchrow(
                """
                SELECT id, show_id, user_id, status
                FROM reservations
                WHERE id = $1
                FOR UPDATE
                """,
                reservation_id,
            )
            if reservation is None:
                return CancelOutcome(
                    404,
                    {"error": "not_found", "message": "reservation not found"},
                )
            if reservation["user_id"] != user_id:
                return CancelOutcome(
                    403,
                    {"error": "forbidden", "message": "only the reservation owner can cancel"},
                )
            if reservation["status"] != "confirmed":
                code = (
                    "already_cancelled"
                    if reservation["status"] == "cancelled"
                    else "not_confirmed"
                )
                return CancelOutcome(
                    409,
                    {"error": code, "message": "reservation is not confirmed"},
                )

            await connection.fetch(
                """
                SELECT seat_label
                FROM seats
                WHERE show_id = $1 AND reservation_id = $2
                ORDER BY seat_label
                FOR UPDATE
                """,
                reservation["show_id"],
                reservation_id,
            )
            released_rows = await connection.fetch(
                """
                UPDATE seats
                SET status = 'available', reservation_id = NULL
                WHERE show_id = $1
                  AND reservation_id = $2
                  AND status = 'confirmed'
                RETURNING seat_label
                """,
                reservation["show_id"],
                reservation_id,
            )
            released = sorted(row["seat_label"] for row in released_rows)
            if released:
                updated = await connection.fetchrow(
                    """
                    UPDATE user_show_counts
                    SET confirmed_seat_count = confirmed_seat_count - $3
                    WHERE user_id = $1
                      AND show_id = $2
                      AND confirmed_seat_count >= $3
                    RETURNING confirmed_seat_count
                    """,
                    user_id,
                    reservation["show_id"],
                    len(released),
                )
                if updated is None:
                    raise RuntimeError("confirmed seat count could not be decremented")

            await connection.execute(
                """
                UPDATE reservations
                SET status = 'cancelled'
                WHERE id = $1 AND status = 'confirmed'
                """,
                reservation_id,
            )
            return CancelOutcome(
                200,
                {
                    "reservation_id": str(reservation_id),
                    "show_id": str(reservation["show_id"]),
                    "status": "cancelled",
                    "seats": released,
                },
            )
