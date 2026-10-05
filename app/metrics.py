"""Prometheus metrics.

Counters move only after a reservation attempt has finished.
The available-seat gauge is read from Postgres on each scrape.
"""

from prometheus_client import Counter, Gauge

from app.db import get_pool

reservations_confirmed_total = Counter(
    "reservations_confirmed_total",
    "Fresh seat reservations confirmed",
)

reservations_declined_total = Counter(
    "reservations_declined_total",
    "Reservation attempts that did not create a new confirmation",
    ["reason"],
)

seats_available = Gauge(
    "seats_available",
    "Seats currently available, grouped by show",
    ["show_id"],
)


async def refresh_seats_available() -> None:
    async with get_pool().acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT show_id,
                   COUNT(*) FILTER (WHERE status = 'available') AS available
            FROM seats
            GROUP BY show_id
            """
        )
    for row in rows:
        seats_available.labels(show_id=str(row["show_id"])).set(row["available"])
