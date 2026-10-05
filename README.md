# Seat Reservation

Concurrency-safe assigned-seat reservation API for the Paytm Money Backend Engineering Deploy & Observe assignment.

A seat is confirmed in one PostgreSQL transaction. The database decides who gets it. There is no Redis, Kafka, or ORM.

## Tech stack

| Piece | Version in this repo |
| --- | --- |
| Python | 3.12 (`Dockerfile`) |
| FastAPI | 0.115.6 |
| Uvicorn | 0.34.0 |
| PostgreSQL | 16 (`docker-compose.yml`) |
| asyncpg | 0.30.0 |
| prometheus-client | 0.21.1 |
| pytest | 8.3.4 |
| httpx | 0.28.1 |
| Docker Compose | `docker-compose.yml` |

## Architecture

```text
Client
  -> FastAPI (one uvicorn process)
  -> PostgreSQL
```

PostgreSQL is the source of truth for seats, reservations, per-user counts, and idempotency. The API pool is asyncpg with `min_size=1` and `max_size=10`. On startup the process applies `migrations/001_init.sql` when `public.shows` does not already exist.

The container command is a single uvicorn worker:

```text
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

## Correctness guarantees

- A seat moves from `available` to `confirmed` only when the conditional `UPDATE` matches that row. Two reservations cannot both commit the same seat.
- Each show has `per_user_limit` 4, the column default. `POST /shows` does not accept a different limit.
- `amount_paise` is `price_paise * seat count`, computed with integers.
- The same idempotency key for the same user and show replays the stored result. A different seat list for that key is rejected.
- The user id comes from the bearer token. Reserve models ignore extra JSON fields, so a `user_id` in the reserve body cannot override the token. Cancel has no JSON body.
- A multi-seat request is all-or-nothing. If any requested seat cannot be confirmed, none of those seats change.
- Only the reservation owner can cancel. A successful cancel sets those seats back to `available`.
- A second cancel, including one that arrives after another user has booked the seat, does not change the newer reservation.

## Atomic reservation

`POST /shows/{id}/reserve` runs one transaction in `app/reserve.py`. Lock order is fixed:

1. Idempotency row for `(user_id, show_id, idempotency_key)`.
2. `user_show_counts` row for that user and show.
3. Requested `seats` rows, one at a time, in `seat_label` order (`SELECT ... FOR UPDATE`).

The claim is a conditional update:

```sql
UPDATE seats
SET status = 'confirmed', reservation_id = $reservation
WHERE show_id = $show
  AND seat_label = $seat
  AND status = 'available'
```

Zero rows updated means the seat was not available. The seat changes and the counter change sit in an inner transaction (a savepoint). A decline rolls that savepoint back, stores the `409` on the idempotency row, and commits the outer transaction. Deadlock and serialization failures are retried up to three times inside the request.

## API

Errors use `{"error": "<code>", "message": "<text>"}` unless noted.

### `POST /shows`

Admin only. Header: `Authorization: Bearer <ADMIN_TOKEN>`.

```json
{
  "name": "Evening show",
  "seats": ["A1", "A2"],
  "price_paise": 25000
}
```

`201`:

```json
{
  "id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "name": "Evening show",
  "price_paise": 25000,
  "seats": [
    {"seat_label": "A1", "status": "available"},
    {"seat_label": "A2", "status": "available"}
  ]
}
```

Empty name, empty seat list, blank labels, duplicate labels, and a negative or non-integer price return `400` `invalid_request`. Missing or wrong admin token returns `401` `unauthorized`.

### `POST /auth/token`

No authentication. This mints a demo bearer token. It is not a login system.

```json
{"user_id": "user-1001"}
```

`201`:

```json
{
  "token": "v1.user-1001.<hmac-sha256-hex>",
  "token_type": "Bearer",
  "user_id": "user-1001"
}
```

`user_id` must be 1–64 characters of letters, digits, `_`, or `-`. The token is `v1.<user_id>.<hmac>` signed with `AUTH_SECRET`. Reserve and cancel read the user id from this token.

### `POST /shows/{id}/reserve`

User token required. Header `Idempotency-Key` is required (1–200 characters after trimming).

```http
Authorization: Bearer v1.user-1001.<hmac>
Idempotency-Key: booking-42
```

```json
{"seats": ["A1", "A2"]}
```

First confirm, `201`:

```json
{
  "reservation_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "show_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "user_id": "user-1001",
  "seats": ["A1", "A2"],
  "amount_paise": 50000,
  "status": "confirmed"
}
```

Seats in the stored body are sorted by label. `amount_paise` is the show price times the seat count.

### `GET /shows/{id}`

No authentication. Returns every seat plus counts taken from the `seats` rows. `available + held + confirmed` equals `total`.

```json
{
  "id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "name": "Evening show",
  "price_paise": 25000,
  "per_user_limit": 4,
  "seats": [
    {"seat_label": "A1", "status": "confirmed"},
    {"seat_label": "A2", "status": "available"}
  ],
  "counts": {"total": 2, "available": 1, "held": 0, "confirmed": 1}
}
```

Unknown or non-UUID show id returns `404` `not_found`.

### `POST /reservations/{id}/cancel`

User token required. No JSON body.

Owner cancel, `200`:

```json
{
  "reservation_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "show_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "status": "cancelled",
  "seats": ["A1", "A2"]
}
```

`seats` lists the labels this call actually released.

### `GET /health/live`

Process liveness. Does not touch PostgreSQL.

```json
{"status": "alive"}
```

### `GET /health/ready`

Runs `SELECT 1`. Success is `200` `{"status": "ready"}`. A failed query is `503` `{"status": "not_ready"}`.

### `GET /metrics`

Prometheus text. On each scrape, `seats_available` is refreshed from PostgreSQL before the registry is rendered.

## Authentication

| Call | Credential |
| --- | --- |
| `POST /shows` | `Authorization: Bearer <ADMIN_TOKEN>` |
| `POST /shows/{id}/reserve` | `Authorization: Bearer <user token>` |
| `POST /reservations/{id}/cancel` | `Authorization: Bearer <user token>` |
| `POST /auth/token`, `GET /shows/{id}`, health, metrics | none |

Compose sets `ADMIN_TOKEN=dev-admin-token` and `AUTH_SECRET=dev-auth-secret`. A missing or bad token is `401` `unauthorized`. Identity for reserve and cancel is the `user_id` inside the token. The reserve model ignores extra fields, so a `user_id` in that JSON body cannot override the token. Cancel takes no JSON body; its identity comes only from the bearer token.

## Reservation behavior

The idempotency key is scoped to `(user_id, show_id, idempotency_key)`. The request hash is SHA-256 of the sorted seat labels joined by newlines.

| Case | Result |
| --- | --- |
| Same key, same seats, original confirm | `200`, body of the original reservation, header `Idempotent-Replay: true` |
| Same key, same seats, stored decline | Stored `409` body again, header `Idempotent-Replay: true`. The seat is not taken if it later becomes free. |
| Same key, different seats | `409` `idempotency_mismatch` |
| Same key, row still `in_progress` after the lock is acquired | `409` `idempotency_in_progress` |
| Seat already taken, or any seat in a multi-seat request is not available | `409` `seat_taken` |
| Request would put the user over 4 confirmed seats for the show | `409` `per_user_limit` |
| Bad JSON, empty seats, duplicate labels, missing `Idempotency-Key`, unknown seat label | `400` `invalid_request`. The key is not stored. |
| Unknown show | `404` `not_found`. The key is not stored. |

`idempotency_in_progress` exists in the reserve code as a defensive outcome. An ordinary concurrent retry of the same key does not take that path. It contends on the idempotency row and its unique key, waits until the owning transaction commits, and then replays that committed result.

`409` bodies:

```json
{"error": "seat_taken", "message": "one or more seats are no longer available"}
```

```json
{"error": "per_user_limit", "message": "seat request exceeds the per-user limit"}
```

```json
{"error": "idempotency_mismatch", "message": "idempotency key was already used with different seats"}
```

## Cancellation

Cancellation is explicit. There is no hold timeout and no expiry worker.

`GET /shows/{id}` can report a `held` count because that status exists on `seats`. Reserve does not write `held`. A successful reserve sets seats to `confirmed` immediately.

Release matches only rows for this reservation that are still `confirmed`:

```sql
UPDATE seats
SET status = 'available', reservation_id = NULL
WHERE show_id = $show
  AND reservation_id = $reservation
  AND status = 'confirmed'
```

`user_show_counts.confirmed_seat_count` decreases by the number of rows that update freed. A second cancel by the owner returns `409` `already_cancelled` and does not decrement again. After the seat is booked by someone else, cancelling the old reservation returns `409` `already_cancelled` and leaves the new owner's row in place. Another user receives `403` `forbidden`. An unknown reservation id returns `404` `not_found`.

The idempotency row is kept. Replaying the original reserve key returns the same reservation and does not book the seat again.

## Local setup

Clean-checkout verification: verified. A fresh clone was started with `docker compose up --build -d` against an empty PostgreSQL instance. The application initialized the schema automatically, `/health/live` and `/health/ready` both returned 200, and the default burst completed with Overall: PASS.

The app reads environment variables from the process. It does not load a `.env` file. Compose injects the values below.

```bash
git clone https://github.com/jazz42156/seat-reservation-paytm.git
cd seat-reservation-paytm
docker compose up --build
```

The API listens on port 8000. Postgres 16 listens on port 5432 (`seat` / `seat`, database `seat`). Compose waits until Postgres is healthy, then starts the API.

| Variable | Compose value | `.env.example` |
| --- | --- | --- |
| `PORT` | `8000` | `8000` |
| `DATABASE_URL` | `postgresql://seat:seat@db:5432/seat` | `postgresql://seat:seat@localhost:5432/seat` |
| `AUTH_SECRET` | `dev-auth-secret` | `change-me` |
| `ADMIN_TOKEN` | `dev-admin-token` | `change-me` |

If `DATABASE_URL` is unset, `app/settings.py` falls back to `postgresql://seat:seat@localhost:5432/seat`. An empty `AUTH_SECRET` makes `POST /auth/token` return `400`. An empty `ADMIN_TOKEN` makes `POST /shows` return `401`.

Liveness: [http://127.0.0.1:8000/health/live](http://127.0.0.1:8000/health/live)

To run the API on the host instead of in Compose, point `DATABASE_URL` at a reachable Postgres and export `AUTH_SECRET` and `ADMIN_TOKEN` in the shell before uvicorn. On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
$env:DATABASE_URL = "postgresql://seat:seat@localhost:5432/seat"
$env:AUTH_SECRET = "dev-auth-secret"
$env:ADMIN_TOKEN = "dev-admin-token"
.venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 8000
```

On macOS or Linux, use `.venv/bin/pip`, `.venv/bin/uvicorn`, and `export` for the same variables.

## Health checks

`GET /health/live` answers `200` `{"status": "alive"}` when the process can serve HTTP. It does not query the database.

`GET /health/ready` answers `200` only when `SELECT 1` succeeds. If the pool query fails, the response is `503` `{"status": "not_ready"}`. If Postgres is unreachable at process start, startup fails and the process does not serve traffic.

## Metrics

`GET /metrics` exposes:

- `reservations_confirmed_total` — one increment per new `201`
- `reservations_declined_total{reason="seat_taken"}`
- `reservations_declined_total{reason="per_user_limit"}`
- `reservations_declined_total{reason="idempotent_replay"}` — a replay increments only this reason
- `seats_available{show_id="..."}` — set from `COUNT(*)` of `seats` with `status = 'available'`, grouped by show, on each scrape

`idempotency_mismatch` is returned to the client and is not a counter label. There are no `seats_held` or `seats_confirmed` gauges. Held and confirmed counts are on `GET /shows/{id}`.

## Structured logging

Every HTTP request writes one JSON line to stdout.

A valid `X-Request-ID` request header is reused and echoed on the response. Valid means it starts with a letter or digit and then contains up to 127 characters from letters, digits, `.`, `_`, and `-`. Any other value is replaced with a new UUID. The log and the response use that id.

A completion line looks like:

```json
{
  "timestamp": "2026-10-05T12:00:00.000000+00:00",
  "level": "info",
  "event": "request.completed",
  "request_id": "caller-req-1",
  "method": "POST",
  "path": "/shows/...",
  "status_code": 201,
  "duration_ms": 12.4,
  "show_id": "...",
  "reservation_id": "...",
  "user_id": "user-1001",
  "outcome": "confirmed"
}
```

`show_id`, `reservation_id`, `user_id`, and `outcome` are included when the handler set them. An unhandled exception before the response starts is logged as `request.failed` with `exception_type` and answered as plain-text `500`. The line does not include the `Authorization` header, bearer token, `Idempotency-Key`, or request body.

## Testing

Pytest runs in-process against PostgreSQL. Start Compose so Postgres is on `localhost:5432`, install dependencies, and export the same auth values the tests expect (`ADMIN_TOKEN` defaults to `dev-admin-token` inside the tests; `AUTH_SECRET` must be set).

```powershell
$env:DATABASE_URL = "postgresql://seat:seat@localhost:5432/seat"
$env:AUTH_SECRET = "dev-auth-secret"
$env:ADMIN_TOKEN = "dev-admin-token"
.venv\Scripts\pytest
```

Latest reported automated result: **51 passed**.

### Burst

`burst/burst.py` talks to a running API. `ADMIN_TOKEN` must match the server. It defaults to `dev-admin-token` and is not printed. The process exits `0` when its checks pass and `1` when they fail or setup fails.

`--concurrency` limits in-flight reservation calls. `--setup-concurrency` limits token minting and other setup calls. A mixed run (`--requests`) also runs the quick hot-seat, per-user-limit, replay, and mismatch checks first. `--requests` must be `0` or at least `12`.

Quick check (default hot seat 20, per-user attempts 10, concurrency 50):

```bash
python burst/burst.py http://localhost:8000
```

Exact hot seat:

```bash
python burst/burst.py http://localhost:8000 --hot-seat 500 --concurrency 500
```

Mixed workloads:

```bash
python burst/burst.py http://localhost:8000 --requests 2000 --concurrency 200
python burst/burst.py http://localhost:8000 --requests 5000 --concurrency 300
python burst/burst.py http://localhost:8000 --requests 20000 --concurrency 500 --setup-concurrency 25
```

The harness fails the run unless:

- the hot seat has exactly one confirm and every other attempt is `seat_taken`, with no `5xx`, timeouts, or transport errors
- the per-user probe confirms 4 seats and declines the rest as `per_user_limit`
- replay and mismatch checks pass
- sampled `GET /shows/{id}` counts satisfy `available + held + confirmed == total`

Recorded results:

| Run | Result |
| --- | --- |
| 500 users, one seat, concurrency 500 | 1 confirmed, 499 `seat_taken`, 0 `5xx` |
| 2,000 mixed attempts, concurrency 200 | 0 `5xx`, 0 timeouts, 0 transport errors, reconciliation invariant held |
| 20,000 mixed attempts, concurrency 500, setup concurrency 25 | Local Windows + Docker Desktop. Not a pass. Application correctness held; 4 client transport failures. See below. |

The 20,000-request command was:

```bash
python burst/burst.py http://localhost:8000 --requests 20000 --concurrency 500 --setup-concurrency 25
```

The harness reported FAIL because of 4 client-side `httpx.ReadError` transport failures. It did not report a server `5xx`. Observed on that local run:

- 20,000 mixed reservation attempts at concurrency 500
- 0 timeouts
- 0 reconciliation failures, and the final `available + held + confirmed == total` check held
- per-user limit and idempotency checks stayed correct
- server logs showed 19,996 `POST /reserve` requests reached the application
- the other 4 never reached FastAPI and produced no application exception

Those 4 losses are a local transport limit on Windows + Docker Desktop, not an application error response.

## Deployment

Live URL: TODO

Deployment platform: TODO

Public log access: TODO

Local Compose is the documented run path. A public host still needs `DATABASE_URL`, `AUTH_SECRET`, `ADMIN_TOKEN`, and the single-worker container command above.

## Repository history

The repository was developed incrementally and preserves the full Git history. Review `git log` for the development sequence.
