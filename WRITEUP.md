# Write-up

Design notes for the seat reservation service. Behavior below is what the current code does.

## 1. Atomic reservation

`reserve_seats` in `app/reserve.py` sorts the seat labels, hashes them, and runs the claim in one database transaction. The sequence is:

1. Load `price_paise` and `per_user_limit` for the show. Missing show returns `404` and does not write an idempotency row.
2. Check that every requested label exists. An unknown label returns `400` and does not write an idempotency row.
3. Reject the request before any idempotency write if `price_paise * seat count` exceeds the signed 64-bit maximum.
4. Insert `idempotency_keys` for `(user_id, show_id, idempotency_key)` with `outcome = 'in_progress'` and the request hash. `ON CONFLICT DO NOTHING`. If the insert wins, this request owns the key. If it loses, the existing row is locked with `SELECT ... FOR UPDATE` and the stored result is used (section 5).
5. Open an inner `connection.transaction()`. asyncpg implements a nested transaction as a savepoint. Inside it:
   - Lock and conditionally increment `user_show_counts`.
   - `SELECT ... FOR UPDATE` each requested seat in sorted `seat_label` order.
   - Insert `reservations` (`status = 'confirmed'`) and `reservation_seats`.
   - `UPDATE seats SET status = 'confirmed', reservation_id = $reservation WHERE show_id = $show AND seat_label = $seat AND status = 'available'`.
6. On success, store `outcome = 'confirmed'` and the `201` body on the idempotency row, then commit the outer transaction.
7. On `seat_taken` or `per_user_limit`, the exception leaves the inner transaction, so the savepoint rolls back the counter change and any seat writes. The outer transaction then stores that `409` body on the idempotency row and commits.

`DeadlockDetectedError` and `SerializationError` abort the attempt. `reserve_seats` retries the whole transaction up to three times. The retry handler covers those two errors.

Cancel uses its own transaction and the same deadlock/serialization retry. Its lock order is the reservation row, then that reservation's seat rows sorted by `seat_label`, then `user_show_counts` when at least one seat was released.

## 2. Why a seat cannot be sold twice

Two transactions that want the same seat both take `FOR UPDATE` on that row, in label order. The second waits until the first commits or rolls back.

The state change is conditional:

```sql
UPDATE seats
SET status = 'confirmed', reservation_id = $reservation
WHERE show_id = $show
  AND seat_label = $seat
  AND status = 'available'
```

The winner's update matches one row and commits `confirmed`. After that commit the loser's `FOR UPDATE` proceeds, sees a status other than `available`, and raises `seat_taken`. If the status check and the update ever disagree, zero rows from the `UPDATE` is also `seat_taken`. The inner savepoint removes the loser's reservation insert and counter increment. PostgreSQL does not commit two `confirmed` rows for one `(show_id, seat_label)`.

## 3. Deadlock avoidance

Every reserve locks in the same order: idempotency key, then the user/show counter, then seat labels sorted ascending. Two multi-seat requests that overlap cannot lock seats in opposite orders, which is the usual seat-booking deadlock.

Cancel locks the reservation first, then its seats in the same label order, then the counter. Reserve and cancel can still deadlock if their lock orders cross. Those two errors are retried up to three times inside the request. They are not treated as a normal `409`.

## 4. Per-user limit

`user_show_counts` stores `confirmed_seat_count` for one user and one show. The check and the increment are one statement:

```sql
INSERT INTO user_show_counts (user_id, show_id, confirmed_seat_count)
VALUES ($user, $show, 0)
ON CONFLICT (user_id, show_id)
DO UPDATE SET confirmed_seat_count = user_show_counts.confirmed_seat_count
```

The `DO UPDATE` is a no-op assignment so the existing row is locked. The next statement updates only when the new total still fits:

```sql
UPDATE user_show_counts
SET confirmed_seat_count = confirmed_seat_count + $seat_count
WHERE user_id = $user
  AND show_id = $show
  AND confirmed_seat_count + $seat_count <= $per_user_limit
```

Zero rows updated is `409` `per_user_limit`. Concurrent requests for the same user serialize on that row. The column default and the create path both leave `shows.per_user_limit` at 4. A decline rolls the increment back with the savepoint. Cancel subtracts only the number of seats its `UPDATE` actually released, and only when the stored count is at least that large.

## 5. Idempotency

This provides an exactly-once reservation effect for a given idempotency key among API processes sharing the same PostgreSQL database. There is no payment provider in this assignment. This does not claim distributed payment exactly-once semantics. PostgreSQL is the source of truth for idempotency.

The row lives in `idempotency_keys`. Primary key: `(user_id, show_id, idempotency_key)`. The hash is SHA-256 over the sorted, trimmed seat labels joined by `\n`. Seat order in the JSON body does not change the hash. The header is `Idempotency-Key`.

| Situation | What is stored | HTTP |
| --- | --- | --- |
| Row still `in_progress` after the waiter acquires the lock | `in_progress`, null response | `409` `idempotency_in_progress`. This state exists in code. An ordinary concurrent retry waits on the row lock and then replays the committed outcome. |
| First request confirmed | `confirmed` plus the `201` body | Later same seats: `200`, same body, `Idempotent-Replay: true` |
| First request declined | `seat_taken` or `per_user_limit` plus the `409` body | Later same seats replay that `409` with `Idempotent-Replay: true` |
| Same key, different labels | Original row unchanged | `409` `idempotency_mismatch` |

A stored decline stays a decline. The replay does not look at the seat again, so a later cancel does not let that key book the seat. Validation failures (`400`) and unknown shows (`404`) return before the insert, so those keys are not consumed.

A replay increments `reservations_declined_total{reason="idempotent_replay"}` only. It does not increment `reservations_confirmed_total`.

## 6. Multi-seat requests

All requested seats are locked and updated in one savepoint. If any label is missing, not `available`, or fails the conditional `UPDATE`, the decline rolls back every seat write and the counter increment for that request. The client gets one `409` `seat_taken`. There is no partial confirm.

## 7. Cancellation

The assignment needs a confirmed seat to become available again, and a late cancel must not undo a newer booking. An expiry clock would release seats without an owner action and would need a sweeper. This service confirms immediately and releases only on `POST /reservations/{id}/cancel`.

The schema still allows `held`, and `GET /shows/{id}` counts that status. No request path writes `held`, and nothing expires a hold.

The owner check is the token user against `reservations.user_id`. Someone else gets `403` `forbidden`. The release update requires both `reservation_id` and `status = 'confirmed'`. After a successful cancel the reservation status is `cancelled`. A second call sees that status and returns `409` `already_cancelled` without touching seats or the counter.

The rebooking case covered by tests is: owner cancels, another user confirms the seat, the first owner cancels again. The second call is `409` `already_cancelled`. The new row stays `confirmed` for the new reservation id. Even if a cancel ran while seats had already moved to another `reservation_id`, the `WHERE reservation_id = $this` clause would update zero of those rows, and the counter would decrease only by the rows actually returned.

## 8. Database unavailable

Selling the same seat twice is worse than refusing the sale. When PostgreSQL cannot be reached at startup, `connect` fails and the process does not start serving. When a later readiness query fails, `GET /health/ready` returns `503` `{"status": "not_ready"}`. `GET /health/live` can still return `200` because it does not query the database.

Reservation writes go through the pool. There is no in-memory seat ledger that can confirm a booking while Postgres is down. The service chooses consistency over availability for seat allocation. During an API-to-PostgreSQL network partition, the API cannot prove current seat ownership, so reservation writes fail closed rather than proceeding from stale or in-memory state. Liveness can still return 200. Readiness fails when the database is not reachable. Reservation writes do not proceed without PostgreSQL.

## 9. Scaling

What runs today is one API process, one asyncpg pool of at most 10 connections, and one PostgreSQL 16 instance. Seat truth stays in `seats`. Gauges and `GET /shows/{id}` read that table.

Not implemented, and the next steps if one show or one database became the limit:

- Run more API processes only after measuring pool wait time. Each process opens its own pool, so worker count times pool size has to stay under Postgres `max_connections`.
- Tune Postgres (connections, memory, autovacuum) and the pool size from those measurements. The pool is 10 today, not a production setting.
- Put a connection pooler such as PgBouncer in front of Postgres if many API processes would otherwise hold too many server connections.
- Partition or shard by `show_id` if one database can no longer take the write rate. A reservation does not cross shows, so the transaction boundary already matches that split.
- Cache read-heavy availability for display. Confirmations would still use the conditional `UPDATE`. The cache would not be the source of truth.

## 10. Observability

Implemented now:

- `GET /metrics` with `reservations_confirmed_total`, `reservations_declined_total` for `seat_taken`, `per_user_limit`, and `idempotent_replay`, and `seats_available{show_id}` read from PostgreSQL on scrape.
- One JSON stdout line per request: timestamp, level, event, request id, method, path, status, duration, and when present show id, reservation id, user id, and outcome. Failures add `exception_type`.
- `X-Request-ID` is taken from the caller when the header matches the allowed pattern, otherwise generated, then returned on the response and written on the log line.
- `GET /health/ready` for Postgres reachability, separate from liveness.
- The burst tool samples `GET /shows/{id}` during a run and checks `available + held + confirmed == total`.

Proposed alerts, not wired up in this repo:

- Any `5xx` during a sale, including a non-zero rate on reservation and cancel routes.
- Readiness failing or flapping.
- Reservation latency above the normal commit time, as a sign of lock waits.
- A spike in `seat_taken` or `per_user_limit` that does not match an expected on-sale shape. Those declines are correct outcomes, so the page is for a change in rate, not for a single `409`.
- Pool or database connection saturation: clients waiting for a connection, or Postgres near `max_connections`.
- Reconciliation drift: `seats_available` for a show disagreeing with `GET /shows/{id}`, or `available + held + confirmed` not equal to the seat count.

At 2am the request id joins the access log to one reservation attempt. Readiness says whether Postgres is usable. The counters say whether new confirms are happening and why attempts are declining. The show endpoint is the seat inventory check.

## 11. Load-test results

Verified:

| Run | Result |
| --- | --- |
| 500 simultaneous users, one seat | 1 confirmed, 499 `seat_taken`, 0 `5xx` |
| 2,000 mixed reservation attempts, concurrency 200 | 0 `5xx`, 0 timeouts, 0 transport errors, 0 reconciliation failures |
| Automated pytest | 51 passed |
| 20,000 mixed attempts, concurrency 500, setup concurrency 25 | Local Windows + Docker Desktop. Not a pass. See below. |

Command:

```bash
python burst/burst.py http://localhost:8000 --requests 20000 --concurrency 500 --setup-concurrency 25
```

That local run sent 20,000 mixed reservation attempts. The application side stayed correct: 0 server-side `5xx`, 0 timeouts, 0 reconciliation failures, the final seat invariant held, and the per-user limit and idempotency checks stayed correct. The harness still failed because of 4 client-side `httpx.ReadError` transport failures. Server logs showed 19,996 reserve requests reached the application. The other 4 never reached FastAPI and produced no application exception. This is a local Windows + Docker Desktop transport limit, not an application error response.

Pending:

| Run | Result |
| --- | --- |
| Burst against the public deployment | TODO. No public URL yet. |

## 12. AI usage

AI was directed to inspect the requirements, propose edge cases, help generate and review tests, help build and review the burst harness, analyze failures, and help draft this documentation.

Engineering decisions were made and validated around PostgreSQL as the source of truth, deterministic seat locking, the conditional seat update, all-or-nothing multi-seat behavior, explicit cancellation instead of expiry, database-backed idempotency, per-user limit enforcement, and the observability scope.

AI-generated suggestions were checked with automated tests, manual HTTP checks, and the concurrency and burst runs. The public-deployment burst has not been run.

## 13. What I would do next

- Deploy the existing container and Postgres, set `DATABASE_URL`, `AUTH_SECRET`, and `ADMIN_TOKEN`, and record the public URL.
- Size the pool and Postgres from the local 20,000-request run. Application correctness held; the four lost requests were client transport failures on Windows + Docker Desktop.
- Add a longer Locust (or equivalent) run after the correctness burst, aimed at sustained throughput rather than a single invariant check.
- Put the existing metrics and readiness check on a dashboard and add the alerts in section 10.
- Repeat the hot-seat and mixed bursts with more than one API process.
- Add payments only if the scope grows. Reservation idempotency would stay in Postgres; a payment would need its own idempotency story.
