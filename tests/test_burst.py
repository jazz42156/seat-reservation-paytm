import asyncio
import os

import httpx
import pytest

import burst.burst as burst_mod
from app.main import app
from burst.burst import (
    Attempt,
    BurstError,
    Report,
    _create_show,
    _reserve,
    _run_mixed,
    _tokens,
    _transport_error_types,
    classify,
    invariant,
    main,
    map_limited,
    plan_mixed,
    render,
    run_burst,
)

_ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")


def _report(**overrides: object) -> Report:
    values: dict[str, object] = {
        "hot_attempts": 8,
        "hot_confirmed": 1,
        "hot_seat_taken": 7,
        "hot_5xx": 0,
        "hot_transport_errors": 0,
        "hot_timeouts": 0,
        "hot_other": 0,
        "limit_attempts": 6,
        "limit_confirmed": 4,
        "limit_declined": 2,
        "limit_5xx": 0,
        "limit_transport_errors": 0,
        "limit_timeouts": 0,
        "limit_other": 0,
        "per_user_limit": 4,
        "replay_ok": True,
        "mismatch_ok": True,
        "mixed_requests": 0,
        "mixed_concurrency": 50,
        "mixed_confirmed": 0,
        "mixed_replay": 0,
        "mixed_seat_taken": 0,
        "mixed_per_user_limit": 0,
        "mixed_mismatch": 0,
        "mixed_other_4xx": 0,
        "mixed_5xx": 0,
        "mixed_timeouts": 0,
        "mixed_transport_errors": 0,
        "mixed_checks_ok": True,
        "elapsed_seconds": 0.0,
        "requests_per_second": 0.0,
        "during_samples": 2,
        "reconciliation_failures": 0,
        "total": 9,
        "available": 3,
        "held": 0,
        "confirmed": 6,
        "invariant_ok": True,
        "counts_match": True,
        "metrics_available": False,
        "observed_confirmed": 6,
        "observed_seat_taken": 7,
        "observed_per_user_limit": 2,
        "observed_replay": 1,
        "metric_confirmed_delta": 0.0,
        "metric_seat_taken_delta": 0.0,
        "metric_per_user_limit_delta": 0.0,
        "metric_replay_delta": 0.0,
        "transport_error_types": {},
    }
    values.update(overrides)
    return Report(**values)  # type: ignore[arg-type]


def test_render_summary_uses_the_check_names() -> None:
    text = render(_report())

    assert "Hot seat:" in text
    assert "  attempts: 8" in text
    assert "  confirmed: 1" in text
    assert "  seat_taken: 7" in text
    assert "  5xx: 0" in text
    assert "  passed: true" in text
    assert "Per-user limit:" in text
    assert "  per_user_limit: 2" in text
    assert "Idempotency:" in text
    assert "  replay_ok: true" in text
    assert "  mismatch_ok: true" in text
    assert "Mixed burst:" in text
    assert "  requests: 0" in text
    assert "Reconciliation:" in text
    assert "  during_samples: 2" in text
    assert "  failures: 0" in text
    assert "  invariant_ok: true" in text
    assert "Overall:" in text
    assert "  PASS" in text


def test_classify_known_outcomes_and_failures() -> None:
    assert classify(201, None, False) == "confirmed"
    assert classify(200, None, True) == "replay"
    assert classify(409, "seat_taken", False) == "seat_taken"
    assert classify(409, "per_user_limit", False) == "per_user_limit"
    assert classify(409, "idempotency_mismatch", False) == "idempotency_mismatch"
    assert classify(400, "invalid_request", False) == "other_4xx"
    assert classify(500, None, False) == "server_error"
    assert classify(0, None, False, timeout=True) == "timeout"
    assert classify(0, None, False, transport=True) == "transport"


def test_duplicate_winner_limit_breach_and_broken_totals_fail() -> None:
    assert _report().ok
    assert not _report(hot_confirmed=2, hot_seat_taken=6).ok
    assert not _report(hot_5xx=1, hot_seat_taken=6).ok
    assert not _report(hot_transport_errors=1, hot_seat_taken=6).ok
    assert not _report(hot_timeouts=1, hot_seat_taken=6).ok
    assert not _report(limit_confirmed=5, limit_declined=1).ok
    assert not _report(replay_ok=False).ok
    assert not _report(mismatch_ok=False).ok
    assert not _report(invariant_ok=False).ok
    assert not _report(counts_match=False).ok
    assert not _report(reconciliation_failures=1).ok
    assert not _report(during_samples=0).ok
    assert not _report(mixed_requests=20, mixed_5xx=1).ok
    assert not _report(mixed_requests=20, mixed_transport_errors=1).ok
    assert not _report(mixed_requests=20, mixed_timeouts=1).ok
    assert not _report(mixed_requests=20, mixed_other_4xx=1).ok
    assert not _report(mixed_requests=20, mixed_checks_ok=False).ok


def test_reconciliation_invariant() -> None:
    assert invariant({"total": 4, "available": 1, "held": 0, "confirmed": 3})
    assert not invariant({"total": 4, "available": 1, "held": 0, "confirmed": 2})
    assert not invariant({"available": 1})


def test_mixed_plan_covers_each_workload_and_uses_every_request() -> None:
    for count in (12, 24, 30, 2000, 20000):
        labels, wave1, wave2 = plan_mixed(count, "abcd1234")
        jobs = [*wave1, *wave2]
        assert len(jobs) == count
        assert len(labels) == len(set(labels))
        kinds = {job.kind for job in jobs}
        assert {"hot", "limit", "seed", "replay", "mismatch"} <= kinds
    _, wave1, wave2 = plan_mixed(30, "abcd1234")
    assert any(job.kind == "multi" for job in wave1)
    replay = next(job for job in wave2 if job.kind == "replay")
    seed = next(job for job in wave1 if job.kind == "seed" and job.key == replay.key)
    assert seed.seats == replay.seats
    mismatch = next(job for job in wave2 if job.kind == "mismatch")
    assert mismatch.untouched_seat not in next(
        job.seats for job in wave1 if job.key == mismatch.key
    )


async def _peak_concurrency() -> int:
    active = 0
    peak = 0

    async def worker(_item: int) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1

    await map_limited(3, range(12), worker)
    return peak


def test_concurrency_stays_inside_the_semaphore() -> None:
    assert asyncio.run(_peak_concurrency()) == 3


def test_worker_pool_bounds_tasks_and_preserves_order() -> None:
    peak_tasks = 0
    peak_active = 0
    active = 0

    async def worker(item: int) -> int:
        nonlocal peak_tasks, peak_active, active
        active += 1
        peak_active = max(peak_active, active)
        peak_tasks = max(peak_tasks, len(asyncio.all_tasks()))
        await asyncio.sleep(0.02 if item % 2 == 0 else 0.001)
        active -= 1
        return item

    async def scenario() -> tuple[int, int, list[int]]:
        produced = await map_limited(3, range(12), worker)
        return peak_tasks, peak_active, produced

    tasks, active_peak, produced = asyncio.run(scenario())
    assert produced == list(range(12))
    assert active_peak == 3
    assert tasks < 12


def test_token_timeout_cancels_the_pool_without_leaked_tasks() -> None:
    class Client:
        def __init__(self) -> None:
            self.started = 0
            self.peak = 0
            self.active = 0

        async def post(self, path: str, **kwargs: object) -> httpx.Response:
            assert path == "/auth/token"
            self.started += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                await asyncio.sleep(0.05)
                raise httpx.ConnectTimeout("timed out")
            finally:
                self.active -= 1

    client = Client()
    loop_errors: list[dict] = []

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
        with pytest.raises(BurstError, match="create token timed out \\(ConnectTimeout\\)"):
            await _tokens(client, [f"user{index}" for index in range(12)], 3)
        leftover = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        assert leftover == []

    asyncio.run(scenario())
    assert client.peak == 3
    assert client.started == 3
    assert loop_errors == []


def test_show_setup_transport_error_is_a_clean_failure() -> None:
    class Client:
        async def post(self, *args: object, **kwargs: object) -> httpx.Response:
            raise httpx.ReadError("reset")

    with pytest.raises(BurstError, match="create show failed \\(ReadError\\)"):
        asyncio.run(_create_show(Client(), "admin", "show", ["A1"]))  # type: ignore[arg-type]


def test_reservation_timeout_and_transport_stay_counted() -> None:
    class TimeoutClient:
        async def post(self, *args: object, **kwargs: object) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

    class TransportClient:
        async def post(self, *args: object, **kwargs: object) -> httpx.Response:
            raise httpx.ReadError("reset")

    timeout = asyncio.run(_reserve(TimeoutClient(), "show", "user", "token", ["A1"], "key"))  # type: ignore[arg-type]
    transport = asyncio.run(_reserve(TransportClient(), "show", "user", "token", ["A1"], "key"))  # type: ignore[arg-type]
    assert timeout.kind == "timeout"
    assert timeout.exception_type == ""
    assert transport.kind == "transport"
    assert transport.exception_type == "ReadError"


def test_transport_errors_are_printed_by_exception_type() -> None:
    def failed(exception_type: str) -> Attempt:
        return Attempt(
            kind="transport",
            status_code=0,
            error=None,
            reservation_id=None,
            replay=False,
            exception_type=exception_type,
        )

    attempts = [
        *[failed("ConnectError") for _ in range(3)],
        *[failed("ReadError") for _ in range(2)],
        Attempt(
            kind="timeout",
            status_code=0,
            error=None,
            reservation_id=None,
            replay=False,
            exception_type="ReadTimeout",
        ),
    ]
    counts = _transport_error_types(attempts)
    report = _report(
        hot_transport_errors=3,
        hot_seat_taken=4,
        mixed_requests=20,
        mixed_transport_errors=2,
        transport_error_types=counts,
    )

    assert counts == {"ConnectError": 3, "ReadError": 2}
    assert not report.ok
    text = render(report)
    assert "  transport_errors: 3" in text
    assert "  transport_errors: 2" in text
    assert "Transport errors:\n  ConnectError: 3\n  ReadError: 2" in text
    assert "Traceback" not in text
    assert "ReadTimeout" not in text
    assert "Transport errors:" not in render(_report())


def test_main_prints_a_concise_setup_failure(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    async def boom(*args: object, **kwargs: object) -> Report:
        raise BurstError("create token timed out (ConnectTimeout)")

    monkeypatch.setattr(burst_mod, "run_burst", boom)
    code = main(["http://127.0.0.1:9"])
    captured = capsys.readouterr()
    assert code == 1
    assert captured.err.strip() == "setup failed: create token timed out (ConnectTimeout)"
    assert "Traceback" not in captured.err
    assert captured.out == ""


def test_setup_limit_is_independent_and_timer_ignores_provisioning(monkeypatch: pytest.MonkeyPatch) -> None:
    token_active = 0
    token_peak = 0

    async def slow_tokens(client: object, user_ids: list[str], concurrency: int) -> list[str]:
        nonlocal token_active, token_peak
        assert concurrency == 2

        async def mint(user_id: str) -> str:
            nonlocal token_active, token_peak
            token_active += 1
            token_peak = max(token_peak, token_active)
            await asyncio.sleep(0.03)
            token_active -= 1
            return user_id

        return await map_limited(concurrency, user_ids, mint)

    async def fast_perform(
        client: object,
        show_id: str,
        jobs: list,
        tokens: dict[str, str],
        concurrency: int,
        sampler: object,
    ) -> list[Attempt]:
        assert concurrency == 8
        await asyncio.sleep(0.01)
        return [
            Attempt(
                kind="timeout",
                status_code=0,
                error=None,
                reservation_id=None,
                replay=False,
                seats=job.seats,
                user_id=job.user_id,
                key=job.key,
            )
            for job in jobs
        ]

    async def create_show(*args: object, **kwargs: object) -> str:
        return "show-1"

    async def read_show(*args: object, **kwargs: object) -> dict:
        return {
            "counts": {"total": 1, "available": 1, "held": 0, "confirmed": 0},
            "seats": [{"seat_label": "H0", "status": "available"}],
        }

    monkeypatch.setattr(burst_mod, "_tokens", slow_tokens)
    monkeypatch.setattr(burst_mod, "_perform", fast_perform)
    monkeypatch.setattr(burst_mod, "_create_show", create_show)
    monkeypatch.setattr(burst_mod, "_show", read_show)

    class Client:
        async def get(self, path: str) -> httpx.Response:
            raise httpx.ConnectError("down")

    async def scenario() -> burst_mod._Mixed:
        return await _run_mixed(
            Client(),  # type: ignore[arg-type]
            admin_token="admin",
            requests=12,
            concurrency=8,
            setup_concurrency=2,
            per_user_limit=4,
        )

    mixed = asyncio.run(scenario())
    assert token_peak == 2
    assert mixed.requests == 12
    assert mixed.elapsed_seconds < 0.1
    assert mixed.timeouts == 12


async def _burst_against_app(**kwargs: int) -> Report:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            timeout=30.0,
        ) as client:
            return await run_burst(client, admin_token=_ADMIN, **kwargs)


def test_burst_against_the_service() -> None:
    report = asyncio.run(_burst_against_app(hot_seat=8, limit_attempts=6, concurrency=8))

    assert report.hot_confirmed == 1
    assert report.hot_seat_taken == 7
    assert report.hot_5xx == 0
    assert report.hot_transport_errors == 0
    assert report.limit_confirmed == 4
    assert report.limit_declined == 2
    assert report.limit_5xx == 0
    assert report.replay_ok
    assert report.mismatch_ok
    assert report.during_samples >= 1
    assert report.reconciliation_failures == 0
    assert report.available + report.held + report.confirmed == report.total
    assert report.confirmed == 6
    assert report.ok
    text = render(report)
    assert "invariant_ok: true" in text
    assert "  PASS" in text


def test_small_mixed_burst_reconciles_during_load() -> None:
    report = asyncio.run(
        _burst_against_app(hot_seat=4, limit_attempts=6, requests=24, concurrency=4)
    )

    assert report.mixed_requests == 24
    assert report.mixed_5xx == 0
    assert report.mixed_timeouts == 0
    assert report.mixed_transport_errors == 0
    assert report.mixed_other_4xx == 0
    assert report.during_samples >= 1
    assert report.reconciliation_failures == 0
    assert report.invariant_ok
    assert report.ok
    assert report.metrics_available
    assert report.metric_confirmed_delta == report.observed_confirmed
    assert report.metric_seat_taken_delta == report.observed_seat_taken
    assert report.metric_per_user_limit_delta == report.observed_per_user_limit
    assert report.metric_replay_delta == report.observed_replay
