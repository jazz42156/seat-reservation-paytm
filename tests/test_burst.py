import asyncio
import os

import httpx

from app.main import app
from burst.burst import Report, render, run_burst

_ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")


def _report(**overrides: object) -> Report:
    values: dict[str, object] = {
        "hot_confirmed": 1,
        "hot_seat_taken": 7,
        "hot_5xx": 0,
        "hot_other": 0,
        "hot_requests": 8,
        "limit_confirmed": 4,
        "limit_declined": 2,
        "limit_5xx": 0,
        "limit_other": 0,
        "limit_attempts": 6,
        "per_user_limit": 4,
        "replay_ok": True,
        "mismatch_ok": True,
        "total": 9,
        "available": 3,
        "held": 0,
        "confirmed": 6,
        "invariant_ok": True,
        "counts_match": True,
    }
    values.update(overrides)
    return Report(**values)  # type: ignore[arg-type]


def test_render_summary_uses_the_check_names() -> None:
    text = render(_report())

    assert "Hot seat:" in text
    assert "  confirmed: 1" in text
    assert "  seat_taken: 7" in text
    assert "  5xx: 0" in text
    assert "Per-user limit:" in text
    assert "  per_user_limit: 2" in text
    assert "Idempotency:" in text
    assert "  replay_ok: true" in text
    assert "  mismatch_ok: true" in text
    assert "Reconciliation:" in text
    assert "  invariant_ok: true" in text


def test_duplicate_winner_limit_breach_and_broken_totals_fail() -> None:
    assert _report().ok
    assert not _report(hot_confirmed=2, hot_seat_taken=6).ok
    assert not _report(hot_5xx=1, hot_seat_taken=6).ok
    assert not _report(limit_confirmed=5, limit_declined=1).ok
    assert not _report(replay_ok=False).ok
    assert not _report(mismatch_ok=False).ok
    assert not _report(invariant_ok=False).ok
    assert not _report(counts_match=False).ok


async def _burst_against_app() -> Report:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            timeout=30.0,
        ) as client:
            return await run_burst(
                client,
                admin_token=_ADMIN,
                hot_seat=8,
                limit_attempts=6,
            )


def test_burst_against_the_service() -> None:
    report = asyncio.run(_burst_against_app())

    assert report.hot_confirmed == 1
    assert report.hot_seat_taken == 7
    assert report.hot_5xx == 0
    assert report.limit_confirmed == 4
    assert report.limit_declined == 2
    assert report.limit_5xx == 0
    assert report.replay_ok
    assert report.mismatch_ok
    assert report.available + report.held + report.confirmed == report.total
    assert report.confirmed == 6
    assert report.ok
    assert "invariant_ok: true" in render(report)
