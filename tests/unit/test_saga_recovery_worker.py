"""SagaRecovery worker loop: stop-aware idle sleep and accurate failure log.

Regression tests for the loop's idle sleep must wake the moment
``stop`` is set (SIGTERM never waits out a full interval), and the failure log
must not claim a backoff the loop does not have. No DB: ``sweep_once`` is
instance-shadowed, so the ``None`` sessionmaker is never touched.
"""

import asyncio
import logging
from typing import cast

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.shared.config.setting import AppSettings
from src.shared.saga_recovery import SagaRecovery, run_recovery

_DSN = "postgresql+asyncpg://u:p@localhost:5432/db"


def _recovery() -> SagaRecovery:
    """Recovery with a shadowed sweep — the sessionmaker is never used."""
    return SagaRecovery(cast(async_sessionmaker, None), None, None)


async def test_run_returns_promptly_when_stop_is_set_during_idle_sleep():
    """SIGTERM during the idle sleep must cut the sleep short: pre-fix a plain
    ``asyncio.sleep`` made ``run`` wait out the full 0.5s interval after stop."""
    stop = asyncio.Event()
    recovery = _recovery()

    async def no_work() -> dict[str, int]:
        return {"completed": 0, "compensated": 0, "deferred": 0}

    recovery.sweep_once = no_work  # type: ignore[method-assign]  # instance shadow — no DB

    task = asyncio.create_task(recovery.run(0.5, stop))
    await asyncio.sleep(0.1)  # first pass is done; the poller sits in its idle sleep
    stop.set()
    await asyncio.wait_for(task, timeout=0.25)


async def test_failure_log_says_interval_not_backoff():
    """A failing sweep logs 'retrying after interval' — the loop retries at a
    fixed interval, so the old 'retrying after backoff' wording was misleading."""
    stop = asyncio.Event()
    recovery = _recovery()
    calls = 0

    async def fails_once() -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("sweep blew up")
        stop.set()
        return {"completed": 0, "compensated": 0, "deferred": 0}

    recovery.sweep_once = fails_once  # type: ignore[method-assign]  # instance shadow — no DB

    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logger = logging.getLogger("src.shared.saga_recovery")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        await asyncio.wait_for(recovery.run(0.01, stop), timeout=2)
    finally:
        logger.removeHandler(handler)

    rendered = [record.getMessage() for record in records]
    assert any("retrying after interval" in line for line in rendered)
    assert all("backoff" not in line for line in rendered)


_SETTINGS = AppSettings(database_url=_DSN, checkout_saga_recovery_poll_interval_seconds=3.25)


async def test_run_recovery_uses_the_dedicated_saga_recovery_poll_interval(monkeypatch):
    """``run_recovery`` must hand the loop its OWN poll interval
    (``checkout_saga_recovery_poll_interval_seconds``) — it used to borrow the
    reservation reaper's, so retuning one worker silently retuned the other."""
    seen: list[float] = []

    async def fake_run(self: SagaRecovery, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        seen.append(poll_interval)

    monkeypatch.setattr(SagaRecovery, "run", fake_run)

    await run_recovery(_SETTINGS, cast(async_sessionmaker, None), None)

    assert seen == [3.25]
