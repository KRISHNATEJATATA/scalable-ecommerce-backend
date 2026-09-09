"""ReservationReaper worker loop: stop-aware idle sleep and accurate failure log.

Regression tests for the loop's idle sleep must wake the moment
``stop`` is set (SIGTERM never waits out a full interval), and the failure log
must not claim a backoff the loop does not have. No DB: ``sweep_once`` is
instance-shadowed, so the ``None`` sessionmaker is never touched.
"""

import asyncio
import logging
from typing import cast

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.inventory.adapters.reaper import ReservationReaper


def _reaper() -> ReservationReaper:
    """Reaper with a shadowed sweep — the sessionmaker is never used."""
    return ReservationReaper(cast(async_sessionmaker, None), batch_size=10, reservation_ttl_seconds=900)


async def test_run_returns_promptly_when_stop_is_set_during_idle_sleep():
    """SIGTERM during the idle sleep must cut the sleep short: pre-fix a plain
    ``asyncio.sleep`` made ``run`` wait out the full 0.5s interval after stop."""
    stop = asyncio.Event()
    reaper = _reaper()

    async def no_work() -> int:
        return 0

    reaper.sweep_once = no_work  # type: ignore[method-assign]  # instance shadow — no DB

    task = asyncio.create_task(reaper.run(0.5, stop))
    await asyncio.sleep(0.1)  # first pass is done; the reaper sits in its idle sleep
    stop.set()
    await asyncio.wait_for(task, timeout=0.25)


async def test_failure_log_says_interval_not_backoff():
    """A failing sweep logs 'retrying after interval' — the loop retries at a
    fixed interval, so the old 'retrying after backoff' wording was misleading."""
    stop = asyncio.Event()
    reaper = _reaper()
    calls = 0

    async def fails_once() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("sweep blew up")
        stop.set()
        return 0

    reaper.sweep_once = fails_once  # type: ignore[method-assign]  # instance shadow — no DB

    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logger = logging.getLogger("src.inventory.adapters.reaper")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        await asyncio.wait_for(reaper.run(0.01, stop), timeout=2)
    finally:
        logger.removeHandler(handler)

    rendered = [record.getMessage() for record in records]
    assert any("retrying after interval" in line for line in rendered)
    assert all("backoff" not in line for line in rendered)
