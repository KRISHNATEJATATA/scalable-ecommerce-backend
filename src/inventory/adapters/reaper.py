"""Reservation reaper — the `service`-role poller that stops stalled sagas from
leaking stock.

A reservation bumps ``inventory.reserved`` the moment it's taken. If the checkout
saga that took it dies (crash, timeout, abandoned payment), nothing ever releases
it and those units stay invisible forever — a *phantom* oversell-block: stock on
the shelf that no one can buy. The reaper is the backstop: every pass releases
whatever is still ``held`` past its ``expires_at``, emitting ``StockReleased``
through the same transactional outbox as a normal release.

One transaction per batch, claimed with ``FOR UPDATE SKIP LOCKED``, so N replicas
split the work rather than double-releasing a row. Safe to run continuously
(``python -m src.inventory.adapters.reaper``, as in docker-compose) or as a
scheduled one-shot in prod (EventBridge → ECS task with ``--once``); TTL expiry is
absolute, so pass frequency only affects how quickly stock comes back.

This module is a **scheduling shell**: it owns the loop, the signal handling and
the session, and delegates the actual sweep to
:meth:`~src.inventory.application.service.InventoryService.release_expired`. Like
every other worker here it builds the repository only to hand it to the service —
Route/Worker → Service → Repository is never short-circuited.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class ReservationReaper:
    """Releases expired holds in batches until stopped."""

    def __init__(self, sessionmaker: async_sessionmaker, *, batch_size: int, reservation_ttl_seconds: int) -> None:
        self._sessionmaker = sessionmaker
        self._batch = batch_size
        self._ttl_seconds = reservation_ttl_seconds

    async def sweep_once(self) -> int:
        """One batch: release expired holds, return how many were released."""
        async with self._sessionmaker() as session:
            service = InventoryService(InventoryRepository(session), reservation_ttl_seconds=self._ttl_seconds)
            return await service.release_expired(self._batch)

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle.

        A full batch means there is likely more waiting, so the next pass runs
        immediately — the same drain-then-sleep shape as the outbox relay.
        """
        while stop is None or not stop.is_set():
            try:
                released = await self.sweep_once()
            except Exception:  # boundary: one bad pass must not kill the reaper
                log.exception("reaper pass failed; retrying after backoff")
                released = 0
            if released < self._batch:
                await asyncio.sleep(poll_interval)


async def run_reaper(
    settings: AppSettings, sessionmaker: async_sessionmaker, *, stop: asyncio.Event | None = None, once: bool = False
) -> int:
    """Build a reaper from settings and run it (one sweep with ``once=True``)."""
    reaper = ReservationReaper(
        sessionmaker,
        batch_size=settings.reservation_reaper_batch_size,
        reservation_ttl_seconds=settings.reservation_ttl_seconds,
    )
    if once:
        return await reaper.sweep_once()
    await reaper.run(settings.reservation_reaper_poll_interval_seconds, stop=stop)
    return 0


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.inventory.adapters.reaper [--once]` — the `service`-role reaper."""
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

    parser = argparse.ArgumentParser(description="Release expired inventory reservations.")
    parser.add_argument("--once", action="store_true", help="run a single sweep and exit (scheduled-task mode)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    # Looping: Prometheus scrapes us. `--once`: we exit before any scrape, so push
    # at the end instead. Both no-ops unless configured.
    if not args.once:
        serve_worker_metrics(settings, job="reservation-reaper")
    engine = create_engine(settings)
    sessionmaker = create_sessionmaker(engine)
    log.info("reservation reaper starting (once=%s)", args.once)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_reaper(settings, sessionmaker, stop=stop, once=args.once)
        finally:
            await engine.dispose()

    asyncio.run(_run())
    if args.once:
        push_worker_metrics(settings, job="reservation-reaper")


if __name__ == "__main__":  # pragma: no cover
    main()
