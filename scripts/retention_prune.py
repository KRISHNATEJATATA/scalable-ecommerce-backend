"""Retention prune — one shared growth-hygiene sweep over terminal DB history.

Four growth paths are written forever and read never (after their settlement
window), so without pruning they are a pure write-amplification tax on every
committed transaction:

* ``published_at IS NOT NULL`` outbox rows — already shipped; the relay claims
  only unpublished rows, the reconciler never reads the outbox. One ``outbox``
  table per event-publishing module schema (5).
* ``inventory.reservations`` rows in a terminal status (``released`` /
  ``committed``) — the settlement window's retry-safety bookkeeping, dead
  weight after it closes.
* ``orders.saga_log`` rows of orders in a terminal status — the recovery
  poller journals only live (``pending``) checkouts.

One script, not five workers: it crosses every module boundary already (like
``scripts/bus_bootstrap.py`` — import-linter boundaries apply to ``src/``
modules, not scripts), and the deletes are per-table batches so a pass never
holds a long lock on a hot table. Run as a looping compose service locally
(``python -m scripts.retention_prune``) or as an EventBridge-scheduled
``--once`` ECS task in prod — the reaper's two shapes (RUNBOOK §8).

The reservation/saga-log retentions are guarded to exceed the payment
reconciliation window (``AppSettings`` validator): pruning them earlier would
let a recovery replay under-count committed rows and compensate a settled
order. Outbox retention has no such coupling (terminal once shipped).

Liveness is liveness-by-backlog, same doctrine as the reaper: a prune that
never runs emits nothing, so alert on the prunable-row backlog query in
``ops/prometheus/`` — ``retention_pruned_total{table}`` says what ran.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from typing import Any, cast

from prometheus_client import Counter
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.inventory.domain.reservation import ReservationStatus
from src.orders.domain.order import OrderStatus
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger("retention_prune")

#: Every schema that owns an ``outbox`` table (one per event-publishing module).
_OUTBOX_SCHEMAS = ("catalog", "inventory", "orders", "payments", "identity")

#: Terminal = everything but the one live status. Derived from the domain enums
#: (the single source of truth) so a new status can't silently become prunable.
_TERMINAL_RESERVATION_STATUSES = tuple(s.value for s in ReservationStatus if s is not ReservationStatus.HELD)
_TERMINAL_ORDER_STATUSES = tuple(s.value for s in OrderStatus if s is not OrderStatus.PENDING)

pruned_total = Counter(
    "retention_pruned_total",
    "Rows deleted by the retention prune, labelled by table.",
    ["table"],
)

# Each DELETE claims one batch per statement; the caller loops until a batch
# comes back short. The inner SELECT is the limiter (ORDER BY the terminal age,
# so the oldest history goes first); the outer DELETE's join on the primary key
# keeps each statement's lock footprint to exactly the rows it removes.
_OUTBOX_SQL = (
    "DELETE FROM {schema}.outbox WHERE id IN ("
    "SELECT id FROM {schema}.outbox "
    "WHERE published_at IS NOT NULL AND occurred_at < now() - make_interval(days => :days) "
    "ORDER BY occurred_at LIMIT :batch)"
)
_RESERVATIONS_SQL = (
    "DELETE FROM inventory.reservations WHERE id IN ("
    "SELECT id FROM inventory.reservations "
    "WHERE status = ANY(:terminal) AND updated_at < now() - make_interval(days => :days) "
    "ORDER BY updated_at LIMIT :batch)"
)
_SAGA_LOG_SQL = (
    "DELETE FROM orders.saga_log WHERE order_id IN ("
    "SELECT id FROM orders.orders "
    "WHERE status = ANY(:terminal) AND updated_at < now() - make_interval(days => :days) "
    "ORDER BY updated_at LIMIT :batch)"
)


async def _prune_batched(session: AsyncSession, sql: str, params: dict, batch: int) -> int:
    """Batched delete until a pass deletes fewer than ``batch``; returns the total.

    One short DELETE per batch: a prune must never hold a long lock on a table
    the relay (SKIP LOCKED on the unpublished partial index) and every module's
    write path are touching. Commit per batch — a slow table drains over many
    passes instead of one transaction.
    """
    total = 0
    while True:
        # DML via execute() is a CursorResult at runtime; the static type is the
        # broader Result (whose ``rowcount`` the stubs hide).
        result = cast(CursorResult[Any], await session.execute(text(sql), {**params, "batch": batch}))
        deleted = int(result.rowcount or 0)
        await session.commit()
        total += deleted
        if deleted < batch:
            return total


async def prune_once(sessionmaker: async_sessionmaker, settings: AppSettings) -> dict[str, int]:
    """One full pass over every retention path; returns rows deleted per table."""
    batch = settings.retention_prune_batch_size
    counts: dict[str, int] = {}
    async with sessionmaker() as session:
        for schema in _OUTBOX_SCHEMAS:
            counts[f"{schema}.outbox"] = await _prune_batched(
                session,
                _OUTBOX_SQL.format(schema=schema),
                {"days": settings.outbox_retention_days},
                batch,
            )
        counts["inventory.reservations"] = await _prune_batched(
            session,
            _RESERVATIONS_SQL,
            {"days": settings.reservation_retention_days, "terminal": list(_TERMINAL_RESERVATION_STATUSES)},
            batch,
        )
        counts["orders.saga_log"] = await _prune_batched(
            session,
            _SAGA_LOG_SQL,
            {"days": settings.saga_log_retention_days, "terminal": list(_TERMINAL_ORDER_STATUSES)},
            batch,
        )
    for label, deleted in counts.items():
        if deleted:
            pruned_total.labels(table=label).inc(deleted)
        log.info("pruned %d %s row(s)", deleted, label)
    return counts


async def run_prune(
    sessionmaker: async_sessionmaker, settings: AppSettings, *, stop: asyncio.Event | None = None, once: bool = False
) -> int:
    """One pass with ``once=True``; otherwise loop on the poll interval until stopped.

    Unlike the reaper there is no drain-then-sleep shape: a pass deletes
    everything eligible in batches already, and the backlog is dead history —
    cadence, not urgency.
    """
    if once:
        return sum((await prune_once(sessionmaker, settings)).values())
    while stop is None or not stop.is_set():
        try:
            await prune_once(sessionmaker, settings)
        except Exception:  # boundary: one bad pass must not kill the prune
            log.exception("retention prune pass failed; retrying after interval")
        if stop is None:
            await asyncio.sleep(settings.retention_prune_poll_interval_seconds)
            continue
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=settings.retention_prune_poll_interval_seconds)
    return 0


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m scripts.retention_prune [--once]` — the shared retention prune."""
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

    parser = argparse.ArgumentParser(
        description="Prune published outbox, terminal reservation and settled saga_log rows."
    )
    parser.add_argument("--once", action="store_true", help="run a single pass and exit (scheduled-task mode)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    # Looping: Prometheus scrapes us. `--once`: we exit before any scrape, so push
    # at the end instead. Both no-ops unless configured.
    if not args.once:
        serve_worker_metrics(settings, job="retention-prune")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    log.info("retention prune starting (once=%s)", args.once)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows Proactor loop has no add_signal_handler
                signal.signal(sig, lambda *_: stop.set())
        try:
            await run_prune(sessionmaker, settings, stop=stop, once=args.once)
        finally:
            await engine.dispose()

    asyncio.run(_run())
    if args.once:
        push_worker_metrics(settings, job="retention-prune")


if __name__ == "__main__":  # pragma: no cover
    main()
