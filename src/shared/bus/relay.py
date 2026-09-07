"""Outbox relay — the `service`-role poller that ships events to SNS.

Per pass, for each publishing schema, the relay claims a batch of unpublished
rows with ``FOR UPDATE SKIP LOCKED`` (so N relay replicas never double-claim),
publishes each to SNS, then stamps ``published_at`` — all in one transaction.
It also stamps the ``outbox_lag_seconds`` gauge at claim time (the claim query
already returns the rows oldest-first); the alertable source remains the
API-side DB poll in ``src.shared.bus.metrics``, which keeps measuring when the
relay itself is dead.

**Publish-then-mark, never the reverse.** If the process dies mid-batch (or a
publish raises), the transaction rolls back, the rows stay unpublished, and the
next pass re-ships them. That is the outbox's whole point: at-least-once
delivery with no dual-write in the request path. Duplicates are expected and
absorbed downstream by the idempotent consumer.

The partial index ``ix_<schema>_outbox_unpublished (published_at) WHERE
published_at IS NULL`` keeps the claim query cheap.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.shared.bus.client import sns_client
from src.shared.bus.constants import OUTBOX_SCHEMAS
from src.shared.bus.metrics import outbox_lag_seconds
from src.shared.bus.polling import poll_forever
from src.shared.bus.publisher import SnsPublisher
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class OutboxRelay:
    """Drains outbox tables into SNS. ``publisher`` may be any object with an
    ``async publish(event_type, payload)`` method (the real :class:`SnsPublisher`
    in production; a fake in tests)."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        publisher,
        *,
        batch_size: int,
        schemas=OUTBOX_SCHEMAS,
        concurrency: int = 10,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._publisher = publisher
        self._batch = batch_size
        self._schemas = tuple(schemas)
        self._sem = asyncio.Semaphore(concurrency)

    async def _publish(self, event_type: str, payload) -> None:
        async with self._sem:
            await self._publisher.publish(event_type, payload)

    async def _drain_schema(self, session, schema: str) -> int:
        # `schema` is a trusted constant from OUTBOX_SCHEMAS, never user input,
        # so f-string interpolation into the identifier position is safe.
        async with session.begin():
            rows = (
                await session.execute(
                    text(
                        f"SELECT id, event_type, payload, occurred_at FROM {schema}.outbox "
                        "WHERE published_at IS NULL ORDER BY occurred_at "
                        "FOR UPDATE SKIP LOCKED LIMIT :batch"
                    ),
                    {"batch": self._batch},
                )
            ).all()
            if not rows:
                outbox_lag_seconds.labels(schema).set(0.0)
                return 0
            # The claim is the lag observation: rows are ordered by occurred_at,
            # so the first one *was* the oldest unpublished row of this schema
            # (a newer insert landing mid-claim is on the next pass). The
            # API-side poll (shared.bus.metrics) re-measures from the DB on its
            # own cadence and owns the alert signal when the relay is dead.
            outbox_lag_seconds.labels(schema).set(max((datetime.now(UTC) - rows[0].occurred_at).total_seconds(), 0.0))
            # Publish concurrently (bounded): serial awaits held the row locks and a
            # pooled connection for batch_size × RTT (~3s at 100 × 30ms), capping a
            # replica near 30 events/s. A TaskGroup cancels siblings on the first
            # failure and the txn rolls back, so nothing is marked published that
            # wasn't. Safe because the topics are standard (non-FIFO) SNS — no
            # ordering guarantee to preserve — and consumers are
            # idempotent, so a publish that landed before the rollback just
            # redelivers. Not PublishBatch: its per-entry ``Failed`` list would have
            # to be reconciled row-by-row or we'd stamp unpublished rows published.
            async with asyncio.TaskGroup() as tg:
                for row in rows:
                    tg.create_task(self._publish(row.event_type, row.payload))
            await session.execute(
                text(f"UPDATE {schema}.outbox SET published_at = now() WHERE id = ANY(:ids)"),
                {"ids": [row.id for row in rows]},
            )
            return len(rows)

    async def drain_once(self) -> int:
        """One pass over every schema; returns the number of rows published.

        Schemas are **isolated from each other**. A publish that fails for good —
        an event type whose topic was never provisioned, a payload over the SNS
        size limit — would otherwise abort the whole pass at the schema it sits
        in, so every schema *after* it in the list would never drain again: one
        stuck row in ``catalog`` silently stops ``orders`` and ``payments`` from
        shipping. Each schema therefore fails on its own; the rest still drain.
        (Within a schema a stuck row does block the rows behind it — that is the
        outbox's ordering guarantee, and the row is visible via the outbox-lag
        metric.)

        A failed pass that shipped **nothing** propagates, so :func:`poll_forever`
        backs off exponentially instead of hammering a dead dependency once per
        poll interval. Forward progress anywhere (any row published) swallows the
        error instead: the bus is demonstrably up, so the failure is local to one
        schema and backing off would only delay the schemas that are working.
        Counting failed *schemas* instead of published rows missed the common
        case — SNS down while every schema but one happens to be idle, where an
        empty schema "succeeds" trivially and hid the outage from the backoff.
        """
        published = 0
        failures: list[Exception] = []
        async with self._sessionmaker() as session:
            for schema in self._schemas:
                try:
                    published += await self._drain_schema(session, schema)
                except Exception as exc:  # boundary: one schema must not starve the others
                    log.exception("outbox drain failed for schema %s; continuing with the rest", schema)
                    failures.append(exc)
        if failures and published == 0:
            raise failures[0]
        return published

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle."""
        await poll_forever(self.drain_once, stop or asyncio.Event(), log, idle_interval=poll_interval)


async def run_relay(
    settings: AppSettings,
    sessionmaker: async_sessionmaker,
    *,
    schemas=OUTBOX_SCHEMAS,
    stop: asyncio.Event | None = None,
) -> None:
    """Build a real SNS-backed relay from settings and run its loop.

    On real AWS (``bus_endpoint_url is None``) the topic ARN namespace is
    **required**: Terraform owns the topics there and the task role is publish-only,
    so falling back to ``create_topic`` would fail with ``AccessDenied`` on the
    first event of every cold start. Refuse to start with a clear message instead
    of discovering it one dropped batch at a time.
    """
    if settings.bus_endpoint_url is None and not settings.bus_topic_arn_prefix:
        raise RuntimeError(
            "BUS_TOPIC_ARN_PREFIX must be set when BUS_ENDPOINT_URL is unset (real AWS): "
            "topics are provisioned by Terraform and the relay task role has sns:Publish only"
        )
    async with sns_client(settings) as sns:
        publisher = SnsPublisher(sns, settings.bus_topic_prefix, settings.bus_topic_arn_prefix)
        relay = OutboxRelay(
            sessionmaker,
            publisher,
            batch_size=settings.relay_batch_size,
            schemas=schemas,
            concurrency=settings.relay_publish_concurrency,
        )
        await relay.run(settings.relay_poll_interval_seconds, stop=stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.shared.bus.relay` — the `service`-role relay worker."""
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import serve_worker_metrics

    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="outbox-relay")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    log.info("outbox relay starting (schemas=%s)", ",".join(OUTBOX_SCHEMAS))

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_relay(settings, sessionmaker, stop=stop)
        finally:
            await engine.dispose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
