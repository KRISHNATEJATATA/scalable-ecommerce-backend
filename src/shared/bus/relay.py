"""Outbox relay — the `service`-role poller that ships events to SNS.

Per pass, for each publishing schema, the relay claims a batch of unpublished
rows with ``FOR UPDATE SKIP LOCKED`` (so N relay replicas never double-claim),
publishes each to SNS, then stamps ``published_at`` — all in one transaction.

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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.shared.bus.client import sns_client
from src.shared.bus.constants import OUTBOX_SCHEMAS
from src.shared.bus.publisher import SnsPublisher
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class OutboxRelay:
    """Drains outbox tables into SNS. ``publisher`` may be any object with an
    ``async publish(event_type, payload)`` method (the real :class:`SnsPublisher`
    in production; a fake in tests)."""

    def __init__(self, sessionmaker: async_sessionmaker, publisher, *, batch_size: int, schemas=OUTBOX_SCHEMAS) -> None:
        self._sessionmaker = sessionmaker
        self._publisher = publisher
        self._batch = batch_size
        self._schemas = tuple(schemas)

    async def _drain_schema(self, session, schema: str) -> int:
        # `schema` is a trusted constant from OUTBOX_SCHEMAS, never user input,
        # so f-string interpolation into the identifier position is safe.
        async with session.begin():
            rows = (
                await session.execute(
                    text(
                        f"SELECT id, event_type, payload FROM {schema}.outbox "
                        "WHERE published_at IS NULL ORDER BY occurred_at "
                        "FOR UPDATE SKIP LOCKED LIMIT :batch"
                    ),
                    {"batch": self._batch},
                )
            ).all()
            if not rows:
                return 0
            for row in rows:
                await self._publisher.publish(row.event_type, row.payload)
            await session.execute(
                text(f"UPDATE {schema}.outbox SET published_at = now() WHERE id = ANY(:ids)"),
                {"ids": [row.id for row in rows]},
            )
            return len(rows)

    async def drain_once(self) -> int:
        """One pass over every schema; returns the number of rows published."""
        published = 0
        async with self._sessionmaker() as session:
            for schema in self._schemas:
                published += await self._drain_schema(session, schema)
        return published

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle."""
        while stop is None or not stop.is_set():
            try:
                published = await self.drain_once()
            except Exception:  # boundary: never let one bad pass kill the relay
                log.exception("relay pass failed; retrying after backoff")
                published = 0
            if published == 0:
                await asyncio.sleep(poll_interval)


async def run_relay(
    settings: AppSettings,
    sessionmaker: async_sessionmaker,
    *,
    schemas=OUTBOX_SCHEMAS,
    stop: asyncio.Event | None = None,
) -> None:
    """Build a real SNS-backed relay from settings and run its loop."""
    async with sns_client(settings) as sns:
        publisher = SnsPublisher(sns, settings.bus_topic_prefix)
        relay = OutboxRelay(sessionmaker, publisher, batch_size=settings.relay_batch_size, schemas=schemas)
        await relay.run(settings.relay_poll_interval_seconds, stop=stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.shared.bus.relay` — the `service`-role relay worker."""
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import serve_worker_metrics

    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="outbox-relay")
    engine = create_engine(settings)
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
