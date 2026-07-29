"""Catalog cache-invalidation worker — the `service`-role consumer that keeps the
Valkey product cache honest.

Thin SQS transport shell over the generic idempotent :class:`SqsConsumer`: it
drains the ``catalog-cache`` queue (subscribed to ``ProductUpdated`` and
``ProductDeleted`` via SNS) and, for each event, evicts ``product:{id}`` from the
Valkey read-cache. That is the invalidation half of the cache-aside pair whose
read half lives in ``catalog/application/service`` — a write emits the event
through the transactional outbox, the relay ships it, and this consumer drops the
now-stale cache entry.

Idempotent by construction: a ``DELETE`` of an already-absent key is a no-op, and
``SqsConsumer`` additionally dedupes on ``event_id``. A handler that raises leaves
the message for SQS redrive → DLQ (replay per ``docs/RUNBOOK.md``).
Run: ``python -m src.catalog.adapters.cache_worker``.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from typing import Any

from src.catalog.adapters.cache import ValkeyProductCache
from src.catalog.ports.cache import ProductCachePort
from src.shared.bus.client import sqs_client
from src.shared.bus.consumer import Handler, SqsConsumer
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


def make_invalidation_handler(cache: ProductCachePort) -> Handler:
    """Build the SqsConsumer handler that evicts a product from the read-cache.

    Both ``ProductUpdated`` and ``ProductDeleted`` carry ``data.product_id``; the
    handler bumps the invalidation generation and drops that key (idempotent — a
    delete of an absent key is a no-op). Kept as a closure over the cache port so
    the same logic is trivially unit-testable with an in-memory fake.
    """

    async def _handle(event: dict[str, Any]) -> None:
        product_id = uuid.UUID(str(event["data"]["product_id"]))
        await cache.invalidate(product_id)
        log.debug("evicted product %s from read-cache (%s)", product_id, event.get("type"))

    return _handle


async def run_worker(settings: AppSettings, valkey: Any, stop: asyncio.Event) -> None:
    """Build a real SQS-backed cache-invalidation consumer and run its loop."""
    if not settings.catalog_cache_queue_url:
        raise RuntimeError("CATALOG_CACHE_QUEUE_URL must be configured for the catalog cache worker")
    cache = ValkeyProductCache(
        valkey,
        ttl_seconds=settings.product_cache_ttl_seconds,
        ttl_jitter_seconds=settings.product_cache_ttl_jitter_seconds,
        lock_ttl_seconds=settings.product_cache_lock_ttl_seconds,
    )
    async with sqs_client(settings) as sqs:
        consumer = SqsConsumer(
            sqs,
            valkey,
            settings.catalog_cache_queue_url,
            make_invalidation_handler(cache),
            dedup_ttl_seconds=settings.consumer_dedup_ttl_seconds,
            lease_ttl_seconds=settings.consumer_lease_ttl_seconds,
            max_messages=settings.consumer_max_messages,
            wait_time_seconds=settings.consumer_wait_time_seconds,
        )
        await consumer.run(stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.catalog.adapters.cache_worker` — the `service`-role cache invalidator."""
    from src.shared.clients import valkey_client
    from src.shared.config.logging import setup_logging

    settings = get_settings()
    setup_logging(settings.log_level)
    valkey = valkey_client.create_client(settings)
    log.info("catalog cache worker starting (queue=%s)", settings.catalog_cache_queue_url)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_worker(settings, valkey, stop)
        finally:
            await valkey.aclose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
