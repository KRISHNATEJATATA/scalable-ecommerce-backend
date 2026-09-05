"""Cart product-event worker — the `service`-role consumer that keeps Valkey
cart snapshots honest.

Thin SQS transport shell over the generic idempotent :class:`SqsConsumer`: it
drains the ``cart-events`` queue (subscribed to ``ProductUpdated`` and
``ProductDeleted`` via SNS) and projects each event into every cart holding
that product:

* ``ProductDeleted`` (any schema version) prunes the line — a tombstone always
  wins, and a pruned line stays pruned.
* ``ProductUpdated`` refreshes the line's ``name``/``unit_price`` snapshot, but
  only for lines still present (a stale update never resurrects a deleted
  line) and only when
  :func:`~src.cart.domain.cart.should_apply_update` passes — SNS is unordered
  and the relay publishes concurrently, so an older update can arrive after a
  newer one. v1 events carry no ``product_version`` and apply only to lines
  with no versioned knowledge; producers emit v2.

``image_url`` is deliberately NOT refreshed here: the event payload is a
notification plus the fields a consumer projects (name, price, category — see
``ProductWriteDataV2``), not a replica of the row. The snapshot re-aligns on
the next add. Stale images in carts are tolerated; stale prices are not.

Idempotent twice over: ``SqsConsumer`` dedupes on ``event_id`` **within this
subscription** (``event:cart-events:{event_id}``), and both projections are
naturally idempotent (re-applying a refresh writes the same snapshot;
re-pruning is a no-op). A handler that raises leaves the message for SQS
redrive → DLQ (replay per ``docs/RUNBOOK.md``).

The handler is pure Valkey (no Postgres): refresh needs nothing the event
doesn't carry. Run: ``python -m src.cart.adapters.cart_consumer``.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from typing import Any

from src.cart.ports.repository import CartRepositoryPort
from src.shared.bus.client import sqs_client
from src.shared.bus.consumer import Handler, SqsConsumer
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


def make_cart_handler(repo: CartRepositoryPort) -> Handler:
    """Build the SqsConsumer handler projecting product events into carts."""

    async def _handle(event: dict[str, Any]) -> None:
        event_type = event.get("type")
        data = event.get("data", {})
        product_id = uuid.UUID(str(data["product_id"]))
        if event_type == "ProductDeleted":
            pruned = await repo.prune_product(product_id)
            log.debug("pruned product %s from %d cart(s)", product_id, pruned)
        elif event_type == "ProductUpdated":
            refreshed = await repo.refresh_product(
                product_id,
                name=str(data["name"]),
                unit_price=str(data["price"]),
                product_version=data.get("product_version"),
            )
            log.debug("refreshed product %s in %d cart(s)", product_id, refreshed)
        else:  # pragma: no cover - the subscription only carries product events
            log.warning("cart consumer ignoring unexpected event type %r", event_type)

    return _handle


async def run_worker(settings: AppSettings, valkey: Any, repo: CartRepositoryPort, stop: asyncio.Event) -> None:
    """Build a real SQS-backed cart consumer and run its loop."""
    if not settings.cart_queue_url:
        raise RuntimeError("CART_QUEUE_URL must be configured for the cart worker")
    async with sqs_client(settings) as sqs:
        consumer = SqsConsumer(
            sqs,
            valkey,
            settings.cart_queue_url,
            make_cart_handler(repo),
            consumer_name="cart-events",
            dedup_ttl_seconds=settings.consumer_dedup_ttl_seconds,
            lease_ttl_seconds=settings.consumer_lease_ttl_seconds,
            max_messages=settings.consumer_max_messages,
            wait_time_seconds=settings.consumer_wait_time_seconds,
        )
        await consumer.run(stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.cart.adapters.cart_consumer` — the `service`-role cart projector."""
    from src.cart.adapters.valkey.repository import ValkeyCartRepository
    from src.shared.clients import valkey_client
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import serve_worker_metrics

    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="cart-events-worker")
    valkey = valkey_client.create_client(settings)
    repo = ValkeyCartRepository(valkey, ttl_seconds=settings.cart_ttl_seconds)
    log.info("cart worker starting (queue=%s)", settings.cart_queue_url)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_worker(settings, valkey, repo, stop)
        finally:
            await valkey.aclose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
