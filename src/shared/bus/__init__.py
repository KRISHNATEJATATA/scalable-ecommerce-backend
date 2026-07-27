"""Transactional-outbox → SNS/SQS event bus.

The domain writes business state **and** an ``outbox`` row in one transaction; 
this package is everything that happens *after* that commit:

- :class:`~src.shared.bus.relay.OutboxRelay` (``run_relay``) — a ``service``-role
  poller that claims unpublished outbox rows with ``FOR UPDATE SKIP LOCKED``,
  publishes them to SNS (one topic per event type), then marks them published.
  Publish-then-mark, so a crash re-ships (at-least-once).
- :class:`~src.shared.bus.consumer.SqsConsumer` — reads one SQS subscription,
  dedupes on the envelope ``event_id`` in Valkey (best-effort; the handler's DB
  write is the real idempotency guard), and continues the W3C trace across the
  queue hop. Poison messages land in a per-subscription DLQ via SQS redrive.

The relay talks to the outbox with **raw SQL against schema-qualified tables**
(never a cross-module ORM import), keeping module-independence intact.
"""

from __future__ import annotations

from src.shared.bus.constants import OUTBOX_SCHEMAS, topic_name
from src.shared.bus.consumer import SqsConsumer
from src.shared.bus.publisher import SnsPublisher
from src.shared.bus.relay import OutboxRelay, run_relay
from src.shared.bus.tracecontext import TRACEPARENT_ATTR, format_traceparent, parse_trace_id

__all__ = [
    "OUTBOX_SCHEMAS",
    "topic_name",
    "SnsPublisher",
    "OutboxRelay",
    "run_relay",
    "SqsConsumer",
    "TRACEPARENT_ATTR",
    "format_traceparent",
    "parse_trace_id",
]
