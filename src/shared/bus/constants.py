"""Static bus topology constants.

The set of event-publishing schemas is structural (it mirrors the modules that
own an ``outbox`` table), so it lives in code, not config.
Cart is Valkey-only and has no outbox.
"""

from __future__ import annotations

# Schemas the relay scans, in a fixed allow-list. The relay f-strings these into
# schema-qualified SQL, so the list MUST stay a trusted constant (never user input).
OUTBOX_SCHEMAS: tuple[str, ...] = ("identity", "catalog", "inventory", "orders", "payments")


def topic_name(prefix: str, event_type: str) -> str:
    """SNS topic name for an event type: ``f"{prefix}{EventType}"`` (topic-per-type)."""
    return f"{prefix}{event_type}"
