"""Transactional-outbox value types shared across modules.

``OutboxMessage`` names the two things every outbox row carries, so a serialized
event stops travelling between application → ports → adapters as an anonymous
``tuple[str, str]`` whose field order you have to remember. It subclasses
``NamedTuple`` deliberately: existing call sites that unpack it positionally keep
working unchanged.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import NamedTuple

#: SNS/SQS reject any message over 256 KB, and a relay claim publishes rows
#: serially per schema — one oversized row would therefore block every event
#: published after it in its own module, forever. The headroom covers the SQS
#: message envelope and the ``traceparent`` attribute the relay attaches.
MAX_OUTBOX_PAYLOAD_BYTES = 240 * 1024

_SCHEMA_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def checked_outbox_schemas(schemas: Iterable[str]) -> tuple[str, ...]:
    """Return ``schemas`` as a tuple, refusing any name that is not a plain SQL identifier.

    The relay and the lag poller interpolate schema names into identifier
    positions. The kernel does not know which modules publish events (the
    composition root owns that list), so it guards the *shape* of each name
    instead of holding a module allow-list.
    """
    checked = tuple(schemas)
    bad = [schema for schema in checked if not _SCHEMA_IDENTIFIER.fullmatch(schema)]
    if bad:
        raise ValueError(f"outbox schema names must be lowercase SQL identifiers, got {bad}")
    return checked


class OutboxPayloadTooLargeError(ValueError):
    """An outbox payload would exceed what SNS/SQS can carry (see MAX_OUTBOX_PAYLOAD_BYTES)."""


class OutboxMessage(NamedTuple):
    """One event ready for the outbox: its type and its serialized JSON envelope."""

    event_type: str
    payload: str
