"""Transactional-outbox value types shared across modules.

``OutboxMessage`` names the two things every outbox row carries, so a serialized
event stops travelling between application → ports → adapters as an anonymous
``tuple[str, str]`` whose field order you have to remember. It subclasses
``NamedTuple`` deliberately: existing call sites that unpack it positionally keep
working unchanged.
"""

from __future__ import annotations

from typing import NamedTuple

#: SNS/SQS reject any message over 256 KB, and a relay claim publishes rows
#: serially per schema — one oversized row would therefore block every event
#: published after it in its own module, forever. The headroom covers the SQS
#: message envelope and the ``traceparent`` attribute the relay attaches.
MAX_OUTBOX_PAYLOAD_BYTES = 240 * 1024


class OutboxPayloadTooLargeError(ValueError):
    """An outbox payload would exceed what SNS/SQS can carry (see MAX_OUTBOX_PAYLOAD_BYTES)."""


class OutboxMessage(NamedTuple):
    """One event ready for the outbox: its type and its serialized JSON envelope."""

    event_type: str
    payload: str
