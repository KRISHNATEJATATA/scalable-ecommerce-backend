"""Transactional-outbox value types shared across modules.

``OutboxMessage`` names the two things every outbox row carries, so a serialized
event stops travelling between application → ports → adapters as an anonymous
``tuple[str, str]`` whose field order you have to remember. It subclasses
``NamedTuple`` deliberately: existing call sites that unpack it positionally keep
working unchanged.
"""

from __future__ import annotations

from typing import NamedTuple


class OutboxMessage(NamedTuple):
    """One event ready for the outbox: its type and its serialized JSON envelope."""

    event_type: str
    payload: str
