"""Reservation domain — pure Python, imports nothing outward.

A reservation is a *hold* on stock: ``inventory.reserved`` is already bumped, but
``on_hand`` is untouched until the order is paid (``committed``). Every hold
carries ``expires_at`` so a stalled saga cannot leak stock — the reaper releases
whatever is still ``held`` past its expiry.

The status set is deliberately terminal-once-left: ``held`` → ``released`` (saga
compensation or reaper) or ``held`` → ``committed`` (payment succeeded). Both
transitions are guarded on ``status = 'held'`` in SQL, which is what makes replay
idempotent.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ReservationStatus(StrEnum):
    """Lifecycle of a stock hold."""

    HELD = "held"
    RELEASED = "released"
    COMMITTED = "committed"


@dataclass(frozen=True, slots=True)
class Reservation:
    """A hold against one SKU for one order (immutable snapshot of the row)."""

    id: uuid.UUID
    sku: str
    qty: int
    order_id: uuid.UUID
    status: ReservationStatus
    expires_at: datetime
