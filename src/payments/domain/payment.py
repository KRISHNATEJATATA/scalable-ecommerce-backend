"""Payments domain entity — pure Python, imports nothing outward.

Frozen slotted dataclass mirroring the ``payments.payments`` row. ``status`` is a
plain ``str`` (the ORM column is a free-form ``String``, not an enum), so there's
no status enum to re-home this phase.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Payment:
    """The payments read model (immutable domain entity)."""

    id: uuid.UUID
    order_id: uuid.UUID
    status: str
    amount: Decimal
    gateway_ref: str | None
    created_at: datetime
    updated_at: datetime
