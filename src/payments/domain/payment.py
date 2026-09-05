"""Payments domain — pure Python, imports nothing outward.

``PaymentStatus`` lives here (the innermost layer) as the single source of truth
for the ``payments.payments.status`` values, mirroring the pattern of
``orders.domain.OrderStatus``. Frozen slotted dataclasses mirror the read model.

The lifecycle is deliberately tiny — ``pending → succeeded | failed`` — and the
transitions are **terminal**: async confirmation (webhook or reconciliation)
applies at most one outcome, and a late/out-of-order notification for an already-
final payment is a no-op, never an overwrite. That invariant is enforced by the
guarded UPDATE in the repository; the enum documents it here.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


class PaymentStatus(enum.StrEnum):
    """Lifecycle of one payment attempt; single source for the DB values."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Payment:
    """One payment attempt against the gateway (immutable domain entity).

    ``gateway_ref`` is the provider's token/reference — never card data (PCI
    SAQ-A: only a token ever crosses our trust boundary). ``failure_reason`` is
    the provider-supplied decline reason, set only on a ``failed`` transition.
    """

    id: uuid.UUID
    order_id: uuid.UUID
    status: PaymentStatus | str
    amount: Decimal
    gateway_ref: str | None
    created_at: datetime
    updated_at: datetime
    failure_reason: str | None = None
