"""Payments domain — pure Python, imports nothing outward.

``PaymentStatus`` lives here (the innermost layer) as the single source of truth
for the ``payments.payments.status`` values, mirroring the pattern of
``orders.domain.OrderStatus``. Frozen slotted dataclasses mirror the read model.

The lifecycle is deliberately small — ``pending → succeeded | failed`` forward,
plus one reverse leg: ``succeeded → refunded`` (the money-moved-but-order-died
undo the checkout saga drives when a charge lands on a cancelled order). All
transitions are **terminal**: async confirmation (webhook or reconciliation)
applies at most one outcome, and a late/out-of-order notification for an
already-final payment is a no-op, never an overwrite. A refund is applied
exactly once for the same reason — the ``succeeded → refunded`` guard UPDATE is
idempotent, so a duplicated refund request changes nothing. That invariant is
enforced by the guarded UPDATE in the repository; the enum documents it here.
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
    #: Terminal reverse leg: the charge succeeded but the order died (cancelled
    #: by a concurrent cancel, or compensated for a stock shortfall), so the
    #: money was returned. Applied by the saga's refund compensation through
    #: the same guarded terminal transition — never an in-place edit.
    REFUNDED = "refunded"


@dataclass(frozen=True, slots=True)
class Payment:
    """One payment attempt against the gateway (immutable domain entity).

    ``gateway_ref`` is the provider's token/reference — never card data (PCI
    SAQ-A: only a token ever crosses our trust boundary). ``failure_reason`` is
    the provider-supplied decline reason, set only on a terminal non-success:
    a ``failed`` charge, or a refund that could not be confirmed provider-side
    (the row then stays ``succeeded`` with the failure stamped here).
    """

    id: uuid.UUID
    order_id: uuid.UUID
    status: PaymentStatus | str
    amount: Decimal
    gateway_ref: str | None
    created_at: datetime
    updated_at: datetime
    failure_reason: str | None = None
    idempotency_key: str = ""
