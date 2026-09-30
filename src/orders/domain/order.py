"""Orders domain — pure Python, imports nothing outward.

``OrderStatus`` lives here (the innermost layer) as the single source of truth;
``adapters/db/models.py`` imports it inward for its ``Enum`` column. Frozen
slotted dataclasses mirror the ``orders`` read model; ``Order.items`` is a tuple
so the aggregate is immutable end to end.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


class OrderStatus(enum.StrEnum):
    """Lifecycle states for an order; the single source of truth for the DB enum."""

    PENDING = "pending"
    PAID = "paid"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class OrderItem:
    """One immutable order line (a product snapshot at purchase time)."""

    id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    unit_price: Decimal
    quantity: int


@dataclass(frozen=True, slots=True)
class Order:
    """The order aggregate mirroring the ``orders`` read model (immutable).

    A point-in-time snapshot: the repository hands these out instead of live ORM
    rows, so nothing above the adapter can touch session state (identity map,
    expiry, lazy loads). ``idempotency_key``/``idempotency_body_hash`` and
    ``user_email`` are the checkout saga's replay + notification inputs; "" means
    unknown (legacy rows / fixtures).
    """

    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    total: Decimal
    items: tuple[OrderItem, ...]
    created_at: datetime
    updated_at: datetime
    idempotency_key: str = ""
    idempotency_body_hash: str = ""
    user_email: str = ""


@dataclass(frozen=True, slots=True)
class SagaStep:
    """One journaled checkout-saga step attempt (immutable snapshot of a ``saga_log`` row)."""

    step: str
    status: str
    created_at: datetime
