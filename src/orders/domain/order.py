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
    """The order aggregate mirroring the ``orders`` read model (immutable)."""

    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    total: Decimal
    items: tuple[OrderItem, ...]
    created_at: datetime
    updated_at: datetime
