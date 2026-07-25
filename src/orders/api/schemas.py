"""Orders wire schemas — the public HTTP response shape for an order + its lines.

``from_attributes`` lets the service build these straight off the domain
``Order`` / ``OrderItem`` dataclasses.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from src.orders.domain.order import OrderStatus


class OrderItemResponse(BaseModel):
    """The public HTTP response shape for one order line."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    unit_price: Decimal
    quantity: int


class OrderResponse(BaseModel):
    """The public HTTP response shape for an order and its lines."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    total: Decimal
    items: list[OrderItemResponse]
    created_at: datetime
    updated_at: datetime
