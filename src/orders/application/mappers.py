"""Orders mappers: ORM ``Order`` (with eager-loaded items) → domain ``Order``.

``items`` becomes a tuple so the domain aggregate is immutable. Typed ``Any`` to
avoid an application → adapters import (the row is duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.orders.domain.order import Order, OrderItem, OrderStatus


def item_to_domain(row: Any) -> OrderItem:
    """Map an ORM order-line row to a domain ``OrderItem``."""
    return OrderItem(
        id=row.id,
        product_id=row.product_id,
        product_name=row.product_name,
        unit_price=row.unit_price,
        quantity=row.quantity,
    )


def to_domain(row: Any) -> Order:
    """Map an ORM ``Order`` (with eager-loaded items) to a domain ``Order``."""
    return Order(
        id=row.id,
        user_id=row.user_id,
        status=OrderStatus(row.status),
        total=row.total,
        items=tuple(item_to_domain(item) for item in row.items),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
