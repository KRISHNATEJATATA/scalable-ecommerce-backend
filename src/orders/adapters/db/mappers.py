"""Orders ORM → domain mappers.

The repository maps every row it returns, so the application layer only ever sees
frozen snapshots. ``to_domain`` needs the order's ``items`` eager-loaded (the
relationship is ``lazy="raise"``); ``items`` becomes a tuple so the aggregate is
immutable end to end.
"""

from __future__ import annotations

from src.orders.adapters.db.models import Order as OrderRow
from src.orders.adapters.db.models import OrderItem as OrderItemRow
from src.orders.adapters.db.models import SagaLog
from src.orders.domain.order import Order, OrderItem, OrderStatus, SagaStep


def item_to_domain(row: OrderItemRow) -> OrderItem:
    """Map an ORM order-line row to a domain ``OrderItem``."""
    return OrderItem(
        id=row.id,
        product_id=row.product_id,
        product_name=row.product_name,
        unit_price=row.unit_price,
        quantity=row.quantity,
    )


def to_domain(row: OrderRow) -> Order:
    """Map an ORM ``Order`` (with eager-loaded items) to a domain ``Order``."""
    return Order(
        id=row.id,
        user_id=row.user_id,
        status=OrderStatus(row.status),
        total=row.total,
        items=tuple(item_to_domain(item) for item in row.items),
        created_at=row.created_at,
        updated_at=row.updated_at,
        idempotency_key=row.idempotency_key,
        idempotency_body_hash=row.idempotency_body_hash,
        user_email=row.user_email,
    )


def saga_step_to_domain(row: SagaLog) -> SagaStep:
    """Map an ORM ``saga_log`` row to a domain ``SagaStep``."""
    return SagaStep(step=row.step, status=row.status, created_at=row.created_at)
