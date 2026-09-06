"""Shared builder for orders outbox rows.

One place to construct the :class:`OutboxMessage` the repository writes to the
transactional outbox, so the ``OrderPlaced`` envelope isn't hand-rolled at each
call site. Emitted only on the ``pending → paid`` transition, from the
transition's own RETURNING values inside the same transaction — "the order is
paid" and "the bus will announce it" are one atomic fact.

``trace_id`` comes from the ambient request context; the recovery poller runs
outside any request, so an empty context falls back to a fresh id rather than
an empty string.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from src.events.models import OrderPlaced, OrderPlacedData, OrderPlacedLine
from src.shared.config.logging import current_trace_id
from src.shared.db.outbox import OutboxMessage


def order_placed_outbox(*, order_id: uuid.UUID, user_id: uuid.UUID, total: Decimal, items: list[Any]) -> OutboxMessage:
    """Build the ``OrderPlaced`` message from the paid transition's row + lines."""
    event = OrderPlaced.new(
        trace_id=current_trace_id(),
        data=OrderPlacedData(
            order_id=order_id,
            user_id=user_id,
            total=total if isinstance(total, Decimal) else Decimal(str(total)),
            items=[
                OrderPlacedLine(
                    product_id=item.product_id,
                    quantity=item.quantity,
                    unit_price=item.unit_price
                    if isinstance(item.unit_price, Decimal)
                    else Decimal(str(item.unit_price)),
                )
                for item in items
            ],
        ),
    )
    return OutboxMessage(event.type, event.model_dump_json())
