"""Shared builders for inventory outbox rows.

One place to construct the :class:`OutboxMessage` the repository writes to the
transactional outbox, so the ``StockReserved``/``StockReleased`` envelope isn't
hand-rolled at each call site (reserve, saga compensation, reaper).

Both builders take ``(sku, order_id, quantity)`` positionally, which *is* the
:data:`~src.inventory.ports.repository.OutboxFactory` signature — so
``stock_released_outbox`` is handed to the repository directly rather than
through a forwarding wrapper.

``trace_id`` comes from the ambient request context; the reaper runs outside any
request, so an empty context falls back to a fresh id rather than an empty string
— an untraceable event is still traceable to *one* reaper pass.
"""

from __future__ import annotations

import uuid

from src.events.models import StockChangeData, StockReleased, StockReserved
from src.shared.config.logging import request_id_ctx
from src.shared.db.outbox import OutboxMessage


def _trace_id() -> str:
    return request_id_ctx.get() or str(uuid.uuid4())


def stock_reserved_outbox(sku: str, order_id: uuid.UUID, quantity: int) -> OutboxMessage:
    """Build the ``StockReserved`` outbox message."""
    event = StockReserved(trace_id=_trace_id(), data=StockChangeData(sku=sku, order_id=order_id, quantity=quantity))
    return OutboxMessage(event.type, event.model_dump_json())


def stock_released_outbox(sku: str, order_id: uuid.UUID, quantity: int) -> OutboxMessage:
    """Build the ``StockReleased`` outbox message."""
    event = StockReleased(trace_id=_trace_id(), data=StockChangeData(sku=sku, order_id=order_id, quantity=quantity))
    return OutboxMessage(event.type, event.model_dump_json())
