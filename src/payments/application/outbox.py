"""Shared builders for payments outbox rows.

One place to construct the :class:`OutboxMessage` the repository writes to the
transactional outbox, so the ``PaymentSucceeded``/``PaymentFailed`` envelope
isn't hand-rolled at each call site (charge result, webhook, reconciliation).

The factories are fed the transition's own RETURNING values (payment_id,
order_id, amount, gateway_ref/failure_reason) — post-update state read inside the
write transaction, never a pre-read a concurrent webhook could make stale.

``trace_id`` comes from the ambient request context; the reconciler runs outside
any request, so an empty context falls back to a fresh id rather than an empty
string — an untraceable event is still traceable to *one* poller pass.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.events.models import PaymentFailed, PaymentFailedData, PaymentSucceeded, PaymentSucceededData
from src.shared.config.logging import current_trace_id
from src.shared.db.outbox import OutboxMessage


def payment_succeeded_outbox(row: Any) -> OutboxMessage:
    """Build the ``PaymentSucceeded`` message from the applied transition's row."""
    event = PaymentSucceeded.new(
        trace_id=current_trace_id(),
        data=PaymentSucceededData(
            payment_id=row["id"], order_id=row["order_id"], amount=_amount(row), gateway_ref=row["gateway_ref"]
        ),
    )
    return OutboxMessage(event.type, event.model_dump_json())


def payment_failed_outbox(row: Any) -> OutboxMessage:
    """Build the ``PaymentFailed`` message from the applied transition's row."""
    event = PaymentFailed.new(
        trace_id=current_trace_id(),
        data=PaymentFailedData(
            payment_id=row["id"],
            order_id=row["order_id"],
            amount=_amount(row),
            reason=row["failure_reason"] or "unknown",
        ),
    )
    return OutboxMessage(event.type, event.model_dump_json())


def _amount(row: Any) -> Decimal:
    amount = row["amount"]
    return amount if isinstance(amount, Decimal) else Decimal(str(amount))
