"""Payments ORM → domain mapper.

The repository maps every row it returns, so the application layer only ever sees
frozen snapshots, never a live ORM row.
"""

from __future__ import annotations

from src.payments.adapters.db.models import Payment as PaymentRow
from src.payments.domain.payment import Payment


def to_domain(row: PaymentRow) -> Payment:
    """Map an ORM ``Payment`` row to a domain ``Payment``."""
    return Payment(
        id=row.id,
        order_id=row.order_id,
        status=row.status,
        amount=row.amount,
        gateway_ref=row.gateway_ref,
        created_at=row.created_at,
        updated_at=row.updated_at,
        failure_reason=row.failure_reason,
        idempotency_key=row.idempotency_key,
    )
