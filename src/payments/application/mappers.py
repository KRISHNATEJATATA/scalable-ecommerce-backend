"""Payments mapper: ORM ``Payment`` row → domain ``Payment``.

Typed ``Any`` to avoid an application → adapters import (the row is duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.payments.domain.payment import Payment


def to_domain(row: Any) -> Payment:
    """Map an ORM ``Payment`` row to a domain ``Payment``."""
    return Payment(
        id=row.id,
        order_id=row.order_id,
        status=row.status,
        amount=row.amount,
        gateway_ref=row.gateway_ref,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
