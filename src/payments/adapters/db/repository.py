"""Payments repository — plain ORM lookup by order.

Returns a keyset page of attempts for an order, oldest first (retries share
``order_id``; ``idempotency_key`` is the unique guard) — reconciliation
decides which is authoritative.

returns ORM ``Payment`` rows, not domain entities. Upgrade to a
domain schema once payments gets a real domain layer.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.payments.adapters.db.models import Payment
from src.shared.db.pagination import Page, PageParams, apply_keyset, build_page, decode_cursor
from src.shared.errors.exceptions import InvalidQueryParamError

_SORT_COLUMNS = {"created_at": Payment.created_at}


class PaymentsRepository:
    """Implements :class:`src.payments.ports.repository.PaymentsRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> Page[Payment]:
        if params.sort_field not in _SORT_COLUMNS:
            raise InvalidQueryParamError("sort", params.sort_field)
        sort_col = _SORT_COLUMNS[params.sort_field]

        stmt = select(Payment).where(Payment.order_id == order_id)
        cursor = decode_cursor(params.cursor) if params.cursor else None
        stmt = apply_keyset(stmt, sort_col, Payment.id, params, cursor)

        rows = list((await self._session.execute(stmt)).scalars().all())
        return build_page(rows, params, key_of=lambda payment: (getattr(payment, params.sort_field), payment.id))
