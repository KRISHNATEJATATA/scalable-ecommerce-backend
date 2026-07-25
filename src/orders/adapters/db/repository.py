"""Orders read repository — ORM ``select()`` with ``selectinload`` for items.

``list_orders`` is always scoped to the owning ``user_id`` (repo-applied, not a
client-supplied filter) and embeds line items via ``selectinload(Order.items)``
so a page of N orders costs 2 queries, not N+1 (``selectinload`` over
``joinedload`` because items is a collection).

returns ORM ``Order`` rows (with items eager-loaded); services
(ticket 03) map them to Pydantic schemas.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.orders.adapters.db.models import Order, OrderStatus
from src.shared.db.pagination import Page, PageParams, apply_keyset, build_page, decode_cursor
from src.shared.errors.exceptions import InvalidQueryParamError

_SORT_COLUMNS = {"created_at": Order.created_at}


class OrdersRepository:
    """Implements :class:`src.orders.ports.repository.OrdersRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_orders(
        self, user_id: uuid.UUID, params: PageParams, status: OrderStatus | None = None
    ) -> Page[Order]:
        if params.sort_field not in _SORT_COLUMNS:
            raise InvalidQueryParamError("sort", params.sort_field)
        sort_col = _SORT_COLUMNS[params.sort_field]

        stmt = select(Order).where(Order.user_id == user_id).options(selectinload(Order.items))
        if status is not None:
            stmt = stmt.where(Order.status == status)

        cursor = decode_cursor(params.cursor) if params.cursor else None
        stmt = apply_keyset(stmt, sort_col, Order.id, params, cursor)

        rows = list((await self._session.execute(stmt)).scalars().all())
        return build_page(rows, params, key_of=lambda order: (getattr(order, params.sort_field), order.id))

    async def get_order(self, order_id: uuid.UUID) -> Order | None:
        stmt = select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        return (await self._session.execute(stmt)).scalar_one_or_none()
