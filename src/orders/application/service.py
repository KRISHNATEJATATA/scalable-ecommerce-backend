"""Orders read use-cases.

Maps ORM orders through the domain to Pydantic response schemas (ORM never
crosses the boundary). ``list_orders`` is always scoped to ``user_id`` by the
repo. Missing single row → ``None`` (route maps to 404 later).
"""

from __future__ import annotations

import uuid

from src.orders.api.schemas import OrderResponse
from src.orders.application.mappers import to_domain
from src.orders.domain.order import OrderStatus
from src.orders.ports.repository import OrdersRepositoryPort
from src.shared.db.pagination import PageParams, PageResponse


class OrdersService:
    """Read-side use-cases over the order aggregate."""

    def __init__(self, repo: OrdersRepositoryPort) -> None:
        self._repo = repo

    async def get_order(self, order_id: uuid.UUID) -> OrderResponse | None:
        """Resolve an order (with its lines) by id, or ``None`` if absent."""
        row = await self._repo.get_order(order_id)
        if row is None:
            return None
        return OrderResponse.model_validate(to_domain(row))

    async def list_orders(
        self, user_id: uuid.UUID, params: PageParams, status: OrderStatus | None = None
    ) -> PageResponse[OrderResponse]:
        """Return a keyset page of a user's orders, optionally filtered by status."""
        page = await self._repo.list_orders(user_id, params, status)
        items = [OrderResponse.model_validate(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)
