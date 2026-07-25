"""Payments read use-case.

Maps ORM rows through the domain to a ``PageResponse`` of ``PaymentResponse``
Pydantic schemas so the ORM row never crosses the service boundary.
"""

from __future__ import annotations

import uuid

from src.payments.api.schemas import PaymentResponse
from src.payments.application.mappers import to_domain
from src.payments.ports.repository import PaymentsRepositoryPort
from src.shared.db.pagination import PageParams, PageResponse


class PaymentsService:
    """Read-side use-cases over the payments model."""

    def __init__(self, repo: PaymentsRepositoryPort) -> None:
        self._repo = repo

    async def get_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> PageResponse[PaymentResponse]:
        """Return a keyset page of payments for an order (newest first by default)."""
        page = await self._repo.get_by_order_id(order_id, params)
        items = [PaymentResponse.model_validate(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)
