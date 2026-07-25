"""Catalog read use-cases.

Maps repo rows through the domain to Pydantic response schemas so ORM/``ProductRow``
never crosses the service boundary. Missing single row → ``None`` (the route maps
``None`` → 404 in its feature ticket; services never raise HTTP).
"""

from __future__ import annotations

import uuid

from src.catalog.api.schemas import ProductResponse
from src.catalog.application.mappers import to_domain
from src.catalog.ports.repository import CatalogRepositoryPort
from src.shared.db.pagination import PageParams, PageResponse


class CatalogService:
    """Read-side use-cases over the product catalog."""

    def __init__(self, repo: CatalogRepositoryPort) -> None:
        self._repo = repo

    async def get_product(self, product_id: uuid.UUID) -> ProductResponse | None:
        """Resolve a product by id, or ``None`` if absent (route maps to 404)."""
        row = await self._repo.get_product(product_id)
        if row is None:
            return None
        return ProductResponse.model_validate(to_domain(row))

    async def list_products(
        self, params: PageParams, filters: dict[str, object] | None = None
    ) -> PageResponse[ProductResponse]:
        """Return a keyset page of products, optionally filtered."""
        page = await self._repo.list_products(params, filters)
        items = [ProductResponse.model_validate(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)
