"""Catalog use-cases (read + write).

Maps repo rows through the domain to Pydantic response schemas so ORM/``ProductRow``
never crosses the service boundary. Missing single row -> ``None`` (the route maps
``None`` -> 404; services never raise HTTP).

Writes own two invariants:
- **Ownership**: a merchant may only mutate their own product;
  a cross-merchant mutation raises ``AuthorizationError`` (->403). ``admin`` bypasses
  ownership (never the role gate - that stays in the route).
- **Product events**: every create/update/delete builds the matching versioned
  event and hands it to the repo, which persists it to the transactional outbox in
  the same transaction as the state change (the relay ships it to the bus).
"""

from __future__ import annotations

import uuid

from src.catalog.api.schemas import ProductCreate, ProductResponse, ProductUpdate
from src.catalog.application.mappers import to_domain
from src.catalog.ports.repository import CatalogRepositoryPort
from src.events.models import ProductCreated, ProductDeleted, ProductDeletedData, ProductUpdated, ProductWriteData
from src.shared.config.logging import request_id_ctx
from src.shared.db.pagination import PageParams, PageResponse
from src.shared.errors.exceptions import AuthorizationError


class CatalogService:
    """Read- and write-side use-cases over the product catalog."""

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

    async def create_product(self, *, merchant_id: uuid.UUID, data: ProductCreate) -> ProductResponse:
        """Create a product owned by ``merchant_id`` and emit ``ProductCreated``."""
        product_id = uuid.uuid4()
        event = ProductCreated(
            trace_id=request_id_ctx.get(),
            data=ProductWriteData(
                product_id=product_id,
                merchant_id=merchant_id,
                name=data.name,
                price=data.price,
                category=data.category,
            ),
        )
        row = await self._repo.create_product(
            product_id=product_id,
            merchant_id=merchant_id,
            name=data.name,
            description=data.description,
            category=data.category,
            price=data.price,
            image_key=data.image_key,
            outbox=(event.type, event.model_dump_json()),
        )
        return ProductResponse.model_validate(to_domain(row))

    async def update_product(
        self, *, product_id: uuid.UUID, merchant_id: uuid.UUID, is_admin: bool, patch: ProductUpdate
    ) -> ProductResponse | None:
        """Update an owned product and emit ``ProductUpdated``; ``None`` if absent."""
        product = await self._repo.get_product(product_id)
        if product is None:
            return None
        self._assert_owner(product.merchant_id, merchant_id, is_admin)

        changes = patch.model_dump(exclude_unset=True)
        event = ProductUpdated(
            trace_id=request_id_ctx.get(),
            data=ProductWriteData(
                product_id=product_id,
                merchant_id=product.merchant_id,
                name=changes.get("name", product.name),
                price=changes.get("price", product.price),
                category=changes.get("category", product.category),
            ),
        )
        row = await self._repo.update_product(product, changes, outbox=(event.type, event.model_dump_json()))
        return ProductResponse.model_validate(to_domain(row))

    async def delete_product(self, *, product_id: uuid.UUID, merchant_id: uuid.UUID, is_admin: bool) -> bool:
        """Soft-delete an owned product and emit ``ProductDeleted``; ``False`` if absent."""
        product = await self._repo.get_product(product_id)
        if product is None:
            return False
        self._assert_owner(product.merchant_id, merchant_id, is_admin)

        event = ProductDeleted(
            trace_id=request_id_ctx.get(),
            data=ProductDeletedData(product_id=product_id, merchant_id=product.merchant_id),
        )
        await self._repo.soft_delete_product(product, outbox=(event.type, event.model_dump_json()))
        return True

    @staticmethod
    def _assert_owner(owner_id: uuid.UUID, caller_id: uuid.UUID, is_admin: bool) -> None:
        """Merchant may only mutate their own products; ``admin`` bypasses ownership."""
        if owner_id != caller_id and not is_admin:
            raise AuthorizationError("not the owner of this product")
