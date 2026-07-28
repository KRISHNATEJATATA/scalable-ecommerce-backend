"""Port (Protocol) for the catalog repository.

Structural contract implemented by ``adapters/db/repository.CatalogRepository``
and wired in ``src/shared/container.py`` (ticket 03). Reads land in ticket 02;
the write methods (``create``/``update``/``soft_delete``) land here with the
product-CRUD feature, each persisting an outbox row in the same txn.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# return type is the adapter's ORM/read-model row (Product / ProductRow), typed
# as Any because ports must not import adapters (ports <- adapters). Upgrade to a domain
# schema type here once catalog gets a real domain layer.
#
# ``outbox`` is an ``(event_type, payload)`` pair the adapter INSERTs into
# ``catalog.outbox`` in the same transaction as the state change (transactional
# outbox — never a dual-write to the bus from the request path).


class CatalogRepositoryPort(Protocol):
    async def list_products(self, params: PageParams, filters: dict[str, object] | None = None) -> Page[Any]: ...

    async def get_product(self, product_id: uuid.UUID) -> Any | None: ...

    async def create_product(
        self,
        *,
        product_id: uuid.UUID,
        merchant_id: uuid.UUID,
        name: str,
        description: str | None,
        category: str | None,
        price: Decimal,
        image_key: str | None,
        outbox: tuple[str, str],
    ) -> Any: ...

    async def update_product(self, product: Any, changes: dict[str, object], outbox: tuple[str, str]) -> Any: ...

    async def soft_delete_product(self, product: Any, outbox: tuple[str, str]) -> None: ...

    async def set_image_pending(self, product: Any, upload_token: str) -> None: ...

    async def mark_image_ready(self, product_id: uuid.UUID, upload_token: str, image_key: str) -> bool: ...

    async def mark_image_failed(self, product_id: uuid.UUID, upload_token: str) -> bool: ...
