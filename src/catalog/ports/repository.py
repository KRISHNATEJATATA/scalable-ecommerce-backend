"""Port (Protocol) for the catalog repository.

Structural contract implemented by ``adapters/db/repository.CatalogRepository``
and wired in ``src/shared/container.py`` . Reads and write methods (``create``/``update``/``soft_delete``)
land here with the product-CRUD feature, each persisting an outbox row in the same txn.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from src.shared.db.outbox import OutboxMessage

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# return type is the adapter's ORM/read-model row (Product / ProductRow), typed
# as Any because ports must not import adapters (ports <- adapters). Upgrade to a domain
# schema type here once catalog gets a real domain layer.
#
# ``outbox`` is the :class:`OutboxMessage` (event type + serialized payload) the
# adapter INSERTs into ``catalog.outbox`` in the same transaction as the state
# change (transactional outbox — never a dual-write to the bus from the request path).

# The image-pipeline writes take a *factory* instead of a ready-made message: the
# worker must not read the product first and publish what it read, because a
# concurrent merchant edit between that read and the flip would publish stale
# fields. The adapter calls this with the guarded UPDATE's ``RETURNING`` row
# (product_id, merchant_id, name, price, category) inside the same transaction,
# so the payload is always the post-update state.
ImageOutboxFactory = Callable[[Mapping[str, Any]], OutboxMessage]


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
        outbox: OutboxMessage,
    ) -> Any: ...

    async def update_product(self, product: Any, changes: dict[str, object], outbox: OutboxMessage) -> Any: ...

    async def soft_delete_product(self, product: Any, outbox: OutboxMessage) -> None: ...

    async def set_image_pending(self, product: Any, upload_token: str, outbox: OutboxMessage | None = None) -> None: ...

    async def mark_image_ready(
        self, product_id: uuid.UUID, upload_token: str, image_key: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...

    async def mark_image_failed(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...
