"""Port (Protocol) for the catalog repository.

Structural contract implemented by ``adapters/db/repository.CatalogRepository``
and wired in ``src/shared/container.py`` . Reads and write methods (``create``/``update``/``soft_delete``)
land here with the product-CRUD feature, each persisting an outbox row in the same txn.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from src.shared.db.outbox import OutboxMessage

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# return type is the adapter's ORM/read-model row (Product / ProductRow), typed
# as Any because ports must not import adapters (ports <- adapters) — except for the
# subset the application layer reads by name, which :class:`ProductRecord` below
# declares structurally. Upgrade to a domain schema type here once catalog gets a
# real domain layer.
#
# ``outbox`` is the :class:`OutboxMessage` (event type + serialized payload) the
# adapter INSERTs into ``catalog.outbox`` in the same transaction as the state
# change (transactional outbox — never a dual-write to the bus from the request path).

# The image-pipeline writes take a *factory* instead of a ready-made message: the
# worker must not read the product first and publish what it read, because a
# concurrent merchant edit between that read and the flip would publish stale
# fields. The adapter calls this with the guarded UPDATE's ``RETURNING`` row
# (product_id, merchant_id, name, price, category, product_version) inside the same
# transaction, so the payload is always the post-update state — including the
# freshly incremented ``version_id``, which orders the event downstream.
ImageOutboxFactory = Callable[[Mapping[str, Any]], OutboxMessage]


@runtime_checkable
class ProductRecord(Protocol):
    """The attributes the **application layer** reads off a repository product row.

    ``get_product`` used to be typed ``Any | None``, which quietly let the service
    depend on fields no contract declared — in particular ``version_id``, which
    exists only on the adapter's ORM model (neither the domain ``Product`` nor the
    ``ProductRow`` read model carries it) yet every write use-case reads it to stamp
    ``product_version`` on the event it emits. import-linter can't catch that: it's
    an attribute, not an import, so the failure mode was a runtime ``AttributeError``
    from a test double that looked complete.

    ``@runtime_checkable`` so the claim is enforceable rather than decorative: this
    repo runs no type checker (CI is Ruff + pytest), so a bare Protocol would be
    IDE-and-docs only. Data-member protocols support ``isinstance`` on 3.12+, and
    the catalog tests assert it for both the ORM ``Product`` and their stand-in
    rows — an incomplete double now fails a test instead of production.

    Deliberately minimal: the *full* row (description, image state, timestamps) is
    still ``Any``, mapped by ``to_domain``. This is only what the use-cases touch
    directly. Note ``isinstance`` checks attribute *presence*, not types — enough
    to catch the omission that actually happens.
    """

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    price: Decimal
    category: str | None
    #: Optimistic-lock counter; ``+ 1`` is the ``product_version`` the event carries.
    version_id: int


class CatalogRepositoryPort(Protocol):
    async def list_products(self, params: PageParams, filters: dict[str, object] | None = None) -> Page[Any]: ...

    async def get_product(self, product_id: uuid.UUID) -> ProductRecord | None: ...

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

    async def update_product(
        self, product: ProductRecord, changes: dict[str, object], outbox: OutboxMessage
    ) -> Any: ...

    async def soft_delete_product(self, product: ProductRecord, outbox: OutboxMessage) -> None: ...

    async def set_image_pending(
        self, product: ProductRecord, upload_token: str, outbox: OutboxMessage | None = None
    ) -> None: ...

    async def mark_image_ready(
        self, product_id: uuid.UUID, upload_token: str, image_key: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...

    async def mark_image_failed(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...
