"""Port (Protocol) for the catalog repository.

Structural contract implemented by ``adapters/db/repository.CatalogRepository``
and wired in ``src/bootstrap/container.py`` . Reads and write methods (``create``/``update``/``soft_delete``)
land here with the product-CRUD feature, each persisting an outbox row in the same txn.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

from src.catalog.domain.product import Product
from src.shared.db.outbox import OutboxMessage

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# Every read returns a frozen domain :class:`Product` snapshot (mapped by the
# adapter in ``src/catalog/adapters/db/mappers.py``), never an ORM row or the raw
# ``ProductRow`` read model — callers hold no session state and need no knowledge
# of SQLAlchemy's identity map. Writes take ids, not rows, so the adapter owns
# reloading the aggregate it mutates.
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


class PendingUpload(NamedTuple):
    """A product still awaiting bytes for a presigned upload (reaper candidate)."""

    product_id: uuid.UUID
    upload_token: str


class ImageReclaimTask(NamedTuple):
    """One claimed row of the durable image-cleanup queue."""

    id: int
    product_id: uuid.UUID
    object_key: str
    attempts: int


class ImageFlip(NamedTuple):
    """Outcome of the guarded ``pending → ready`` image flip.

    ``previous_key`` is the ``image_key`` the flip replaced (``None`` when the
    product had no image, or when the guards rejected the write). It is read in
    the same locked statement so the caller can reclaim the superseded public
    renditions — nothing else ever would: ``public/`` is live CDN content and sits
    outside the ``uploads/`` lifecycle rule.
    """

    applied: bool
    previous_key: str | None


class CatalogRepositoryPort(Protocol):
    async def list_products(
        self, params: PageParams, filters: dict[str, object] | None = None, *, search: str | None = None
    ) -> Page[Product]: ...

    async def get_product(self, product_id: uuid.UUID) -> Product | None: ...

    async def get_products_by_ids(self, product_ids: list[uuid.UUID]) -> Sequence[Product]:
        """Every live product in ``product_ids``, in no guaranteed order.

        The authoritative (uncached) batch read — checkout's price revalidation
        reads here precisely because the service's cache-aside can lag the DB.
        Soft-deleted and unknown ids are simply absent from the result.
        """
        ...

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
    ) -> Product: ...

    async def update_product(
        self, product_id: uuid.UUID, changes: dict[str, object], outbox: OutboxMessage
    ) -> Product: ...

    async def soft_delete_product(self, product_id: uuid.UUID, outbox: OutboxMessage) -> None: ...

    async def set_image_pending(
        self,
        product_id: uuid.UUID,
        upload_token: str,
        *,
        expires_at: datetime,
        outbox: OutboxMessage | None = None,
    ) -> None: ...

    async def mark_image_ready(
        self, product_id: uuid.UUID, upload_token: str, image_key: str, outbox: ImageOutboxFactory | None = None
    ) -> ImageFlip: ...

    async def mark_image_failed(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...

    async def current_image_key(self, product_id: uuid.UUID) -> str | None: ...

    async def schedule_image_reclaim(self, product_id: uuid.UUID, object_key: str) -> None: ...

    async def claim_image_reclaims(self, *, batch_size: int) -> list[ImageReclaimTask]: ...

    async def finish_image_reclaim(self, ids: list[int]) -> None: ...

    async def defer_image_reclaim(self, task_id: int, *, delay_seconds: int, error: str) -> None: ...

    async def due_pending_uploads(self, *, grace_seconds: int, batch_size: int) -> list[PendingUpload]: ...

    async def expire_abandoned_upload(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool: ...

    async def defer_upload_expiry(self, product_id: uuid.UUID, upload_token: str, *, delay_seconds: int) -> None: ...
