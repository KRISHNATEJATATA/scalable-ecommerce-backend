"""Shared builders for catalog outbox rows.

One place to construct the :class:`OutboxMessage` the repository persists to the
transactional outbox, so the identical ``ProductUpdated`` envelope isn't
hand-rolled at each call site (product edit, presign re-upload, image-worker flip).

``trace_id`` comes from the ambient request context; the **image worker** runs
outside any request, so an empty context falls back to a fresh id rather than an
empty string — same rule as the inventory reaper.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from src.events.models import ProductUpdatedV2, ProductWriteDataV2
from src.shared.config.logging import current_trace_id
from src.shared.db.outbox import OutboxMessage


def product_updated_outbox(
    *,
    product_id: uuid.UUID,
    merchant_id: uuid.UUID,
    name: str,
    price: Decimal,
    category: str | None,
    product_version: int,
) -> OutboxMessage:
    """Build the ``ProductUpdated`` outbox message (type + serialized payload).

    Used for cache invalidation on any change that alters a product's cached
    response — a field edit or an image-state transition.

    ``product_version`` must be the aggregate's ``version_id`` **after** the write
    this event describes, so an out-of-order delivery is detectable downstream.
    """
    event = ProductUpdatedV2.new(
        trace_id=current_trace_id(),
        data=ProductWriteDataV2(
            product_id=product_id,
            merchant_id=merchant_id,
            name=name,
            price=price,
            category=category,
            product_version=product_version,
        ),
    )
    return OutboxMessage(event.type, event.model_dump_json())
