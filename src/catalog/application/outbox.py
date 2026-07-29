"""Shared builders for catalog outbox rows.

One place to construct the ``(event_type, payload)`` tuple the repository persists
to the transactional outbox, so the identical ``ProductUpdated`` envelope isn't
hand-rolled at each call site (product edit, presign re-upload, image-worker flip).
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from src.events.models import ProductUpdated, ProductWriteData
from src.shared.config.logging import request_id_ctx


def product_updated_outbox(
    *,
    product_id: uuid.UUID,
    merchant_id: uuid.UUID,
    name: str,
    price: Decimal,
    category: str | None,
) -> tuple[str, str]:
    """Build the ``ProductUpdated`` outbox tuple (type + serialized payload).

    ``trace_id`` is pulled from the ambient request context so the event carries
    the originating trace. Used for cache invalidation on any change that alters a
    product's cached response — a field edit or an image-state transition.
    """
    event = ProductUpdated(
        trace_id=request_id_ctx.get(),
        data=ProductWriteData(
            product_id=product_id,
            merchant_id=merchant_id,
            name=name,
            price=price,
            category=category,
        ),
    )
    return (event.type, event.model_dump_json())
