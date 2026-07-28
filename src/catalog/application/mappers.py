"""Catalog mappers: repo row (ORM ``Product`` or raw ``ProductRow``) → domain ``Product``.

Both the list hot path (``ProductRow``) and the ``get`` path (ORM ``Product``)
expose the same attributes, so one attribute-reading mapper covers both. Typed
``Any`` to avoid an application → adapters import (the row is duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.catalog.domain.image_status import ImageStatus
from src.catalog.domain.product import Product


def to_domain(row: Any) -> Product:
    """Map a repo row (ORM ``Product`` or ``ProductRow``) to a domain ``Product``."""
    return Product(
        id=row.id,
        merchant_id=row.merchant_id,
        name=row.name,
        description=row.description,
        category=row.category,
        price=row.price,
        image_key=row.image_key,
        image_status=ImageStatus(row.image_status),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
