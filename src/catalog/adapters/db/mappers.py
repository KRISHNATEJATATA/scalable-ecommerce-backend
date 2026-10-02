"""Catalog ORM/read-model → domain mappers.

Both catalog read paths land here: the raw-SQL list hot path builds a
:class:`ProductRow` read model (no ORM hydration) and the ORM paths hand back a
``Product`` row; :func:`to_domain` folds either into the frozen domain
:class:`~src.catalog.domain.product.Product` snapshot the ports expose. The
repository maps every row it returns, so the application layer only ever sees
domain snapshots — never a live ORM instance or a read-model dataclass — and
holds no session state of its own.

``ProductRow`` lives here (not in ``repository``) so this module can name both
inputs without a ``repository`` ↔ ``mappers`` import cycle.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from src.catalog.adapters.db.models import Product as ProductOrm
from src.catalog.domain.image_status import ImageStatus
from src.catalog.domain.product import Product


@dataclass(slots=True)
class ProductRow:
    """Lightweight read model for the raw catalog list (rows are not hydrated ORM)."""

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    image_status: str
    created_at: datetime
    updated_at: datetime
    version_id: int


def to_domain(row: ProductOrm | ProductRow) -> Product:
    """Map an ORM ``Product`` row or a raw ``ProductRow`` to a domain ``Product`` snapshot.

    Both expose the same attribute names, so one attribute-reading mapper covers
    the ORM ``get``/write paths and the raw-SQL list path.
    """
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
        version=row.version_id,
    )
