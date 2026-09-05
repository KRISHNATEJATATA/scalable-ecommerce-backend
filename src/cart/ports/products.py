"""Port (Protocol) for the product snapshots a cart line carries.

Implemented at the composition root (``src/shared/container.py``) over the
catalog service — cart never names catalog (the ``module-independence``
contract forbids even application-layer imports between modules). Read-only:
``None`` means unknown or soft-deleted, and the caller answers 404.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ProductSnapshot:
    """The catalog facts a cart line snapshots at add time."""

    product_id: uuid.UUID
    name: str
    unit_price: Decimal
    image_url: str | None


class CartProductPort(Protocol):
    async def get_snapshot(self, product_id: uuid.UUID) -> ProductSnapshot | None:
        """The product's current snapshot, or ``None`` if unknown/soft-deleted."""
        ...
