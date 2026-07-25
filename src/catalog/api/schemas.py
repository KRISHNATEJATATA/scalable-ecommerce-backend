"""Catalog wire schemas — the public HTTP response shape for a product.

``from_attributes`` lets the service build this straight off the domain
``Product`` dataclass (attribute read, no dict round-trip).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class ProductResponse(BaseModel):
    """The public HTTP response shape for a product."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    created_at: datetime
    updated_at: datetime
