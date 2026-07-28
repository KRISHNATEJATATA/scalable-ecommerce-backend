"""Catalog domain entity — pure Python, imports nothing outward.

Frozen slotted dataclass mirroring the ``catalog.products`` read model. No
SQLAlchemy, Pydantic, or FastAPI here (domain is the innermost layer).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from src.catalog.domain.image_status import ImageStatus


@dataclass(frozen=True, slots=True)
class Product:
    """The catalog product read model (immutable domain entity)."""

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    image_status: ImageStatus
    created_at: datetime
    updated_at: datetime
