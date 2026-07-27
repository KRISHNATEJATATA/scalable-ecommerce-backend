"""Catalog wire schemas — the public HTTP response shape for a product.

``from_attributes`` lets the service build this straight off the domain
``Product`` dataclass (attribute read, no dict round-trip).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


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


class ProductCreate(BaseModel):
    """Merchant create payload. ``merchant_id`` is never accepted from input —
    it is bound from the authenticated caller (ownership can't be spoofed).
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    category: str | None = Field(default=None, max_length=255)
    price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    image_key: str | None = Field(default=None, max_length=1024)


class ProductUpdate(BaseModel):
    """Partial merchant update — every field optional; unset fields are untouched."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    category: str | None = Field(default=None, max_length=255)
    price: Decimal | None = Field(default=None, gt=0, max_digits=12, decimal_places=2)
    image_key: str | None = Field(default=None, max_length=1024)
