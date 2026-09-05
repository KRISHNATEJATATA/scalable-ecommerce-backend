"""Cart application DTOs — the service layer's input/output shapes.

Live in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports these for route type hints / OpenAPI.

Quantities arrive as plain ``int`` (no Pydantic range): the caps are
settings-driven, so the *service* enforces them and answers 400
(:class:`InvalidCartOperationError`) — a schema ``ge/le`` would surface 422
and contradict the frozen frontend contract.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class CartItemResponse(BaseModel):
    """One cart line — a product snapshot plus the chosen quantity."""

    model_config = ConfigDict(from_attributes=True)

    product_id: uuid.UUID
    name: str
    unit_price: Decimal  # decimal-as-string on the wire (never float)
    image_url: str | None
    quantity: int


class CartResponse(BaseModel):
    """The caller's cart. An empty cart is ``{items: [], updated_at}``, never 404."""

    model_config = ConfigDict(from_attributes=True)

    items: list[CartItemResponse]
    updated_at: datetime


class CartAddItem(BaseModel):
    """Add-to-cart payload: the line is created or incremented (clamped to the cap)."""

    model_config = ConfigDict(extra="forbid")

    product_id: uuid.UUID
    quantity: int


class CartUpdateItem(BaseModel):
    """Set-quantity payload: ``0`` removes the line."""

    model_config = ConfigDict(extra="forbid")

    quantity: int
