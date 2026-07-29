"""Inventory application DTO — the service layer's output shape.

Lives in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports this for route type hints / OpenAPI.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from src.inventory.domain.reservation import ReservationStatus


class InventoryResponse(BaseModel):
    """The ``inventory.inventory`` stock row as returned by the service layer."""

    model_config = ConfigDict(from_attributes=True)

    sku: str
    on_hand: int
    reserved: int
    version: int

    @property
    def available(self) -> int:
        """Free-to-sell units (``on_hand`` minus what is currently held)."""
        return self.on_hand - self.reserved


class ReservationResponse(BaseModel):
    """A stock hold as returned by the service layer (never the ORM row)."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    sku: str
    qty: int
    order_id: uuid.UUID
    status: ReservationStatus
    expires_at: datetime
