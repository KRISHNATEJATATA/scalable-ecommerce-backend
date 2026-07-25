"""Inventory wire schema — the response shape for a stock row.

``from_attributes`` lets the service build this straight off the domain
``Inventory`` dataclass. ``version`` is the optimistic-lock column, surfaced so
callers can pass it back on a conditional write.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class InventoryResponse(BaseModel):
    """The ``inventory.inventory`` stock row as returned by the service layer."""

    model_config = ConfigDict(from_attributes=True)

    sku: str
    on_hand: int
    reserved: int
    version: int
