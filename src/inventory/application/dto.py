"""Inventory application DTO — the service layer's output shape.

Lives in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports this for route type hints / OpenAPI.
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
