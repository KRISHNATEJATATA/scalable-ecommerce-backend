"""Inventory wire schemas — the stock row response + the stock upsert request.

``InventoryResponse`` lives in ``application.dto`` (layers contract:
application must not depend on api) and is re-exported here for route type
hints / OpenAPI.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from src.inventory.application.dto import InventoryResponse as InventoryResponse


class StockUpsertRequest(BaseModel):
    """The body of ``PUT /v1/admin/inventory/{sku}`` — the SKU's new ``on_hand``."""

    model_config = ConfigDict(extra="forbid")

    on_hand: int = Field(ge=0, le=1_000_000_000)
