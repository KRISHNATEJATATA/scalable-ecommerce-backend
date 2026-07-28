"""Inventory wire schema — the response shape for a stock row.

``InventoryResponse`` lives in ``application.dto`` (layers contract:
application must not depend on api) and is re-exported here for route type
hints / OpenAPI.
"""

from __future__ import annotations

from src.inventory.application.dto import InventoryResponse as InventoryResponse
