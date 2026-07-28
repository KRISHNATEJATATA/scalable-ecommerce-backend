"""Inventory read use-case.

Maps the repo row through the domain to the ``InventoryResponse`` Pydantic schema
so the ORM row never crosses the service boundary. No service method for
``try_reserve_decrement`` — that's a ticket-10 write primitive.
"""

from __future__ import annotations

from src.inventory.application.dto import InventoryResponse
from src.inventory.application.mappers import to_domain
from src.inventory.ports.repository import InventoryRepositoryPort


class InventoryService:
    """Read-side use-cases over the inventory stock model."""

    def __init__(self, repo: InventoryRepositoryPort) -> None:
        self._repo = repo

    async def get_by_sku(self, sku: str) -> InventoryResponse | None:
        """Resolve a stock row by SKU, or ``None`` if absent."""
        row = await self._repo.get_by_sku(sku)
        if row is None:
            return None
        return InventoryResponse.model_validate(to_domain(row))
