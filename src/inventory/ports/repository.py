"""Port (Protocol) for the inventory repository.

Implemented by ``adapters/db/repository.InventoryRepository``. Ships only the
raw conditional-decrement CAS primitive this phase; the reservation-row INSERT,
TTL/reaper, and ``StockReserved/Released`` events compose it in ticket 10.
"""

from __future__ import annotations

from typing import Any, Protocol

# return type is the adapter's ORM Inventory row, typed as Any because
# ports must not import adapters (ports <- adapters). Upgrade to a domain schema
# type once inventory gets a real domain layer.


class InventoryRepositoryPort(Protocol):
    async def get_by_sku(self, sku: str) -> Any | None: ...

    async def try_reserve_decrement(self, sku: str, qty: int) -> int: ...
