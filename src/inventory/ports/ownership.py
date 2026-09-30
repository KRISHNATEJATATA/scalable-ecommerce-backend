"""Port (Protocol) for resolving a stock SKU to its owning merchant.

The stock upsert is inventory's only external write surface, but ownership lives
in catalog (a SKU is ``str(product.id)``). Inventory declares the port; the
container implements it over the catalog repository — the one place allowed to
touch every module — so inventory never names catalog (the module-independence
contract).
"""

from __future__ import annotations

import uuid
from typing import Protocol


class StockOwnershipPort(Protocol):
    async def merchant_id_for_sku(self, sku: str) -> uuid.UUID | None:
        """The owning merchant's local ``users.id`` for ``sku``, or ``None``.

        ``None`` covers every unresolvable SKU alike — unknown product,
        soft-deleted product, or a string that isn't a product id at all — so
        the upsert answers 404 without distinguishing them.
        """
        ...
