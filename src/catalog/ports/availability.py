"""Port (Protocol) for catalog-side stock availability.

Implemented at the composition root (``src/shared/container.py``) over the
inventory service — catalog never names inventory (see ``CONTEXT-MAP.md`` and
the ``module-independence`` import-linter contract: no cross-module imports,
not even at the application layer). The seam is batch-shaped so a 20-item
listing costs one stock query, not 21.
"""

from __future__ import annotations

from typing import Protocol


class StockAvailabilityPort(Protocol):
    async def available_for(self, skus: list[str]) -> dict[str, int]:
        """Purchasable units per SKU (``max(on_hand - reserved, 0)``).

        Only SKUs with a stock row appear in the map; a missing key means
        *unknown* (never assume zero). Read-only — how rows come to exist is
        inventory's domain.
        """
        ...
