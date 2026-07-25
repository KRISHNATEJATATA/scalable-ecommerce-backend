"""Port (Protocol) for the catalog read repository.

Structural contract implemented by ``adapters/db/repository.CatalogRepository``
and wired in ``src/shared/container.py`` (ticket 03). Reads only this phase;
``create``/``update``/``delete`` writes land with the catalog feature ticket.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# ponytail: return type is the adapter's ORM/read-model row (Product / ProductRow), typed
# as Any because ports must not import adapters (ports <- adapters). Upgrade to a domain
# schema type here once catalog gets a real domain layer.


class CatalogRepositoryPort(Protocol):
    async def list_products(self, params: PageParams, filters: dict[str, object] | None = None) -> Page[Any]: ...

    async def get_product(self, product_id: uuid.UUID) -> Any | None: ...
