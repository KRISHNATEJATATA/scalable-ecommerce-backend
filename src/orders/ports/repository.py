"""Port (Protocol) for the orders read repository.

Implemented by ``adapters/db/repository.OrdersRepository``,
``user_id`` scope is repo-applied ownership, not a client filter.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# return type is the adapter's ORM Order row (with items eager-loaded),
# typed as Any because ports must not import adapters (ports <- adapters); the
# ``status`` filter is similarly untyped here. Upgrade to a domain schema type
# once orders gets a real domain layer.


class OrdersRepositoryPort(Protocol):
    async def list_orders(self, user_id: uuid.UUID, params: PageParams, status: Any | None = None) -> Page[Any]: ...

    async def get_order(self, order_id: uuid.UUID) -> Any | None: ...
