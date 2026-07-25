"""Port (Protocol) for the payments repository.

Implemented by ``adapters/db/repository.PaymentsRepository``. Returns every
payment attempt for an order (retries share ``order_id``; only
``idempotency_key`` is unique) — reconciliation decides which is authoritative.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.shared.db.pagination import Page, PageParams

# return type is the adapter's ORM Payment row, typed as Any because ports
# must not import adapters (ports <- adapters). Upgrade to a domain schema type once
# payments gets a real domain layer.


class PaymentsRepositoryPort(Protocol):
    async def get_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> Page[Any]: ...
