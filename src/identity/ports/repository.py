"""Port (Protocol) for the identity repository.

Implemented by ``adapters/db/repository.IdentityRepository``. ``users`` is not
soft-delete filtered — disabled users must still resolve for FK anchoring and
JIT lookups (only the ``is_active`` mirror flips).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Protocol

from src.shared.db.outbox import OutboxMessage

# return type is the adapter's ORM User row, typed as Any because ports
# must not import adapters (ports <- adapters). Upgrade to a domain schema type once
# identity gets a real domain layer.

#: Builds the ``UserCreated`` outbox message from ``(user_id, email)``.
#: Defined here (the contract), imported by the adapter — never redeclared.
#: ``user_created_outbox`` satisfies it directly.
OutboxFactory = Callable[[uuid.UUID, str], OutboxMessage]


class IdentityRepositoryPort(Protocol):
    async def get_by_oidc_sub(self, oidc_sub: str) -> Any | None: ...

    async def get_by_id(self, user_id: uuid.UUID) -> Any | None: ...

    async def get_or_create(self, oidc_sub: str, email: str, outbox: OutboxFactory | None = None) -> Any: ...

    async def set_active(self, oidc_sub: str, is_active: bool) -> Any | None: ...
