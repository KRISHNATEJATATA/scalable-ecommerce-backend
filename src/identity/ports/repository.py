"""Port (Protocol) for the identity repository.

Implemented by ``adapters/db/repository.IdentityRepository``. ``users`` is not
soft-delete filtered — disabled users must still resolve for FK anchoring and
JIT lookups (only the ``is_active`` mirror flips).
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol

# return type is the adapter's ORM User row, typed as Any because ports
# must not import adapters (ports <- adapters). Upgrade to a domain schema type once
# identity gets a real domain layer.


class IdentityRepositoryPort(Protocol):
    async def get_by_oidc_sub(self, oidc_sub: str) -> Any | None: ...

    async def get_by_id(self, user_id: uuid.UUID) -> Any | None: ...
