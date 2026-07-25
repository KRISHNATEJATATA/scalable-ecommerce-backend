"""Identity read use-cases.

Maps repo rows through the domain to the ``UserResponse`` Pydantic schema so the
ORM row never crosses the service boundary. No soft-delete filter: disabled users
still resolve so FKs anchor and JIT lookups find an existing row. Missing row →
``None`` (the route maps ``None`` → 404 in its feature ticket).
"""

from __future__ import annotations

import uuid

from src.identity.api.schemas import UserResponse
from src.identity.application.mappers import to_domain
from src.identity.ports.repository import IdentityRepositoryPort


class IdentityService:
    """Read-side use-cases over the local user mirror."""

    def __init__(self, repo: IdentityRepositoryPort) -> None:
        self._repo = repo

    async def get_by_oidc_sub(self, oidc_sub: str) -> UserResponse | None:
        """Resolve a user by their OIDC ``sub``, or ``None`` if absent."""
        row = await self._repo.get_by_oidc_sub(oidc_sub)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))

    async def get_by_id(self, user_id: uuid.UUID) -> UserResponse | None:
        """Resolve a user by primary key, or ``None`` if absent."""
        row = await self._repo.get_by_id(user_id)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))
