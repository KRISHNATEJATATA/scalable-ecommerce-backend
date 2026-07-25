"""Identity repository — plain ORM lookups keyed by OIDC ``sub`` / local id.

No soft-delete filter: disabled (``is_active = false``) users must still resolve
so FKs anchor and JIT provisioning can find an existing row.

Ports/repos return the ORM ``User`` row, not a domain entity.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.identity.adapters.db.models import User


class IdentityRepository:
    """Implements :class:`src.identity.ports.repository.IdentityRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_oidc_sub(self, oidc_sub: str) -> User | None:
        stmt = select(User).where(User.oidc_sub == oidc_sub)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        stmt = select(User).where(User.id == user_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()
