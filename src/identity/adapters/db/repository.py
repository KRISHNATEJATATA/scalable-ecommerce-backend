"""Identity repository — plain ORM lookups keyed by OIDC ``sub`` / local id.

No soft-delete filter: disabled (``is_active = false``) users must still resolve
so FKs anchor and JIT provisioning can find an existing row.

Ports/repos return the ORM ``User`` row, not a domain entity.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from src.identity.adapters.db.models import Outbox, User
from src.identity.ports.repository import OutboxFactory


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

    async def get_or_create(self, oidc_sub: str, email: str, outbox: OutboxFactory | None = None) -> User:
        """JIT-provision the local mirror, idempotent & race-safe.

        A single ``INSERT ... ON CONFLICT (oidc_sub) DO UPDATE`` survives the
        concurrent-first-request race on the ``UNIQUE(oidc_sub)`` constraint:
        the loser's insert conflicts and the ``DO UPDATE`` returns the existing
        row instead of raising. Commits in the request session.

        When the row is genuinely **inserted**, ``outbox`` builds the ``UserCreated``
        event and it is written in the *same* transaction as the insert (state +
        outbox atomically — never a dual-write). ``xmax = 0`` is Postgres' marker
        for "this returned row came from the INSERT, not the conflicting UPDATE",
        so the loser of a concurrent JIT race emits nothing and creation events
        can't duplicate.
        """
        stmt = (
            pg_insert(User)
            .values(oidc_sub=oidc_sub, email=email)
            .on_conflict_do_update(index_elements=["oidc_sub"], set_={"updated_at": func.now()})
            .returning(User, text("(xmax = 0) AS inserted"))
        )
        row, inserted = (await self._session.execute(stmt)).one()
        if inserted and outbox is not None:
            message = outbox(row.id, row.email)
            self._session.add(Outbox(event_type=message.event_type, payload=message.payload))
        await self._session.commit()
        return row

    async def set_active(self, oidc_sub: str, is_active: bool) -> User | None:
        """Flip the local ``is_active`` mirror; returns the row (``None`` if absent)."""
        stmt = update(User).where(User.oidc_sub == oidc_sub).values(is_active=is_active).returning(User)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        await self._session.commit()
        return row
