"""Identity repository — plain ORM lookups keyed by OIDC ``sub`` / local id.

No soft-delete filter: disabled (``is_active = false``) users must still resolve
so FKs anchor and JIT provisioning can find an existing row.

Ports/repos return the ORM ``User`` row, not a domain entity.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from src.identity.adapters.db.models import Outbox, User
from src.identity.ports.repository import OutboxFactory


def _lock_key(oidc_sub: str) -> int:
    """Stable signed-bigint advisory-lock key for an ``oidc_sub``.

    Hashed in Python rather than with Postgres' ``hashtext`` so the key does not
    depend on an undocumented server function.
    """
    return int.from_bytes(hashlib.blake2b(oidc_sub.encode(), digest_size=8).digest(), "big", signed=True)


class IdentityRepository:
    """Implements :class:`src.identity.ports.repository.IdentityRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def lock(self, oidc_sub: str) -> None:
        """Take the per-``oidc_sub`` advisory lock for this transaction.

        Held until the session commits/rolls back, so a caller that needs several
        statements (admin disable: Keycloak call, then the mirror write) excludes a
        concurrent JIT provision for the *whole* flow — not just its final write.
        """
        await self._session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _lock_key(oidc_sub)})

    async def get_by_oidc_sub(self, oidc_sub: str) -> User | None:
        stmt = select(User).where(User.oidc_sub == oidc_sub)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        stmt = select(User).where(User.id == user_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_or_create(
        self,
        oidc_sub: str,
        email: str,
        outbox: OutboxFactory | None = None,
        *,
        is_active: bool = True,
    ) -> User:
        """JIT-provision the local mirror, idempotent & race-safe.

        A single ``INSERT ... ON CONFLICT (oidc_sub) DO UPDATE`` survives the
        concurrent-first-request race on the ``UNIQUE(oidc_sub)`` constraint:
        the loser's insert conflicts and the ``DO UPDATE`` returns the existing
        row instead of raising. Commits in the request session.

        ``is_active=False`` provisions the row **already disabled** in that same
        statement, so an admin disable can never leave a locally-active mirror
        behind (no insert-then-update window to crash in).

        The mirror deliberately has **no ``UNIQUE(email)``** (see ``models.py``):
        Keycloak issues a new ``sub`` when an account is recreated, and that
        recreated account is a new principal — it gets its own row rather than
        inheriting the previous holder's orders and products.

        When the row is genuinely **inserted**, ``outbox`` builds the ``UserCreated``
        event and it is written in the *same* transaction as the insert (state +
        outbox atomically — never a dual-write). ``xmax = 0`` is Postgres' marker
        for "this returned row came from the INSERT, not the conflicting UPDATE",
        so the loser of a concurrent JIT race emits nothing and creation events
        can't duplicate.
        """
        # Serialise against a concurrent admin disable of the same account: without
        # this, JIT can insert-and-return an *active* mirror while disable is still
        # talking to Keycloak, and that request proceeds as an enabled user even
        # though the final row ends up disabled. Re-entrant, released on commit.
        await self.lock(oidc_sub)
        # Keycloak owns the email too, so an existing row follows it on re-login.
        conflict_set: dict[str, Any] = {"email": email, "updated_at": func.now()}
        if not is_active:
            conflict_set["is_active"] = False
        stmt = (
            pg_insert(User)
            .values(oidc_sub=oidc_sub, email=email, is_active=is_active)
            .on_conflict_do_update(index_elements=["oidc_sub"], set_=conflict_set)
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
