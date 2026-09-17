"""Notifications repository — implements the port over this module's schema.

All queries are local to the ``notifications`` schema (id-value refs to other
modules, never cross-schema joins). ``record_sent`` is the durable backstop: a
concurrent duplicate hits the UNIQUE(order_id, email_type), rolls back, and is
reported as "already sent" — never mistranslated.
"""

from __future__ import annotations

import uuid

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.notifications.adapters.db.models import EmailSuppression, Recipient, SentEmail
from src.notifications.ports.repository import NotificationRepositoryPort

# SQLSTATEs, not constraint names: names drift with migrations, these are standard.
_UNIQUE_VIOLATION = "23505"


def _sqlstate(exc: IntegrityError) -> str | None:
    """The SQLSTATE behind a SQLAlchemy ``IntegrityError`` (asyncpg nests one level deep)."""
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        state = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if state:
            return str(state)
    return None


class NotificationRepository(NotificationRepositoryPort):
    """Implements :class:`src.notifications.ports.repository.NotificationRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_recipient(self, user_id: uuid.UUID, email: str) -> None:
        """Materialize ``UserCreated``: insert or refresh the email (idempotent on re-delivery)."""
        await self._session.execute(
            pg_insert(Recipient)
            .values(user_id=user_id, email=email)
            .on_conflict_do_update(
                index_elements=[Recipient.user_id],
                set_={"email": email, "updated_at": func.now()},
            )
        )
        await self._session.commit()

    async def get_recipient_email(self, user_id: uuid.UUID) -> str | None:
        stmt = select(Recipient.email).where(Recipient.user_id == user_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def is_suppressed(self, email: str) -> bool:
        stmt = select(EmailSuppression.id).where(EmailSuppression.recipient == email).limit(1)
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def has_sent(self, order_id: uuid.UUID, email_type: str) -> bool:
        stmt = select(SentEmail.id).where(SentEmail.order_id == order_id, SentEmail.email_type == email_type).limit(1)
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def record_sent(self, order_id: uuid.UUID, email_type: str, recipient: str) -> bool:
        """Record one sent confirmation; ``False`` when the UNIQUE backstop says it exists.

        Any other integrity failure is re-raised, never mistranslated into "already sent".
        """
        self._session.add(SentEmail(order_id=order_id, email_type=email_type, recipient=recipient))
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if _sqlstate(exc) != _UNIQUE_VIOLATION:
                raise
            return False
        return True
