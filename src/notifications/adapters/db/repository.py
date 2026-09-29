"""Notifications repository — implements the port over this module's schema.

All queries are local to the ``notifications`` schema (id-value refs to other
modules, never cross-schema joins). ``claim_send`` is the durable claim: the
``pending`` row commits BEFORE the send; a concurrent or repeated
claim hits UNIQUE(order_id, email_type), rolls back, and is resolved by the
recorded status — never mistranslated (any other integrity failure re-raises).
"""

from __future__ import annotations

import uuid

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.notifications.adapters.db.models import EmailSuppression, Recipient, SentEmail
from src.notifications.domain.email import EmailStatus
from src.notifications.ports.repository import ClaimOutcome, ClaimResult, NotificationRepositoryPort

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
        stmt = (
            select(SentEmail.id)
            .where(
                SentEmail.order_id == order_id,
                SentEmail.email_type == email_type,
                SentEmail.status == EmailStatus.SENT.value,
            )
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def claim_send(self, order_id: uuid.UUID, email_type: str, recipient: str) -> ClaimResult:
        """Insert the ``pending`` claim (fresh uuid4 message_id) and commit BEFORE
        the send; on the UNIQUE backstop, resolve from the recorded status AND
        reuse the persisted message_id (``sent`` ⇒ never resend; ``pending`` ⇒
        the prior attempt crashed mid-window — take it over with the same id).
        """
        message_id = uuid.uuid4()
        self._session.add(
            SentEmail(
                order_id=order_id,
                email_type=email_type,
                recipient=recipient,
                status=EmailStatus.PENDING.value,
                message_id=message_id,
            )
        )
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if _sqlstate(exc) != _UNIQUE_VIOLATION:
                raise
            stmt = select(SentEmail.status, SentEmail.message_id).where(
                SentEmail.order_id == order_id, SentEmail.email_type == email_type
            )
            status, persisted_id = (await self._session.execute(stmt)).one()
            outcome: ClaimOutcome = "already_sent" if status == EmailStatus.SENT.value else "takeover"
            return ClaimResult(outcome, persisted_id)
        return ClaimResult("claimed", message_id)

    async def mark_sent(self, order_id: uuid.UUID, email_type: str, recipient: str) -> None:
        """Flip the claimed row to ``sent`` (and the recipient actually used)."""
        await self._session.execute(
            update(SentEmail)
            .where(SentEmail.order_id == order_id, SentEmail.email_type == email_type)
            .values(status=EmailStatus.SENT.value, recipient=recipient, updated_at=func.now())
        )
        await self._session.commit()
