"""SQLAlchemy models for the ``notifications`` schema.

Four small tables, no outbox (this module emits no events — it consumes them):

* ``recipients`` — the bus-delivered materialization of ``UserCreated``
  (user_id → email). ``user_id`` is an id-value ref to ``identity.users``
  (no cross-module FK). The confirmation send path falls back to it when an
  ``OrderPlaced`` predates the event-carried address.
* ``order_recipients`` — the bus-delivered materialization of ``OrderPlaced``
  (order_id → email), upserted before any send decision. ``order_id`` is an
  id-value ref to ``orders.orders`` (no cross-module FK). This is the refund
  notice's address source: ``PaymentRefunded`` carries no recipient, and the
  projection is written from events the worker already consumes.
* ``sent_emails`` — the durable send state: ``UNIQUE(order_id, email_type)``
  claimed (``pending``) BEFORE the send and marked ``sent`` after, so every send
  has a durable record and a crash mid-window is visible + reconcilable.
  ``message_id`` is the uuid4 generated at claim time, reused by every resend.
* ``email_suppressions`` — hard bounce / spam complaint ⇒ never send again.
  The check is live now; the writer is the SES bounce/complaint consumer,
  a future SES step (documented in docs/DEPLOYMENT.md).
"""

import uuid

from sqlalchemy import CheckConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.notifications.domain.email import EmailStatus
from src.shared.db.mixins import TimestampMixin

SCHEMA = "notifications"

__all__ = ["SCHEMA", "Base", "EmailSuppression", "OrderRecipient", "Recipient", "SentEmail"]


class Base(DeclarativeBase):
    pass


class Recipient(Base, TimestampMixin):
    """``UserCreated`` materialized: which email belongs to which local user id."""

    __tablename__ = "recipients"
    __table_args__ = {"schema": SCHEMA}

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)


class OrderRecipient(Base, TimestampMixin):
    """``OrderPlaced`` materialized: which email belongs to which order id.

    The refund notice's address source — ``PaymentRefunded`` carries no
    recipient, so the worker resolves the Order Recipient here (from the
    ``OrderPlaced`` it already consumes, upserted before any send decision).
    """

    __tablename__ = "order_recipients"
    __table_args__ = {"schema": SCHEMA}

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )  # id-value ref to orders.orders (no cross-module FK)
    email: Mapped[str] = mapped_column(String(255), nullable=False)


class SentEmail(Base, TimestampMixin):
    """One confirmation's durable send state. ``UNIQUE(order_id, email_type)`` is the claim guard."""

    __tablename__ = "sent_emails"
    __table_args__ = (
        UniqueConstraint("order_id", "email_type", name="uq_sent_emails_order_id_email_type"),
        CheckConstraint("status IN ('pending', 'sent')", name="ck_sent_emails_status"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )  # id-value ref to orders.orders (no cross-module FK)
    email_type: Mapped[str] = mapped_column(String(64), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=EmailStatus.PENDING.value)
    # NULL only for rows that predate the claim state (they were all sent).
    message_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class EmailSuppression(Base, TimestampMixin):
    """A recipient that must never be mailed again (hard bounce / spam complaint)."""

    __tablename__ = "email_suppressions"
    __table_args__ = (UniqueConstraint("recipient", name="uq_email_suppressions_recipient"), {"schema": SCHEMA})

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
