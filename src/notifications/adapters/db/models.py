"""SQLAlchemy models for the ``notifications`` schema.

Three small tables, no outbox (this module emits no events — it consumes them):

* ``recipients`` — the bus-delivered materialization of ``UserCreated``
  (user_id → email). ``user_id`` is an id-value ref to ``identity.users``
  (no cross-module FK). The send path resolves the recipient here, so the
  consumer never reads identity/Keycloak directly.
* ``sent_emails`` — the idempotency backstop: ``UNIQUE(order_id, email_type)``
  written with the send flow, so a dedupe-TTL expiry can never double-send.
* ``email_suppressions`` — hard bounce / spam complaint ⇒ never send again.
  The check is live now; the writer is the SES bounce/complaint consumer,
  a future SES step (documented in docs/DEPLOYMENT.md).
"""

import uuid

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.shared.db.mixins import TimestampMixin

SCHEMA = "notifications"

__all__ = ["SCHEMA", "Base", "EmailSuppression", "Recipient", "SentEmail"]


class Base(DeclarativeBase):
    pass


class Recipient(Base, TimestampMixin):
    """``UserCreated`` materialized: which email belongs to which local user id."""

    __tablename__ = "recipients"
    __table_args__ = {"schema": SCHEMA}

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)


class SentEmail(Base, TimestampMixin):
    """One sent confirmation. ``UNIQUE(order_id, email_type)`` is the send backstop."""

    __tablename__ = "sent_emails"
    __table_args__ = (
        UniqueConstraint("order_id", "email_type", name="uq_sent_emails_order_id_email_type"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )  # id-value ref to orders.orders (no cross-module FK)
    email_type: Mapped[str] = mapped_column(String(64), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)


class EmailSuppression(Base, TimestampMixin):
    """A recipient that must never be mailed again (hard bounce / spam complaint)."""

    __tablename__ = "email_suppressions"
    __table_args__ = (UniqueConstraint("recipient", name="uq_email_suppressions_recipient"), {"schema": SCHEMA})

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
