"""SQLAlchemy models for the ``payments`` schema."""

import uuid
from decimal import Decimal

from sqlalchemy import Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.shared.db.mixins import OutboxMixin, TimestampMixin, outbox_unpublished_index

SCHEMA = "payments"


class Base(DeclarativeBase):
    pass


class Payment(Base, TimestampMixin):
    """A payment attempt against the stub gateway. ``order_id`` is an id-value ref to ``orders.orders``.

    ``idempotency_key`` is the checkout idempotency key propagated to the gateway so a
    retried charge can't double-charge (spec.md:224-226). ``gateway_ref`` is the
    token/reference the gateway returns — never raw card data (PCI SAQ-A).
    """

    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    gateway_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")


class Outbox(Base, OutboxMixin):
    """Transactional outbox for payments-originated events (``PaymentSucceeded/Failed``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("payments"), {"schema": SCHEMA})
