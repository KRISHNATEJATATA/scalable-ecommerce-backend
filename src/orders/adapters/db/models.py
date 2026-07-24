"""SQLAlchemy models for the ``orders`` schema."""

import enum
import uuid
from decimal import Decimal

from sqlalchemy import Enum, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.shared.db.mixins import OutboxMixin, TimestampMixin, outbox_unpublished_index

SCHEMA = "orders"


class Base(DeclarativeBase):
    pass


class OrderStatus(enum.StrEnum):
    PENDING = "pending"
    PAID = "paid"
    FULFILLED = "fulfilled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class Order(Base, TimestampMixin):
    """Checkout aggregate root. ``user_id`` is an id-value ref to ``identity.users``.

    ``UNIQUE(user_id, idempotency_key)`` is the durable guard for idempotent
    checkout: a replay with the same key returns the stored response; the
    same key with a different body is rejected (409) at the service layer.
    """

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_orders_user_idempotency_key"),
        Index("ix_orders_user_id_status", "user_id", "status"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, name="order_status", schema=SCHEMA), nullable=False, default=OrderStatus.PENDING
    )
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)


class OrderItem(Base):
    """A line snapshotted at checkout time — price/name copied so later catalog edits don't rewrite history."""

    __tablename__ = "order_items"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(f"{SCHEMA}.orders.id", ondelete="CASCADE"), nullable=False)
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )  # id-value ref to catalog.products
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)  # snapshot
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)  # snapshot
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)


class SagaLog(Base, TimestampMixin):
    """Persisted checkout-saga step log: drives recovery/compensation after a crash or timeout."""

    __tablename__ = "saga_log"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(f"{SCHEMA}.orders.id", ondelete="CASCADE"), nullable=False)
    step: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="started")


class Outbox(Base, OutboxMixin):
    """Transactional outbox for orders-originated events (``OrderPlaced``, ...)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("orders"), {"schema": SCHEMA})
