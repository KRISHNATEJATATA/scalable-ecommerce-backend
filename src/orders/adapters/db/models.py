"""SQLAlchemy models for the ``orders`` schema."""

import uuid
from decimal import Decimal

from sqlalchemy import Enum, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# OrderStatus is owned by the domain (innermost layer); imported inward here for
# the Enum column. Re-exported so existing `from ...models import OrderStatus`
# callers (e.g. the repository) keep working.
from src.orders.domain.order import OrderStatus
from src.shared.db.mixins import OutboxMixin, TimestampMixin, outbox_unpublished_index

SCHEMA = "orders"

__all__ = ["SCHEMA", "Base", "Order", "OrderItem", "OrderStatus", "Outbox", "SagaLog"]


class Base(DeclarativeBase):
    pass


class Order(Base, TimestampMixin):
    """Checkout aggregate root. ``user_id`` is an id-value ref to ``identity.users``.

    ``UNIQUE(user_id, idempotency_key)`` is the durable guard for idempotent
    checkout: a replay with the same key returns the stored response, while the
    same key with a different body is rejected (409) by comparing
    ``idempotency_body_hash``. Composite (not global on the key alone) so two
    different users may reuse the same client-supplied key.
    """

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_orders_user_id_idempotency_key"),
        Index("ix_orders_user_id_status", "user_id", "status"),
        # ``list_orders`` is always user-scoped and keyset-ordered by
        # ``(created_at, id)`` — this is the index that ORDER BY seeks on.
        Index("ix_orders_user_id_created_at_id", "user_id", "created_at", "id"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    # sha256 over the checkout body (the payment token — the cart is
    # server-side, so the token is the whole body). Lets the DB backstop answer
    # "same key, different body → 409" even after the Valkey fast-path record
    # was evicted.
    idempotency_body_hash: Mapped[str] = mapped_column(String(64), nullable=False, server_default="")
    status: Mapped[OrderStatus] = mapped_column(
        Enum(
            OrderStatus,
            name="order_status",
            schema=SCHEMA,
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        ),
        nullable=False,
        default=OrderStatus.PENDING,
    )
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    # lazy="raise": accessing items without an explicit selectinload/joinedload
    # raises instead of firing a lazy N+1 query — the list path must eager-load.
    items: Mapped[list["OrderItem"]] = relationship("OrderItem", lazy="raise")


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
    """Persisted checkout-saga step log: drives recovery/compensation after a crash or timeout.

    One row per step attempt (``reserve``/``charge``/``commit``/``mark_paid``).
    ``status`` vocabulary: ``started`` → ``completed``, ``failed`` →
    ``compensated``, or ``unknown`` (a charge timeout with no recorded outcome —
    left pending for the reconciler/recovery poller, never compensated). The
    recovery poller claims stuck ``started`` rows (order still ``pending`` past
    the step timeout) with ``FOR UPDATE SKIP LOCKED`` — the row lock is the
    lease, so concurrent poller replicas split the batch instead of
    double-resuming a saga.
    """

    __tablename__ = "saga_log"
    __table_args__ = (
        # Every journal read is ``WHERE order_id`` (heartbeat in the recovery
        # claim, cancel's in-flight guard, the batch's re-check): without this
        # each is a full scan of the log.
        Index("ix_saga_log_order_id", "order_id"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(f"{SCHEMA}.orders.id", ondelete="CASCADE"), nullable=False)
    step: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="started")


class Outbox(Base, OutboxMixin):
    """Transactional outbox for orders-originated events (``OrderPlaced``, ...)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("orders"), {"schema": SCHEMA})
