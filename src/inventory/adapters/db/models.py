"""SQLAlchemy models for the ``inventory`` schema."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.inventory.domain.reservation import ReservationStatus
from src.shared.db.mixins import ManualVersionMixin, OutboxMixin, TimestampMixin, outbox_unpublished_index

SCHEMA = "inventory"


class Base(DeclarativeBase):
    pass


class Inventory(Base, ManualVersionMixin):
    """On-hand/reserved stock for a SKU.

    Reservation/release is a raw ``UPDATE ... WHERE version = :v`` compare-
    and-swap (see ``ManualVersionMixin``) so the atomic conditional decrement
    (``on_hand - reserved >= :qty``) and the version bump happen in one
    statement, not through the ORM unit of work.
    """

    __tablename__ = "inventory"
    __table_args__ = (
        CheckConstraint("on_hand >= 0", name="ck_inventory_on_hand_non_negative"),
        CheckConstraint("reserved >= 0", name="ck_inventory_reserved_non_negative"),
        CheckConstraint("reserved <= on_hand", name="ck_inventory_reserved_lte_on_hand"),
        {"schema": SCHEMA},
    )

    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    on_hand: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Reservation(Base, TimestampMixin):
    """A hold against ``Inventory`` for one order line, released by the reaper on expiry.

    ``uq_reservations_active_order_sku`` is the idempotency guard: a saga step that
    retries (or a duplicated command) hits the constraint instead of placing a
    second hold on the same line. The reserve transaction inserts this row *first*
    and only then runs the CAS decrement — one lock order with every other path
    (reservation row, then inventory row), and it means the duplicate is rejected
    before any stock moves.

    The guard is **partial over the statuses that still own stock**, ``held`` and
    ``committed``. Covering only ``held`` would let a retry arriving after payment
    committed the line place a second hold and deduct the stock twice. ``released``
    is excluded, so a line whose hold was compensated or reaped can legitimately be
    reserved again.
    """

    __tablename__ = "reservations"
    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_reservations_qty_positive"),
        # Without this a typo'd status is an unreapable hold: it is neither `held`
        # (so the reaper's sweep skips it) nor terminal, and its stock never returns.
        CheckConstraint(
            "status IN ('held', 'released', 'committed')",
            name="ck_reservations_status_valid",
        ),
        Index(
            "uq_reservations_active_order_sku",
            "order_id",
            "sku",
            unique=True,
            postgresql_where=text("status IN ('held', 'committed')"),
        ),
        Index("ix_reservations_expiry_sweep", "expires_at", postgresql_where=text("status = 'held'")),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(ForeignKey(f"{SCHEMA}.inventory.sku", ondelete="CASCADE"), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)  # id-value ref to orders.orders
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=ReservationStatus.HELD.value)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for inventory-originated events (``StockReserved/Released``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("inventory"), {"schema": SCHEMA})
