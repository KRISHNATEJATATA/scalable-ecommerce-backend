"""SQLAlchemy models for the ``inventory`` schema."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
    """A hold against ``Inventory`` for one order line, released by the reaper on expiry."""

    __tablename__ = "reservations"
    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_reservations_qty_positive"),
        {"schema": SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(ForeignKey(f"{SCHEMA}.inventory.sku", ondelete="CASCADE"), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)  # id-value ref to orders.orders
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="held")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for inventory-originated events (``StockReserved/Released``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("inventory"), {"schema": SCHEMA})
