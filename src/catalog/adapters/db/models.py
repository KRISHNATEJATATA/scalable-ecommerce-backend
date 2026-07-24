"""SQLAlchemy models for the ``catalog`` schema."""

import uuid
from decimal import Decimal

from sqlalchemy import CheckConstraint, Index, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column

from src.shared.db.mixins import OutboxMixin, SoftDeleteMixin, TimestampMixin, VersionIdMixin, outbox_unpublished_index

SCHEMA = "catalog"


class Base(DeclarativeBase):
    pass


class Product(Base, TimestampMixin, SoftDeleteMixin, VersionIdMixin):
    """A merchant's listing. ``merchant_id`` is an id-value reference to
    ``identity.users`` (a User with the ``merchant`` role) — never a
    cross-schema FK (see CONTEXT-MAP.md relationships).
    """

    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_products_price_positive"),
        Index("ix_products_name", "name"),
        Index("ix_products_category", "category"),
        Index("ix_products_merchant_id", "merchant_id"),
        {"schema": SCHEMA},
    )

    @declared_attr
    def __mapper_args__(cls) -> dict:
        # version_id_col must be the actual Column object, resolved lazily
        # once the table is built (the mixin column isn't available yet at
        # class-body-evaluation time).
        return {"version_id_col": cls.__table__.c.version_id}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    image_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for catalog-originated events (``ProductCreated/Updated/Deleted``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("catalog"), {"schema": SCHEMA})
