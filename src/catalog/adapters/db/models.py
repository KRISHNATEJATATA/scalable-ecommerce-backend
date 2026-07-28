"""SQLAlchemy models for the ``catalog`` schema."""

import uuid
from decimal import Decimal

from sqlalchemy import CheckConstraint, Index, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column

from src.catalog.domain.image_status import IMAGE_STATUS_VALUES, ImageStatus
from src.shared.db.mixins import OutboxMixin, SoftDeleteMixin, TimestampMixin, VersionIdMixin, outbox_unpublished_index

SCHEMA = "catalog"

_IMAGE_STATUS_IN = ", ".join(f"'{v}'" for v in IMAGE_STATUS_VALUES)


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
        CheckConstraint(
            f"image_status IN ({_IMAGE_STATUS_IN})",
            name="ck_products_image_status",
        ),
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
    # ``image_key`` is the PROCESSED, public object key (set by the image worker
    # only after the upload passes sniff + re-encode). ``image_status`` tracks the
    # pipeline: none → pending (presigned, awaiting upload) → ready | failed.
    # ``image_upload_token`` is the token of the CURRENTLY-pending upload — the
    # worker only applies a result whose token matches, so a late/stale event for
    # a superseded upload can't clobber newer image state.
    image_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    image_status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=ImageStatus.NONE.value)
    image_upload_token: Mapped[str | None] = mapped_column(String(64), nullable=True)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for catalog-originated events (``ProductCreated/Updated/Deleted``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("catalog"), {"schema": SCHEMA})
