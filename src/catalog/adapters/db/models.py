"""SQLAlchemy models for the ``catalog`` schema."""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Identity, Index, Integer, Numeric, String, text
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
        # Keyset pagination sorts by ``(<sort col>, id)`` and always filters
        # ``deleted_at IS NULL``, so each sortable column gets a partial composite
        # index the ORDER BY can seek on instead of sort-then-discard.
        # ponytail: filters (category/merchant_id) keep their own single-column
        # index and are applied as a bitmap/filter on top; add
        # ``(category, created_at, id)``-style composites only if a filtered
        # listing shows up in slow-query logs — one per filter×sort pair is a
        # combinatorial explosion nobody should pay for speculatively.
        Index("ix_products_created_at_id", "created_at", "id", postgresql_where=text("deleted_at IS NULL")),
        Index("ix_products_price_id", "price", "id", postgresql_where=text("deleted_at IS NULL")),
        Index("ix_products_name_id", "name", "id", postgresql_where=text("deleted_at IS NULL")),
        Index("ix_products_category", "category"),
        Index("ix_products_merchant_id", "merchant_id"),
        # Mirror of the index the e2a7f1c48d90 migration creates: the abandoned-upload
        # reaper scans due pending uploads only. Declared here so autogenerate sees it
        # in the metadata — a migration-only index would otherwise be dropped by the
        # next `alembic revision --autogenerate`.
        Index(
            "ix_products_pending_upload_expiry",
            "image_upload_expires_at",
            postgresql_where=text(f"image_status = '{ImageStatus.PENDING.value}'"),
        ),
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
    # ``image_upload_expires_at`` is when that presigned POST stops being accepted;
    # past it (plus a grace for in-flight processing) an abandoned upload is reaped
    # so the product doesn't sit `pending` — and imageless — forever.
    image_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    image_status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=ImageStatus.NONE.value)
    image_upload_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    image_upload_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for catalog-originated events (``ProductCreated/Updated/Deleted``)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("catalog"), {"schema": SCHEMA})


class ImageReclaim(Base):
    """Public image renditions that must be deleted from S3 (durable cleanup queue).

    Same idea as the outbox, for object storage instead of the bus: the row is
    written **in the same transaction** as the image flip that orphaned the key, so
    a crash between "the DB says this image is replaced" and "S3 no longer has it"
    can only ever leave work *to do*, never work silently lost. ``public/`` is live
    CDN content outside the ``uploads/`` lifecycle rule, so nothing else would ever
    reclaim these objects. Drained with retries + backoff by the image worker.
    """

    __tablename__ = "image_reclaim"
    __table_args__ = (Index("ix_image_reclaim_due", "next_attempt_at"), {"schema": SCHEMA})

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False, unique=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
