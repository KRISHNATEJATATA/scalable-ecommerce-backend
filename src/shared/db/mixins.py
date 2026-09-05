"""SQLAlchemy declarative mixins shared across module schemas.

Each module owns its own declarative ``Base`` (own metadata, own schema, own
Alembic chain). These mixins just keep the recurring columns (timestamps, soft-delete, optimistic
locking) consistent across modules without sharing a metadata object.

Two distinct optimistic-lock mechanisms exist on purpose:
- ``VersionIdMixin``: SQLAlchemy's built-in ``version_id_col`` (ORM-managed,
  bumped automatically on every UPDATE). Use for aggregates updated through
  the ORM, e.g. ``catalog.Product``.
- ``ManualVersionMixin``: a plain ``version`` column bumped by hand inside raw
  single-statement writes whose *arithmetic guard* is the real concurrency
  control (e.g. ``inventory.inventory``: ``WHERE on_hand - reserved >= :qty``).
  The column exists so a future compare-and-swap (``WHERE version = :v``) can be
  layered on without a migration — today nothing branches on it.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, validates

from src.shared.db.outbox import MAX_OUTBOX_PAYLOAD_BYTES, OutboxPayloadTooLargeError


class TimestampMixin:
    """``created_at`` / ``updated_at``, server-side defaults."""

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class SoftDeleteMixin:
    """``deleted_at``; repositories filter ``WHERE deleted_at IS NULL``."""

    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class VersionIdMixin:
    """ORM-managed optimistic lock column; pair with ``__mapper_args__ = {"version_id_col": ...}``."""

    version_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class ManualVersionMixin:
    """Optimistic lock column for hand-rolled compare-and-swap updates."""

    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class OutboxMixin:
    """Transactional-outbox columns, one table per event-publishing module.

    Written in the same DB transaction as the state change it announces
    (never a separate dual-write). A relay poller (later phase) ships
    unpublished rows to SNS; pair with :func:`outbox_unpublished_index` for
    the ``published_at IS NULL`` partial index that keeps that poll cheap.
    """

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[str] = mapped_column(String, nullable=False)  # serialized JSON event body
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    @validates("payload")
    def _reject_oversized_payload(self, _key: str, payload: str) -> str:
        """Refuse a payload the bus can never carry (SNS/SQS cap at 256 KB).

        Enforced here rather than per call site so every current and future
        producer inherits it: one oversized row would otherwise sit unpublished
        ahead of its schema's lane in the relay and block that module's events
        indefinitely.
        """
        if len(payload.encode("utf-8")) > MAX_OUTBOX_PAYLOAD_BYTES:
            raise OutboxPayloadTooLargeError(
                f"payload of {len(payload.encode('utf-8'))} bytes exceeds "
                f"MAX_OUTBOX_PAYLOAD_BYTES ({MAX_OUTBOX_PAYLOAD_BYTES}); the bus cannot carry it"
            )
        return payload


def outbox_unpublished_index(module: str) -> Index:
    """Partial index the relay poller scans: ``WHERE published_at IS NULL``."""

    return Index(f"ix_{module}_outbox_unpublished", "published_at", postgresql_where=text("published_at IS NULL"))
