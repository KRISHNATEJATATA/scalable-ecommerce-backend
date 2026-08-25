"""durable image-rendition reclaim queue

Adds ``catalog.image_reclaim``: object keys whose public renditions are no longer
referenced and must be deleted from S3.

Cleanup used to be a best-effort delete after the DB commit, so a worker crash or
an S3 outage left the replaced/unreferenced ``public/`` objects addressable
forever — ``public/`` is live CDN content and deliberately sits outside the
``uploads/`` lifecycle rule, so nothing else ever reclaims them. The intent is now
persisted **in the same transaction as the image flip that orphaned it** and
drained with retries + backoff, exactly like the outbox pattern used for events.

``product_id`` is kept so the drain can re-verify the key isn't the product's live
image before deleting; ``UNIQUE (object_key)`` makes scheduling idempotent.

Revision ID: b7d41c9e2f83
Revises: c5e8a2b1f6d7
Create Date: 2026-08-19 11:10:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7d41c9e2f83"
down_revision: str | None = "c5e8a2b1f6d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "image_reclaim",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True),
        sa.Column("product_id", sa.Uuid(), nullable=False),
        sa.Column("object_key", sa.String(length=1024), nullable=False, unique=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(length=512), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="catalog",
    )
    # The drain claims by due time; partial-free plain index is enough (the table
    # is empty in steady state — rows are deleted the moment the objects are gone).
    op.create_index(
        "ix_image_reclaim_due",
        "image_reclaim",
        ["next_attempt_at"],
        schema="catalog",
    )


def downgrade() -> None:
    op.drop_index("ix_image_reclaim_due", table_name="image_reclaim", schema="catalog")
    op.drop_table("image_reclaim", schema="catalog")
