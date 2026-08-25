"""abandoned-presign expiry for product images

Adds ``catalog.products.image_upload_expires_at``: when the currently-pending
presigned POST stops being accepted by S3.

Presigning flips the product to ``pending`` immediately, which drops ``image_url``
from every response. If the client never uploads (closed tab, crash, network),
nothing ever moved the row out of ``pending``, so the product — **and any image it
was already serving** — stayed unavailable forever. Recording the deadline lets the
image worker's reaper restore the previous state (``ready`` when a processed
``image_key`` survives, else ``none``) once the presign is safely dead.

Backfill: existing ``pending`` rows get a deadline in the past, so the first sweep
after deploy clears out anything already stranded.

Revision ID: e2a7f1c48d90
Revises: b7d41c9e2f83
Create Date: 2026-08-19 12:05:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e2a7f1c48d90"
down_revision: str | None = "b7d41c9e2f83"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column("image_upload_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema="catalog",
    )
    op.execute(
        "UPDATE catalog.products SET image_upload_expires_at = now() "
        "WHERE image_status = 'pending' AND image_upload_expires_at IS NULL"
    )
    # The reaper scans for due pending uploads only; a partial index keeps that scan
    # off the (vastly larger) set of products that aren't mid-upload.
    op.create_index(
        "ix_products_pending_upload_expiry",
        "products",
        ["image_upload_expires_at"],
        schema="catalog",
        postgresql_where=sa.text("image_status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_products_pending_upload_expiry", table_name="products", schema="catalog")
    op.drop_column("products", "image_upload_expires_at", schema="catalog")
