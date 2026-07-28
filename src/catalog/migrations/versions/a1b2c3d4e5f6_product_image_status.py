"""product image pipeline columns

Adds ``catalog.products.image_status`` (none → pending → ready | failed) and
``image_upload_token`` (the currently-pending upload's token). The image worker
marks a product image ``ready`` only after it passes sniff + re-encode — so
``image_key`` is never merchant-supplied — and only when the processed event's
token still matches ``image_upload_token`` (a stale event for a superseded
upload is a no-op).

Revision ID: a1b2c3d4e5f6
Revises: 27123711f655
Create Date: 2026-07-28 10:55:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "27123711f655"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column("image_status", sa.String(length=16), server_default="none", nullable=False),
        schema="catalog",
    )
    op.add_column(
        "products",
        sa.Column("image_upload_token", sa.String(length=64), nullable=True),
        schema="catalog",
    )
    op.create_check_constraint(
        "ck_products_image_status",
        "products",
        "image_status IN ('none', 'pending', 'ready', 'failed')",
        schema="catalog",
    )


def downgrade() -> None:
    op.drop_constraint("ck_products_image_status", "products", schema="catalog", type_="check")
    op.drop_column("products", "image_upload_token", schema="catalog")
    op.drop_column("products", "image_status", schema="catalog")
