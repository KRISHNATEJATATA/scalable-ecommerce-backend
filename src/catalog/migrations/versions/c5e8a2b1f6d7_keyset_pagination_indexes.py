"""keyset pagination indexes for catalog.products

Every list page orders by ``(<sort col>, id)`` under ``deleted_at IS NULL``; without
a matching composite the planner sorts the whole live table and discards all but a
page. Partial on ``deleted_at IS NULL`` so soft-deleted rows don't bloat the index.

``ix_products_name`` is superseded by ``ix_products_name_id`` (same leading column,
also serves the ORDER BY tiebreaker), so it is dropped rather than kept alongside.

Revision ID: c5e8a2b1f6d7
Revises: a1b2c3d4e5f6
Create Date: 2026-08-11 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c5e8a2b1f6d7"
down_revision: str | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LIVE_ONLY = "deleted_at IS NULL"


def upgrade() -> None:
    for name, cols in (
        ("ix_products_created_at_id", ["created_at", "id"]),
        ("ix_products_price_id", ["price", "id"]),
        ("ix_products_name_id", ["name", "id"]),
    ):
        op.create_index(name, "products", cols, schema="catalog", postgresql_where=_LIVE_ONLY)
    op.drop_index("ix_products_name", table_name="products", schema="catalog")


def downgrade() -> None:
    op.create_index("ix_products_name", "products", ["name"], schema="catalog")
    for name in ("ix_products_name_id", "ix_products_price_id", "ix_products_created_at_id"):
        op.drop_index(name, table_name="products", schema="catalog")
