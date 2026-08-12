"""keyset pagination index for orders.orders

``list_orders`` is always scoped to the owning ``user_id`` and keyset-ordered by
``(created_at, id)``; ``ix_orders_user_id_status`` can't serve that ORDER BY.

Revision ID: d6f9b3c2a7e8
Revises: 3a6f18ca326a
Create Date: 2026-08-11 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "d6f9b3c2a7e8"
down_revision: str | None = "3a6f18ca326a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("ix_orders_user_id_created_at_id", "orders", ["user_id", "created_at", "id"], schema="orders")


def downgrade() -> None:
    op.drop_index("ix_orders_user_id_created_at_id", table_name="orders", schema="orders")
