"""composite idempotency unique + body hash for orders.orders

The global ``UNIQUE(idempotency_key)`` let one user's key block every other
user's identical key. The guard must be ``UNIQUE(user_id, idempotency_key)``,
with ``idempotency_body_hash`` so the DB backstop answers "same key, different
body → 409" even after the Valkey fast-path record was evicted.

Revision ID: e7a1c4f9b2d3
Revises: d6f9b3c2a7e8
Create Date: 2026-09-06 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e7a1c4f9b2d3"
down_revision: str | None = "d6f9b3c2a7e8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column("idempotency_body_hash", sa.String(length=64), nullable=False, server_default=""),
        schema="orders",
    )
    op.drop_constraint("uq_orders_idempotency_key", "orders", schema="orders", type_="unique")
    op.create_unique_constraint(
        "uq_orders_user_id_idempotency_key", "orders", ["user_id", "idempotency_key"], schema="orders"
    )


def downgrade() -> None:
    op.drop_constraint("uq_orders_user_id_idempotency_key", "orders", schema="orders", type_="unique")
    op.create_unique_constraint("uq_orders_idempotency_key", "orders", ["idempotency_key"], schema="orders")
    op.drop_column("orders", "idempotency_body_hash", schema="orders")
