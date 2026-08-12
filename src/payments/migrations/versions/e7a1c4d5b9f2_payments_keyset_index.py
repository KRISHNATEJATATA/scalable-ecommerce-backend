"""keyset pagination index for payments.payments

``list_by_order_id`` looks up by ``order_id`` and keyset-orders by
``(created_at, id)``; one composite serves both.

Revision ID: e7a1c4d5b9f2
Revises: 8baa6009b4c4
Create Date: 2026-08-11 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e7a1c4d5b9f2"
down_revision: str | None = "8baa6009b4c4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_payments_order_id_created_at_id", "payments", ["order_id", "created_at", "id"], schema="payments"
    )


def downgrade() -> None:
    op.drop_index("ix_payments_order_id_created_at_id", table_name="payments", schema="payments")
