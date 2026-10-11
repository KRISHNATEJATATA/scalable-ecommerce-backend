"""order_recipients projection (OrderPlaced → order_id → email)

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-10-11 10:00:00.000000

The refund notice's address source: ``PaymentRefunded`` carries no recipient,
so the worker materializes the checkout-time address from the ``OrderPlaced``
events it already consumes (upserted before any send decision, so a suppressed
or unsent confirmation still records the address). No backfill: orders that
predate this table simply redrive their refund event if one arrives before any
re-played ``OrderPlaced`` writes the row — and an unresolvable refund raises
for redrive → DLQ rather than silently skipping.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: str | None = "b2c3d4e5f6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "order_recipients",
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("order_id"),
        schema="notifications",
    )


def downgrade() -> None:
    op.drop_table("order_recipients", schema="notifications")
