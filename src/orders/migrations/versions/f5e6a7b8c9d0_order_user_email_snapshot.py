"""orders.orders user_email snapshot column

The buyer's email is snapshotted on the order at checkout (from the
authenticated caller's token claim) so ``OrderPlaced`` can carry it: the
notification send path then resolves the recipient from the event itself
instead of depending on the ``UserCreated`` event having landed first
(cross-topic/cross-subscription ordering is not guaranteed — the miss used to
redrive → DLQ a healthy order's confirmation). ``''`` marks rows that predate
the snapshot; the outbox builder normalizes it to ``None`` and consumers fall
back to the recipients table.

Revision ID: f5e6a7b8c9d0
Revises: c7f2a9d4e6b1
Create Date: 2026-09-29 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f5e6a7b8c9d0"
down_revision: str | None = "c7f2a9d4e6b1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column("user_email", sa.String(length=320), nullable=False, server_default=""),
        schema="orders",
    )


def downgrade() -> None:
    op.drop_column("orders", "user_email", schema="orders")
