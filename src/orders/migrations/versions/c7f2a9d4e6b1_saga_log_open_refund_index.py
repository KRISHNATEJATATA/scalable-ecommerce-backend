"""saga_log open-refund-marker partial index for the refund-retry claim

The recovery poller's refund claim finds candidate orders from the
journal's open refund markers (``step = 'refund' AND status = 'requested'``)
rather than scanning the ever-growing set of cancelled orders. The journal is
insert-only and markers are few (one per refund-of-a-dead-order), so a partial
index keeps the per-pass lookup at the size of the refund history, not the
order history.

Revision ID: c7f2a9d4e6b1
Revises: b4c8e2d1a6f7
Create Date: 2026-09-28 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c7f2a9d4e6b1"
down_revision: str | None = "b4c8e2d1a6f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_saga_log_open_refund",
        "saga_log",
        ["created_at"],
        schema="orders",
        postgresql_where=sa.text("step = 'refund' AND status = 'requested'"),
    )


def downgrade() -> None:
    op.drop_index("ix_saga_log_open_refund", table_name="saga_log", schema="orders")
