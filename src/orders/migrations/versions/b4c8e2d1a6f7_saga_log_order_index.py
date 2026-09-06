"""saga_log order_id index for the recovery heartbeat

The recovery claim's liveness heartbeat (``NOT EXISTS (SELECT 1 FROM
orders.saga_log WHERE order_id = o.id AND updated_at >= :cutoff)``) and the
cancel endpoint's in-flight-charge guard both read the journal by ``order_id``.
Postgres does not index foreign-key columns automatically, so without this each
is a full scan of the log per candidate order.

Revision ID: b4c8e2d1a6f7
Revises: e7a1c4f9b2d3
Create Date: 2026-09-06 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b4c8e2d1a6f7"
down_revision: str | None = "e7a1c4f9b2d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("ix_saga_log_order_id", "saga_log", ["order_id"], schema="orders")


def downgrade() -> None:
    op.drop_index("ix_saga_log_order_id", table_name="saga_log", schema="orders")
