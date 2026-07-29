"""reservation idempotency + expiry sweep index

Adds the guards to ``inventory.reservations``:

* ``uq_reservations_active_order_sku`` — a retried saga step can't place a second
  hold on the same order line (the whole reserve txn rolls back, undoing its CAS
  decrement too). Partial (``WHERE status = 'held'``) so a released hold doesn't
  block a legitimate re-reservation of the same line.
* ``ix_reservations_expiry_sweep`` — partial index on ``expires_at WHERE status
  = 'held'`` so the reaper's claim query stays cheap as released/committed rows
  accumulate.

Revision ID: b7c1d2e3f4a5
Revises: 8810866cbee0
Create Date: 2026-07-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b7c1d2e3f4a5"
down_revision: str | None = "8810866cbee0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "uq_reservations_active_order_sku",
        "reservations",
        ["order_id", "sku"],
        unique=True,
        schema="inventory",
        postgresql_where=sa.text("status = 'held'"),
    )
    op.create_index(
        "ix_reservations_expiry_sweep",
        "reservations",
        ["expires_at"],
        unique=False,
        schema="inventory",
        postgresql_where=sa.text("status = 'held'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_reservations_expiry_sweep",
        table_name="reservations",
        schema="inventory",
        postgresql_where=sa.text("status = 'held'"),
    )
    op.drop_index(
        "uq_reservations_active_order_sku",
        table_name="reservations",
        schema="inventory",
        postgresql_where=sa.text("status = 'held'"),
    )
