"""abandoned-payment failure reason

Adds ``payments.payments.failure_reason``: the provider-supplied decline reason
recorded when a payment transitions to ``failed`` (webhook or reconciliation).
Nullable — a succeeded (or still-pending) payment has none.

Revision ID: f4c8a1d9b2e7
Revises: e7a1c4d5b9f2
Create Date: 2026-08-25 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f4c8a1d9b2e7"
down_revision: str | None = "e7a1c4d5b9f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("failure_reason", sa.String(length=512), nullable=True),
        schema="payments",
    )


def downgrade() -> None:
    op.drop_column("payments", "failure_reason", schema="payments")
