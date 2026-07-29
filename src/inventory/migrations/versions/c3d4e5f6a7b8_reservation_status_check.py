"""constrain reservation status to the three known states

``ck_reservations_status_valid`` closes the last hole in the reservation status
machine. Every transition is guarded on ``status = 'held'`` in SQL, so a row that
somehow reaches an unknown status is **unreapable**: the expiry sweep won't claim
it (not ``held``) and nothing else will either, so its stock never returns to the
pool. The DB rejects the write instead.

Revision ID: c3d4e5f6a7b8
Revises: b7c1d2e3f4a5
Create Date: 2026-07-29
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c3d4e5f6a7b8"
down_revision: str | None = "b7c1d2e3f4a5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_reservations_status_valid",
        "reservations",
        "status IN ('held', 'released', 'committed')",
        schema="inventory",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_reservations_status_valid",
        "reservations",
        schema="inventory",
        type_="check",
    )
