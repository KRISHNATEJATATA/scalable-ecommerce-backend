"""drop redundant oidc_sub index

``UNIQUE(oidc_sub)`` already provides the index every ``sub`` lookup uses, so the
separate ``ix_identity_users_oidc_sub`` was a second index maintained on every
write for nothing.

Revision ID: b7c4a1f9d2e3
Revises: ed22f9c3db1f
Create Date: 2026-08-11 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b7c4a1f9d2e3"
down_revision: str | None = "ed22f9c3db1f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("ix_identity_users_oidc_sub", table_name="users", schema="identity")


def downgrade() -> None:
    op.create_index("ix_identity_users_oidc_sub", "users", ["oidc_sub"], unique=False, schema="identity")
