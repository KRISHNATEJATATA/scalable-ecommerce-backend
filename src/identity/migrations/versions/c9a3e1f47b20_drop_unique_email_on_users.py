"""drop UNIQUE(email) on the identity mirror

``UNIQUE(email)`` is a leftover from the pre-OIDC design where the app owned
credentials. Keycloak now owns identity, and it enforces email uniqueness only
among *current* accounts — a freed address is reusable. With the constraint in
place a recreated Keycloak account (new ``sub``, recycled email) either 500s on
JIT provisioning or has to be reconciled onto the existing row, which would hand
the new principal the previous holder's ``orders.user_id`` /
``products.merchant_id`` rows. Dropping it makes a recreated account a genuinely
new principal, which is the correct outcome.

The mirror keys on ``UNIQUE(oidc_sub)``; email is descriptive, so a plain
(non-unique) index is kept for admin lookups.

Revision ID: c9a3e1f47b20
Revises: b7c4a1f9d2e3
Create Date: 2026-08-12 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c9a3e1f47b20"
down_revision: str | None = "b7c4a1f9d2e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("ix_identity_users_email", table_name="users", schema="identity")
    op.create_index("ix_identity_users_email", "users", ["email"], unique=False, schema="identity")


def downgrade() -> None:
    # May fail if duplicate emails exist by now — that is the point of the drop.
    op.drop_index("ix_identity_users_email", table_name="users", schema="identity")
    op.create_index("ix_identity_users_email", "users", ["email"], unique=True, schema="identity")
