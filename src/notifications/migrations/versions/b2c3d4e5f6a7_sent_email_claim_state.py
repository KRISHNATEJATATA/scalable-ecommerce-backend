"""sent_emails claim state (status + persisted message_id)

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-09-29 09:30:00.000000

The send state is claimed (``pending``) BEFORE the send and marked
``sent`` after, so every send has a durable record and the crash window is
reconcilable. Existing rows all completed their send ⇒ backfilled ``sent``;
their ``message_id`` stays NULL (it was never recorded).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: str | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sent_emails",
        sa.Column("status", sa.String(length=16), server_default="sent", nullable=False),
        schema="notifications",
    )
    op.add_column("sent_emails", sa.Column("message_id", sa.UUID(), nullable=True), schema="notifications")
    op.create_check_constraint(
        "ck_sent_emails_status", "sent_emails", "status IN ('pending', 'sent')", schema="notifications"
    )
    # The server_default STAYS: during a rolling deploy an old-code worker still
    # inserts status-less rows (post-send), and 'sent' is the truthful value for
    # those. Drop it in a follow-up migration once every worker runs the claim flow.


def downgrade() -> None:
    op.drop_constraint("ck_sent_emails_status", "sent_emails", schema="notifications")
    op.drop_column("sent_emails", "message_id", schema="notifications")
    op.drop_column("sent_emails", "status", schema="notifications")
