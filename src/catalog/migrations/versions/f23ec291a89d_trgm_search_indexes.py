"""trigram GIN indexes for the catalog substring search

``GET /v1/products?search=`` filters with ``name ILIKE '%term%' OR description
ILIKE '%term%'`` (an exact substring *filter*, not relevance ranking).
A leading-wildcard ILIKE cannot use a B-tree, so every searched listing scanned
the live table. ``pg_trgm``'s GIN opclass indexes all 3-char substrings, which
accelerates exactly this predicate shape **without changing semantics** — the
filter stays literal/exact, the keyset ``(sort, id)`` order is untouched, and
clients see no contract change.

Both indexes are partial on ``deleted_at IS NULL``: the query always carries that
predicate (soft-delete filter), and dead rows would only bloat the index.

Known ceiling: terms shorter than 3 characters extract no trigram, so the planner
falls back to a scan for them — correctness is unchanged (the GIN index is only
ever an access-path choice), and the 200-char cap already bounds the term.

The extension is created idempotently; downgrade drops the indexes but leaves
``pg_trgm`` installed — dropping an extension is a fleet-wide decision (another
schema/module may come to depend on it), not this chain's to reverse.

Revision ID: f23ec291a89d
Revises: e2a7f1c48d90
Create Date: 2026-09-29 12:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f23ec291a89d"
down_revision: str | None = "e2a7f1c48d90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LIVE_ONLY = "deleted_at IS NULL"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.create_index(
        "ix_products_name_trgm",
        "products",
        ["name"],
        schema="catalog",
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
        postgresql_where=sa.text(_LIVE_ONLY),
    )
    op.create_index(
        "ix_products_description_trgm",
        "products",
        ["description"],
        schema="catalog",
        postgresql_using="gin",
        postgresql_ops={"description": "gin_trgm_ops"},
        postgresql_where=sa.text(_LIVE_ONLY),
    )


def downgrade() -> None:
    op.drop_index("ix_products_description_trgm", table_name="products", schema="catalog")
    op.drop_index("ix_products_name_trgm", table_name="products", schema="catalog")
