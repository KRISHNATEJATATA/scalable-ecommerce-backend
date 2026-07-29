"""widen the reservation idempotency guard to committed lines

``uq_reservations_active_order_sku`` covered only ``status = 'held'``, so once a
line was committed (payment succeeded) it no longer participated in the guard. A
delayed saga retry for that line therefore inserted a *second* reservation and
decremented the stock again — the same units deducted twice.

The guard now spans every status that still owns stock, ``held`` and
``committed``. ``released`` stays excluded so a compensated or reaped line can
legitimately be reserved again.

**Existing rows can already violate the widened guard** — that is the whole point:
the old index permitted exactly the duplicates this one forbids. Creating the
index blindly would fail the deployment, so we reconcile first:

* ``held`` + ``committed`` for the same line. The committed row is authoritative
  (payment consumed those units); the held row is the stray. It is released
  *including its stock*, exactly as the reaper would — status flip plus
  ``reserved -= qty`` in the same transaction, so the counters can't drift.
* two or more ``committed`` rows for one line. This is the double-deduction bug
  already materialised in the data: stock was deducted twice and there is no safe
  automatic un-deduct. The migration **aborts** with the offending lines listed —
  a human decides whether to refund or restock.

No outbox rows are written for the reconciling releases: re-announcing long-dead
holds as ``StockReleased`` to live consumers would be a worse lie than silence.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-07-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "uq_reservations_active_order_sku"
_HELD_ONLY = sa.text("status = 'held'")
_HELD_OR_COMMITTED = sa.text("status IN ('held', 'committed')")

# Lines that already carry >1 committed reservation: unfixable here, so abort.
_UNRECONCILABLE_SQL = sa.text(
    "SELECT order_id, sku, count(*) AS n FROM inventory.reservations "
    "WHERE status = 'committed' GROUP BY order_id, sku HAVING count(*) > 1"
)

# Held rows shadowed by a committed row for the same line — the reconcilable case.
_STRAY_HELD_SQL = sa.text(
    "SELECT r.id, r.sku, r.qty FROM inventory.reservations r "
    "WHERE r.status = 'held' AND EXISTS ("
    "  SELECT 1 FROM inventory.reservations c "
    "  WHERE c.order_id = r.order_id AND c.sku = r.sku AND c.status = 'committed')"
)

_GIVE_STOCK_BACK_SQL = sa.text(
    "UPDATE inventory.inventory SET reserved = reserved - :qty, version = version + 1 "
    "WHERE sku = :sku AND reserved >= :qty"
)

_MARK_RELEASED_SQL = sa.text("UPDATE inventory.reservations SET status = 'released', updated_at = now() WHERE id = :id")


def _reconcile_existing_duplicates(bind=None) -> None:
    """Make the data satisfy the widened guard, or abort with an actionable message.

    ``bind`` defaults to the migration's connection; it is a parameter so the
    reconciliation can be exercised against a real database in tests without
    standing up an Alembic context.
    """
    bind = bind if bind is not None else op.get_bind()

    blocked = bind.execute(_UNRECONCILABLE_SQL).all()
    if blocked:
        lines = ", ".join(f"(order_id={row.order_id}, sku={row.sku!r}, committed={row.n})" for row in blocked)
        raise RuntimeError(
            f"Cannot widen {_INDEX}: these order lines already hold multiple committed "
            f"reservations, so their stock was deducted more than once: {lines}. "
            "Resolve them (refund or restock, then release the surplus rows) and re-run."
        )

    for row in bind.execute(_STRAY_HELD_SQL).all():
        # The same two writes the reaper performs: stock back, then status flip.
        # The give-back is a guarded UPDATE (`reserved >= :qty`); 0 rows means the
        # counters contradict the hold, and flipping the status anyway would strand
        # those units forever. Abort — same reasoning as the repository's
        # `_require_one`, and the whole migration rolls back.
        result = bind.execute(_GIVE_STOCK_BACK_SQL, {"sku": row.sku, "qty": row.qty})
        if result.rowcount != 1:
            raise RuntimeError(
                f"Cannot widen {_INDEX}: releasing stray held reservation {row.id} "
                f"(sku={row.sku!r}, qty={row.qty}) updated {result.rowcount} stock rows, expected 1. "
                "inventory.reserved is inconsistent with the reservations table; fix the counters and re-run."
            )
        bind.execute(_MARK_RELEASED_SQL, {"id": row.id})


def _recreate(where: sa.TextClause, dropped_where: sa.TextClause) -> None:
    op.drop_index(_INDEX, table_name="reservations", schema="inventory", postgresql_where=dropped_where)
    op.create_index(
        _INDEX,
        "reservations",
        ["order_id", "sku"],
        unique=True,
        schema="inventory",
        postgresql_where=where,
    )


def upgrade() -> None:
    _reconcile_existing_duplicates()
    _recreate(_HELD_OR_COMMITTED, _HELD_ONLY)


def downgrade() -> None:
    # Narrowing can't fail: unique over (held, committed) implies unique over held alone.
    _recreate(_HELD_ONLY, _HELD_OR_COMMITTED)
