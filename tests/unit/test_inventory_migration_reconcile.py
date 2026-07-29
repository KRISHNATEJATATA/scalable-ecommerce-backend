"""The pre-flight reconciliation in migration ``d4e5f6a7b8c9``.

Widening ``uq_reservations_active_order_sku`` from ``held`` to ``held ∪
committed`` forbids duplicates the *old* index deliberately allowed, so existing
production rows can already violate it. Creating the index blindly fails the
deploy. These checks cover the reconciliation that runs first — against a real
Postgres, since the whole thing is SQL.
"""

import contextlib
import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "inventory"
    / "migrations"
    / "versions"
    / "d4e5f6a7b8c9_widen_reservation_uniqueness.py"
)


def _load_migration():
    """Import the migration by path — `versions/` isn't an importable package."""
    spec = importlib.util.spec_from_file_location("_widen_migration", _MIGRATION_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_MIGRATION = _load_migration()
_INDEX = _MIGRATION._INDEX


async def _seed_stock(session, sku: str, on_hand: int, reserved: int) -> None:
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, :o, :r, 1)"),
        {"sku": sku, "o": on_hand, "r": reserved},
    )


async def _insert_reservation(session, *, sku: str, order_id, qty: int, status: str) -> uuid.UUID:
    reservation_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO inventory.reservations (id, sku, qty, order_id, status, expires_at) "
            "VALUES (:id, :sku, :qty, :order_id, :status, :expires_at)"
        ),
        {
            "id": reservation_id,
            "sku": sku,
            "qty": qty,
            "order_id": order_id,
            "status": status,
            "expires_at": datetime.now(UTC) + timedelta(minutes=5),
        },
    )
    return reservation_id


async def _status(session, reservation_id: uuid.UUID) -> str:
    return (
        await session.execute(text("SELECT status FROM inventory.reservations WHERE id = :id"), {"id": reservation_id})
    ).scalar_one()


@contextlib.asynccontextmanager
async def _pre_migration_state(engine):
    """Roll the schema back to *before* the widening, so the duplicates it exists
    to reconcile can be seeded at all.

    The migrated test database already carries the widened index, which forbids
    exactly the rows under test. Dropping it reproduces the deployment's starting
    point; the index is always restored afterwards so later tests see a migrated
    schema. Restoring it is also a real assertion in the happy path — it only
    succeeds if reconciliation genuinely made the data legal.
    """
    async with engine.begin() as conn:
        await conn.execute(text(f"DROP INDEX inventory.{_INDEX}"))
    try:
        yield
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM inventory.reservations"))
            await conn.execute(
                text(
                    f"CREATE UNIQUE INDEX {_INDEX} ON inventory.reservations (order_id, sku) "
                    "WHERE status IN ('held', 'committed')"
                )
            )


async def test_a_held_row_shadowed_by_a_committed_one_is_released_with_its_stock(async_engine):
    """The reconcilable duplicate: keep the committed row, release the stray hold.

    Releasing the status alone would leave `reserved` permanently inflated, so the
    stock give-back has to happen in the same pass.
    """

    order_id = uuid.uuid4()
    async with _pre_migration_state(async_engine), async_engine.begin() as conn:
        await _seed_stock(conn, "sku-dupe", on_hand=10, reserved=2)
        committed = await _insert_reservation(conn, sku="sku-dupe", order_id=order_id, qty=2, status="committed")
        stray = await _insert_reservation(conn, sku="sku-dupe", order_id=order_id, qty=2, status="held")

        await conn.run_sync(lambda sync_conn: _MIGRATION._reconcile_existing_duplicates(sync_conn))

        assert await _status(conn, stray) == "released"
        assert await _status(conn, committed) == "committed"  # authoritative, untouched
        reserved = (
            await conn.execute(text("SELECT reserved FROM inventory.inventory WHERE sku = 'sku-dupe'"))
        ).scalar_one()
        assert reserved == 0  # the stray's 2 units handed back


async def test_reconciliation_leaves_legitimate_rows_alone(async_engine):
    """A held row with no committed twin is a live hold — it must survive untouched."""

    async with async_engine.begin() as conn:
        await _seed_stock(conn, "sku-fine", on_hand=10, reserved=3)
        live = await _insert_reservation(conn, sku="sku-fine", order_id=uuid.uuid4(), qty=3, status="held")

        await conn.run_sync(lambda sync_conn: _MIGRATION._reconcile_existing_duplicates(sync_conn))

        assert await _status(conn, live) == "held"
        reserved = (
            await conn.execute(text("SELECT reserved FROM inventory.inventory WHERE sku = 'sku-fine'"))
        ).scalar_one()
        assert reserved == 3


async def test_reconciliation_aborts_when_the_stock_give_back_matches_no_row(async_engine):
    """`reserved` too low to return the stray hold's units: abort, don't flip the status.

    Marking it released anyway would commit the flip while the stock never moved —
    those units are then unreachable forever. Same reasoning as `_require_one` in
    the repository, in the one place a migration can silently lose them.
    """

    order_id = uuid.uuid4()
    async with _pre_migration_state(async_engine), async_engine.begin() as conn:
        # reserved=0 while a held row claims 2 units: the counters already disagree.
        await _seed_stock(conn, "sku-skew", on_hand=10, reserved=0)
        await _insert_reservation(conn, sku="sku-skew", order_id=order_id, qty=2, status="committed")
        stray = await _insert_reservation(conn, sku="sku-skew", order_id=order_id, qty=2, status="held")

        with pytest.raises(RuntimeError, match="expected 1"):
            await conn.run_sync(lambda sync_conn: _MIGRATION._reconcile_existing_duplicates(sync_conn))
        assert await _status(conn, stray) == "held"


async def test_multiple_committed_rows_abort_the_migration_with_the_offending_lines(async_engine):
    """Stock already deducted twice: no safe automatic un-deduct, so fail loudly.

    Silently creating the index would fail anyway with an opaque constraint error;
    this fails first with the actual order lines to investigate.
    """

    order_id = uuid.uuid4()
    async with _pre_migration_state(async_engine), async_engine.begin() as conn:
        await _seed_stock(conn, "sku-doubled", on_hand=10, reserved=0)
        await _insert_reservation(conn, sku="sku-doubled", order_id=order_id, qty=1, status="committed")
        await _insert_reservation(conn, sku="sku-doubled", order_id=order_id, qty=1, status="committed")

        with pytest.raises(RuntimeError, match="sku-doubled"):
            await conn.run_sync(lambda sync_conn: _MIGRATION._reconcile_existing_duplicates(sync_conn))
