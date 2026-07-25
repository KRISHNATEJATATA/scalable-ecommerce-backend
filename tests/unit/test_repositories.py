"""Repository-layer tests (ticket 02) against Testcontainers-Postgres.

The repository is the highest seam that exists this phase — api/ has no routes
yet — so keyset correctness, whitelist/cursor rejection, soft-delete, the N+1
guard, and the atomic decrement are all asserted here. HTTP-seam + concurrency
races land with tickets 07/10/13/17.

Spins up a real Postgres container for the module (never SQLite — the design
relies on Postgres CHECK constraints, ``version_id`` locking, and ``ON
DELETE``) and runs every module's Alembic chain against it. Requires Docker;
no environment-dependent skip.
"""

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from src.catalog.adapters.db.repository import CatalogRepository
from src.inventory.adapters.db.repository import InventoryRepository
from src.orders.adapters.db.repository import OrdersRepository
from src.shared.config.setting import get_settings
from src.shared.db.pagination import PageParams
from src.shared.errors.exceptions import InvalidCursorError, InvalidQueryParamError

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog", "inventory", "orders", "payments"]

_TRUNCATE = text(
    "TRUNCATE catalog.products, orders.order_items, orders.orders, inventory.reservations, inventory.inventory CASCADE"
)


@pytest.fixture(scope="module")
def _migrated():
    with PostgresContainer("postgres:16-alpine") as pg:
        async_url = pg.get_connection_url(driver="asyncpg")
        old_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = async_url
        get_settings.cache_clear()
        try:
            for module in MODULES:
                subprocess.run(
                    [sys.executable, "-m", "alembic", "-c", f"src/{module}/alembic.ini", "upgrade", "head"],
                    cwd=REPO_ROOT,
                    check=True,
                )
            yield
        finally:
            if old_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_url
            get_settings.cache_clear()


@pytest.fixture
async def async_engine(_migrated):
    engine = create_async_engine(str(get_settings().database_url))
    async with engine.begin() as conn:
        await conn.execute(_TRUNCATE)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session(async_engine):
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with maker() as sess:
        yield sess


# --- seed helpers ---------------------------------------------------------


async def _insert_product(session, *, name, price, created_at, category=None, merchant_id=None, deleted_at=None):
    pid = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO catalog.products "
            "(id, merchant_id, name, category, price, created_at, updated_at, deleted_at, version_id) "
            "VALUES (:id, :merchant_id, :name, :category, :price, :created_at, :created_at, :deleted_at, 1)"
        ),
        {
            "id": pid,
            "merchant_id": merchant_id or uuid.uuid4(),
            "name": name,
            "category": category,
            "price": price,
            "created_at": created_at,
            "deleted_at": deleted_at,
        },
    )
    return pid


async def _insert_order(session, *, user_id, created_at, item_count):
    oid = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO orders.orders (id, user_id, idempotency_key, status, total, created_at, updated_at) "
            "VALUES (:id, :user_id, :key, 'pending', :total, :created_at, :created_at)"
        ),
        {"id": oid, "user_id": user_id, "key": str(uuid.uuid4()), "total": Decimal("10.00"), "created_at": created_at},
    )
    for i in range(item_count):
        await session.execute(
            text(
                "INSERT INTO orders.order_items "
                "(id, order_id, product_id, product_name, unit_price, quantity) "
                "VALUES (gen_random_uuid(), :order_id, gen_random_uuid(), :name, :price, 1)"
            ),
            {"order_id": oid, "name": f"line-{i}", "price": Decimal("5.00")},
        )
    return oid


async def _walk(list_fn):
    """Page through every cursor and return the flat list of items seen."""
    seen = []
    cursor = None
    while True:
        page = await list_fn(cursor)
        seen.extend(page.items)
        if page.next_cursor is None:
            return seen
        cursor = page.next_cursor


# --- 1. keyset correctness ------------------------------------------------


async def test_catalog_keyset_full_walk_no_dup_or_skip_with_midwalk_insert(session):
    repo = CatalogRepository(session)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # 5 products, distinct timestamps (newest-first walk).
    originals = []
    for i in range(5):
        pid = await _insert_product(session, name=f"p{i}", price=Decimal("9.99"), created_at=base - timedelta(days=i))
        originals.append(pid)
    await session.commit()

    seen = []
    cursor = None
    inserted_midwalk = False
    while True:
        page = await repo.list_products(PageParams(limit=2, sort="-created_at", cursor=cursor))
        seen.extend(p.id for p in page.items)
        if not inserted_midwalk:
            # Concurrent insert BELOW the current cursor position — keyset must
            # not skip or duplicate any original as a result.
            await _insert_product(
                session, name="mid", price=Decimal("1.00"), created_at=base - timedelta(days=2, hours=12)
            )
            await session.commit()
            inserted_midwalk = True
        if page.next_cursor is None:
            break
        cursor = page.next_cursor

    for pid in originals:
        assert seen.count(pid) == 1, f"{pid} skipped or duplicated"
    # newest-first ordering held for the originals subset
    original_order = [pid for pid in seen if pid in originals]
    assert original_order == originals


async def test_orders_keyset_terminates(session):
    repo = OrdersRepository(session)
    user_id = uuid.uuid4()
    base = datetime(2026, 2, 1, tzinfo=UTC)
    for i in range(3):
        await _insert_order(session, user_id=user_id, created_at=base - timedelta(days=i), item_count=1)
    await session.commit()

    seen = await _walk(lambda c: repo.list_orders(user_id, PageParams(limit=1, cursor=c)))
    assert len(seen) == 3
    assert len({o.id for o in seen}) == 3


async def test_orders_scoped_to_user(session):
    repo = OrdersRepository(session)
    mine, theirs = uuid.uuid4(), uuid.uuid4()
    await _insert_order(session, user_id=mine, created_at=datetime(2026, 3, 1, tzinfo=UTC), item_count=1)
    await _insert_order(session, user_id=theirs, created_at=datetime(2026, 3, 2, tzinfo=UTC), item_count=1)
    await session.commit()

    page = await repo.list_orders(mine, PageParams(limit=10))
    assert {o.user_id for o in page.items} == {mine}


# --- 2. whitelist / cursor rejection --------------------------------------


async def test_unknown_sort_field_rejected(session):
    with pytest.raises(InvalidQueryParamError):
        await CatalogRepository(session).list_products(PageParams(sort="secret_column"))
    with pytest.raises(InvalidQueryParamError):
        await OrdersRepository(session).list_orders(uuid.uuid4(), PageParams(sort="total"))


async def test_unknown_filter_field_rejected(session):
    with pytest.raises(InvalidQueryParamError):
        await CatalogRepository(session).list_products(PageParams(), filters={"price": 5})


async def test_undecodable_cursor_rejected(session):
    with pytest.raises(InvalidCursorError):
        await CatalogRepository(session).list_products(PageParams(cursor="!!!not-a-cursor!!!"))


# --- 3. soft-delete -------------------------------------------------------


async def test_soft_deleted_products_excluded(session):
    repo = CatalogRepository(session)
    live = await _insert_product(
        session, name="live", price=Decimal("5.00"), created_at=datetime(2026, 4, 1, tzinfo=UTC)
    )
    dead = await _insert_product(
        session,
        name="dead",
        price=Decimal("5.00"),
        created_at=datetime(2026, 4, 2, tzinfo=UTC),
        deleted_at=datetime(2026, 4, 3, tzinfo=UTC),
    )
    await session.commit()

    page = await repo.list_products(PageParams(limit=10))
    ids = {p.id for p in page.items}
    assert live in ids and dead not in ids
    assert await repo.get_product(dead) is None
    assert (await repo.get_product(live)).id == live


# --- 4. N+1 guard ---------------------------------------------------------


async def test_orders_list_is_constant_query_regardless_of_page_size(session, async_engine):
    repo = OrdersRepository(session)
    user_id = uuid.uuid4()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    for i in range(5):
        await _insert_order(session, user_id=user_id, created_at=base - timedelta(days=i), item_count=3)
    await session.commit()

    counter = {"n": 0}

    @event.listens_for(async_engine.sync_engine, "before_cursor_execute")
    def _count(*_args):  # noqa: ANN001
        counter["n"] += 1

    try:
        page = await repo.list_orders(user_id, PageParams(limit=10))
    finally:
        event.remove(async_engine.sync_engine, "before_cursor_execute", _count)

    assert len(page.items) == 5
    # 1 query for the orders page + 1 selectinload for all items = 2, not 1 + N.
    assert counter["n"] == 2
    # items are eager-loaded — accessing them under lazy="raise" must not blow up.
    assert all(len(o.items) == 3 for o in page.items)


# --- 5. atomic decrement --------------------------------------------------


async def test_decrement_rejects_when_insufficient_free_stock(session):
    repo = InventoryRepository(session)
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES ('sku-1', 5, 0, 1)")
    )
    await session.commit()

    assert await repo.try_reserve_decrement("sku-1", 3) == 1  # 5-0 >= 3 → reserved
    assert await repo.try_reserve_decrement("sku-1", 3) == 0  # 5-3 < 3 → rejected
    row = await repo.get_by_sku("sku-1")
    assert row.reserved == 3 and row.version == 2
