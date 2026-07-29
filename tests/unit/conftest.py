"""Shared DB fixtures: one real Postgres for the whole unit-test session.

Never SQLite — the design leans on Postgres CHECK constraints, partial unique
indexes, ``FOR UPDATE SKIP LOCKED``, ``version_id`` locking and ``ON DELETE``.
The container is session-scoped (spinning one up per test module is the slow
part), and every test starts from a truncated schema. Requires Docker; no
environment-dependent skip.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from src.shared.config.setting import get_settings

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog", "inventory", "orders", "payments"]

_TRUNCATE = text(
    "TRUNCATE catalog.products, orders.order_items, orders.orders, "
    "inventory.reservations, inventory.inventory, inventory.outbox CASCADE"
)


@pytest.fixture(scope="session")
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


@pytest.fixture
def sessionmaker_factory(async_engine):
    """A sessionmaker on the test engine — for code that owns its own session
    (the reaper), which can't reuse the request-scoped ``session`` fixture."""
    return async_sessionmaker(async_engine, expire_on_commit=False)
