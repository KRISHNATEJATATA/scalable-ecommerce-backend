"""Shared DB fixtures: one real Postgres for the whole unit-test session.

Never SQLite — the design leans on Postgres CHECK constraints, partial unique
indexes, ``FOR UPDATE SKIP LOCKED``, ``version_id`` locking and ``ON DELETE``.
The container is session-scoped (spinning one up per test module is the slow
part), and every test starts from a truncated schema. Requires Docker; no
environment-dependent skip.
"""

import asyncio
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

# Every table the tests can write, truncated (with CASCADE for the intra-module
# FKs) so no rows leak between tests — payments accumulate per order_id today,
# and outbox/image_reclaim rows must not survive a test that asserted on them.
_TRUNCATE = text(
    "TRUNCATE catalog.products, catalog.outbox, catalog.image_reclaim, "
    "orders.order_items, orders.orders, orders.outbox, "
    "inventory.reservations, inventory.inventory, inventory.outbox, "
    "identity.users, identity.outbox, "
    "payments.payments, payments.outbox CASCADE"
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


@pytest.fixture(scope="session")
def _valkey_server():
    """One real Valkey for the whole session (compose runs ``valkey/valkey:8``).

    Shared by every test that exercises Valkey-backed code through the real
    engine (cache adapter + cache-aside orchestration, bus dedupe). Tests flush
    before use, so sharing one container is safe — spinning one up per module was
    the slow part.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer("valkey/valkey:8").with_exposed_ports(6379)
    with container:
        yield container.get_container_host_ip(), int(container.get_exposed_port(6379))


@pytest.fixture
async def real_valkey(_valkey_server):
    """A ready-gated per-test client on the session Valkey, flushed before use.

    ``_valkey_server`` yields as soon as the container is *running*, which on a
    loaded CI runner can precede Valkey actually accepting connections: the
    docker-proxy accepts the TCP handshake, then can't reach the not-yet-listening
    engine and closes the socket, so the client's first write dies with
    ``Error UNKNOWN while writing to socket. Connection lost.`` — a CI-only flake
    (2026-09, test_cold_stampede_single_fills_through_the_real_valkey_adapter).
    Ping until the engine answers before any real command, and flush per test so
    sharing the session container stays safe.
    """
    from valkey.asyncio import Valkey

    host, port = _valkey_server
    client = Valkey(host=host, port=port)
    for _ in range(100):  # ~10s ceiling: wait for "Ready to accept connections"
        try:
            if await client.ping():
                break
        except Exception:
            await asyncio.sleep(0.1)
    else:
        pytest.fail("Valkey container never became ready")
    await client.flushdb()
    yield client
    await client.aclose()


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
