"""Outbox relay tests against Testcontainers-Postgres.

The relay's guarantees are Postgres behaviours — ``FOR UPDATE SKIP LOCKED``, the
partial index, and one-transaction publish-then-mark — so they are verified
against real Postgres (never SQLite), mirroring ``test_repositories``. The SNS
edge is a recording fake; SNS delivery itself is verified locally on LocalStack.

Covers the ticket's acceptance criteria:
- state write + outbox row → relay ships it and marks it published;
- a crash between publish and mark leaves rows unpublished → next pass re-ships
  (effectively-once downstream);
- two racing relays never double-claim a row (``SKIP LOCKED``).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from src.shared.bus.relay import OutboxRelay
from src.shared.config.setting import get_settings

REPO_ROOT = Path(__file__).resolve().parents[3]


class RecordingPublisher:
    """Records publishes; optionally raises once to simulate a mid-batch crash."""

    def __init__(self, *, fail_after: int | None = None) -> None:
        self.published: list[tuple[str, str]] = []
        self._fail_after = fail_after

    async def publish(self, event_type: str, payload: str) -> None:
        if self._fail_after is not None and len(self.published) >= self._fail_after:
            raise RuntimeError("SNS unavailable")
        self.published.append((event_type, payload))


@pytest.fixture(scope="module")
def _migrated():
    with PostgresContainer("postgres:16-alpine") as pg:
        async_url = pg.get_connection_url(driver="asyncpg")
        old_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = async_url
        get_settings.cache_clear()
        try:
            subprocess.run(
                [sys.executable, "-m", "alembic", "-c", "src/orders/alembic.ini", "upgrade", "head"],
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
async def sessionmaker(_migrated):
    engine = create_async_engine(str(get_settings().database_url))
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE orders.outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def _seed(maker, count: int) -> list[uuid.UUID]:
    ids = []
    async with maker() as session:
        for _ in range(count):
            eid = uuid.uuid4()
            ids.append(eid)
            payload = json.dumps({"type": "OrderPlaced", "event_id": str(eid), "trace_id": uuid.uuid4().hex})
            await session.execute(
                text("INSERT INTO orders.outbox (id, event_type, payload) VALUES (:id, 'OrderPlaced', :p)"),
                {"id": eid, "p": payload},
            )
        await session.commit()
    return ids


async def _unpublished_count(maker) -> int:
    async with maker() as session:
        return (
            await session.execute(text("SELECT count(*) FROM orders.outbox WHERE published_at IS NULL"))
        ).scalar_one()


@pytest.mark.asyncio
async def test_relay_publishes_and_marks_rows(sessionmaker) -> None:
    await _seed(sessionmaker, 3)
    publisher = RecordingPublisher()
    relay = OutboxRelay(sessionmaker, publisher, batch_size=100, schemas=("orders",))

    published = await relay.drain_once()

    assert published == 3
    assert len(publisher.published) == 3
    assert await _unpublished_count(sessionmaker) == 0
    # a second pass has nothing to ship
    assert await relay.drain_once() == 0


@pytest.mark.asyncio
async def test_crash_between_publish_and_mark_leaves_rows_for_retry(sessionmaker) -> None:
    await _seed(sessionmaker, 3)
    crashing = OutboxRelay(sessionmaker, RecordingPublisher(fail_after=1), batch_size=100, schemas=("orders",))

    with pytest.raises(RuntimeError):
        await crashing.drain_once()

    # transaction rolled back → nothing marked published → next pass re-ships all
    assert await _unpublished_count(sessionmaker) == 3

    recovered = RecordingPublisher()
    relay = OutboxRelay(sessionmaker, recovered, batch_size=100, schemas=("orders",))
    assert await relay.drain_once() == 3
    assert await _unpublished_count(sessionmaker) == 0


@pytest.mark.asyncio
async def test_skip_locked_prevents_double_claim(sessionmaker) -> None:
    await _seed(sessionmaker, 10)
    p1, p2 = RecordingPublisher(), RecordingPublisher()
    r1 = OutboxRelay(sessionmaker, p1, batch_size=100, schemas=("orders",))
    r2 = OutboxRelay(sessionmaker, p2, batch_size=100, schemas=("orders",))

    n1, n2 = await asyncio.gather(r1.drain_once(), r2.drain_once())

    assert n1 + n2 == 10
    assert await _unpublished_count(sessionmaker) == 0
    # no row published by both relays (SKIP LOCKED partitioned the batch)
    payloads = [p for _, p in p1.published] + [p for _, p in p2.published]
    assert len(payloads) == len(set(payloads)) == 10
