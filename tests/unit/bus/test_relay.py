"""Outbox relay tests against Testcontainers-Postgres.

The relay's guarantees are Postgres behaviours — ``FOR UPDATE SKIP LOCKED``, the
partial index, and one-transaction publish-then-mark — so they are verified
against real Postgres (never SQLite), mirroring ``test_repositories``. The SNS
edge is a recording fake; SNS delivery itself is verified locally on LocalStack.

Covers the ticket's acceptance criteria:
- state write + outbox row → relay ships it and marks it published;
- a crash between publish and mark leaves rows unpublished → next pass re-ships
  (effectively-once downstream);
- two racing relays never double-claim a row (``SKIP LOCKED``);
- every schema in ``OUTBOX_SCHEMAS`` actually drains — a module whose outbox the
  relay never scans would otherwise write events that are never visible on the bus.
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

from src.shared.bus.constants import OUTBOX_SCHEMAS
from src.shared.bus.metrics import outbox_lag_seconds, update_outbox_lag
from src.shared.bus.relay import OutboxRelay
from src.shared.config.setting import get_settings

REPO_ROOT = Path(__file__).resolve().parents[3]
# The publishing modules, written out independently of ``OUTBOX_SCHEMAS`` on purpose:
# asserting the constant against itself is a tautology (drop ``catalog`` from the
# constant and a self-referential test drops it too, silently). This literal is the
# expected topology — changing it must be a deliberate edit here.
EXPECTED_OUTBOX_SCHEMAS = ("identity", "catalog", "inventory", "orders", "payments")
# A schema the relay is pointed at but that does not exist in the container — the
# same shape as any permanent per-schema failure (unprovisioned topic, oversized
# payload). Deliberately not a real module: every real outbox must actually drain.
MISSING_SCHEMA = "not_a_module"


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
            # Every publishing module, not just orders: the relay is only correct if
            # each schema it scans really owns an ``outbox`` table of the expected
            # shape. Migrating one module hid catalog/inventory/payments regressions.
            for module in EXPECTED_OUTBOX_SCHEMAS:
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
async def sessionmaker(_migrated):
    engine = create_async_engine(str(get_settings().database_url))
    async with engine.begin() as conn:
        for schema in EXPECTED_OUTBOX_SCHEMAS:
            await conn.execute(text(f"TRUNCATE {schema}.outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def _seed(maker, count: int, schema: str = "orders") -> list[uuid.UUID]:
    ids = []
    async with maker() as session:
        for _ in range(count):
            eid = uuid.uuid4()
            ids.append(eid)
            payload = json.dumps({"type": "OrderPlaced", "event_id": str(eid), "trace_id": uuid.uuid4().hex})
            await session.execute(
                text(f"INSERT INTO {schema}.outbox (id, event_type, payload) VALUES (:id, 'OrderPlaced', :p)"),
                {"id": eid, "p": payload},
            )
        await session.commit()
    return ids


async def _unpublished_count(maker, schema: str = "orders") -> int:
    async with maker() as session:
        return (
            await session.execute(text(f"SELECT count(*) FROM {schema}.outbox WHERE published_at IS NULL"))
        ).scalar_one()


@pytest.mark.asyncio
async def test_relay_refuses_to_start_on_real_aws_without_a_topic_arn_prefix() -> None:
    """Terraform owns the topics in the cloud and the task role is publish-only, so a
    missing ARN namespace must fail at startup, not AccessDenied on the first event."""
    from src.shared.bus.relay import run_relay
    from src.shared.config.setting import AppSettings

    dsn = "postgresql+asyncpg://u:p@localhost:5432/db"
    real_aws = AppSettings(_env_file=None, database_url=dsn, bus_endpoint_url=None)

    with pytest.raises(RuntimeError, match="BUS_TOPIC_ARN_PREFIX"):
        await run_relay(real_aws, None, schemas=("orders",))

    # ...and it is not required against LocalStack, which creates topics on demand.
    local = AppSettings(_env_file=None, database_url=dsn, bus_endpoint_url="http://localhost:4566")
    assert local.bus_topic_arn_prefix is None


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

    with pytest.raises(ExceptionGroup) as exc_info:  # TaskGroup wraps the publish failure
        await crashing.drain_once()
    assert exc_info.group_contains(RuntimeError)

    # transaction rolled back → nothing marked published → next pass re-ships all
    assert await _unpublished_count(sessionmaker) == 3

    recovered = RecordingPublisher()
    relay = OutboxRelay(sessionmaker, recovered, batch_size=100, schemas=("orders",))
    assert await relay.drain_once() == 3
    assert await _unpublished_count(sessionmaker) == 0


@pytest.mark.asyncio
async def test_publishes_are_concurrent_and_bounded(sessionmaker) -> None:
    """Batch publishes overlap (not serial) but never exceed ``concurrency``."""
    in_flight = peak = 0

    class SlowPublisher:
        published: list[tuple[str, str]] = []

        async def publish(self, event_type: str, payload: str) -> None:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1

    await _seed(sessionmaker, 10)
    relay = OutboxRelay(sessionmaker, SlowPublisher(), batch_size=100, schemas=("orders",), concurrency=4)

    assert await relay.drain_once() == 10
    assert peak > 1  # serial publishing held the row locks for batch x RTT
    assert peak <= 4  # and it stays inside the configured bound
    assert await _unpublished_count(sessionmaker) == 0


@pytest.mark.asyncio
async def test_every_outbox_schema_actually_drains(sessionmaker) -> None:
    """The relay must ship rows from **every** publishing module.

    Exercising only ``orders`` left a whole class of regression invisible: a module
    whose schema was dropped from the allow-list (or whose outbox table drifted)
    would keep writing events in-transaction that never became visible on the bus.
    The expected set is the independent ``EXPECTED_OUTBOX_SCHEMAS`` literal, so a
    module silently disappearing from ``OUTBOX_SCHEMAS`` fails here.
    """
    assert set(OUTBOX_SCHEMAS) == set(EXPECTED_OUTBOX_SCHEMAS)

    for schema in EXPECTED_OUTBOX_SCHEMAS:
        await _seed(sessionmaker, 2, schema)
    publisher = RecordingPublisher()
    relay = OutboxRelay(sessionmaker, publisher, batch_size=100)  # default = OUTBOX_SCHEMAS

    assert await relay.drain_once() == 2 * len(EXPECTED_OUTBOX_SCHEMAS)
    for schema in EXPECTED_OUTBOX_SCHEMAS:
        assert await _unpublished_count(sessionmaker, schema) == 0, schema
    assert len(publisher.published) == 2 * len(EXPECTED_OUTBOX_SCHEMAS)


@pytest.mark.asyncio
async def test_one_broken_schema_does_not_starve_the_others(sessionmaker) -> None:
    """A schema that fails must not abort the pass for the schemas after it.

    ``MISSING_SCHEMA`` has no ``outbox`` table in this container, which is the same
    shape as any permanent per-schema failure — an unprovisioned topic, an oversized
    payload. ``orders`` still has to drain, and the pass must not raise, or one stuck
    module would silently stop every other module from ever shipping an event.
    """
    await _seed(sessionmaker, 3)
    publisher = RecordingPublisher()
    relay = OutboxRelay(sessionmaker, publisher, batch_size=100, schemas=(MISSING_SCHEMA, "orders"))

    assert await relay.drain_once() == 3
    assert await _unpublished_count(sessionmaker) == 0


@pytest.mark.asyncio
async def test_a_failing_schema_still_backs_off_when_the_others_are_idle(sessionmaker) -> None:
    """A failure with nothing shipped anywhere must propagate so the loop backs off.

    Counting failed schemas let this slip: with ``orders`` empty, its drain
    "succeeded" trivially, so a broken ``catalog`` was not *every* schema failing
    and ``drain_once`` returned 0. SNS being down during a quiet minute therefore
    looked identical to having no work, and the poll loop hammered it at the full
    poll rate instead of backing off exponentially.
    """
    relay = OutboxRelay(sessionmaker, RecordingPublisher(), batch_size=100, schemas=(MISSING_SCHEMA, "orders"))

    with pytest.raises(Exception):  # noqa: B017 - any propagated failure engages the backoff
        await relay.drain_once()


@pytest.mark.asyncio
async def test_every_schema_failing_propagates_so_the_loop_backs_off(sessionmaker) -> None:
    """A shared cause (SNS down) must still surface, so poll_forever backs off
    exponentially instead of retrying once per poll interval forever."""
    await _seed(sessionmaker, 1)
    relay = OutboxRelay(
        sessionmaker, RecordingPublisher(fail_after=0), batch_size=100, schemas=("orders", MISSING_SCHEMA)
    )

    with pytest.raises((ExceptionGroup, Exception)):
        await relay.drain_once()
    assert await _unpublished_count(sessionmaker) == 1


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


# --- outbox lag gauge -------------------------------------------------------


def _lag_value(schema: str) -> float:
    value = outbox_lag_seconds.labels(schema)._value.get()  # noqa: SLF001 - test-only read
    return 0.0 if value is None else value


@pytest.mark.asyncio
async def test_update_outbox_lag_measures_the_database(sessionmaker) -> None:
    """The gauge reads the DB, not the observer: a seeded unpublished row's age
    lands on its schema, an empty schema reads 0 (drained, not unknown)."""
    await _seed(sessionmaker, 2, "orders")
    await _seed(sessionmaker, 1, "catalog")

    await update_outbox_lag(sessionmaker, schemas=("orders", "catalog", "identity"))

    assert _lag_value("orders") > 0
    assert _lag_value("catalog") > 0
    assert _lag_value("identity") == 0.0


@pytest.mark.asyncio
async def test_relay_stamps_lag_at_claim_and_drains_to_zero(sessionmaker) -> None:
    """The claim *is* a lag observation (rows arrive oldest-first), and a fully
    drained schema reports 0 on the next pass. The seeded rows are backdated so
    the age is unambiguously positive even if the app clock runs ahead of the
    DB clock (``max(..., 0.0)`` would otherwise clamp the very-young case)."""
    await _seed(sessionmaker, 2, "orders")
    async with sessionmaker() as session:
        await session.execute(
            text(
                "UPDATE orders.outbox SET occurred_at = occurred_at - interval '10 seconds' WHERE published_at IS NULL"
            )
        )
        await session.commit()
    relay = OutboxRelay(sessionmaker, RecordingPublisher(), batch_size=100, schemas=("orders",))

    await relay.drain_once()
    assert _lag_value("orders") > 0

    assert await relay.drain_once() == 0
    assert _lag_value("orders") == 0.0
