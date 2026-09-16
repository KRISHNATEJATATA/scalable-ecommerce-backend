"""Retention prune (``scripts/retention_prune.py``): the settings invariant + the sweep.

* The ``AppSettings`` validator must refuse reservation/saga-log retentions
  inside the payment reconciliation window (pruning settlement-sensitive
  history early would let a recovery replay under-count and compensate a
  settled order) — while outbox retention stays deliberately uncoupled.
* The batched sweep deletes exactly the old terminal rows against real
  Postgres (Testcontainers) and leaves recent/live ones untouched; a re-run
  is a no-op.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from scripts.retention_prune import prune_once
from src.shared.config.setting import AppSettings

_DSN = "postgresql+asyncpg://u:p@localhost:5432/test"


def _settings(**overrides) -> AppSettings:
    return AppSettings(_env_file=None, database_url=_DSN, **overrides)


# --- settings invariant -----------------------------------------------------


def test_reservation_retention_inside_settlement_window_is_refused():
    with pytest.raises(ValidationError, match="reservation_retention_days"):
        _settings(reservation_retention_days=1)


def test_saga_log_retention_inside_settlement_window_is_refused():
    with pytest.raises(ValidationError, match="saga_log_retention_days"):
        _settings(saga_log_retention_days=1)


def test_outbox_retention_is_not_coupled_to_the_settlement_window():
    """Published outbox rows are terminal once shipped — 1 day is legal."""
    assert _settings(outbox_retention_days=1).outbox_retention_days == 1


# --- the sweep --------------------------------------------------------------


async def _seed_outbox(session, schema: str, *, old: bool, published: bool) -> None:
    age = 200 if old else 1
    ts = datetime.now(UTC) - timedelta(days=age)
    await session.execute(
        text(
            f"INSERT INTO {schema}.outbox (id, event_type, payload, occurred_at, published_at) "
            "VALUES (:id, 'TestEvent', '{}', :ts, :published_at)"
        ),
        {"id": uuid.uuid4(), "ts": ts, "published_at": ts if published else None},
    )


async def _seed_terminal_history(sessionmaker, *, old: bool) -> None:
    """One released reservation + one saga_log row under a terminal order, aged old/recent."""
    age = 200 if old else 1
    ts = datetime.now(UTC) - timedelta(days=age)
    async with sessionmaker() as s:
        await s.execute(
            text(
                "INSERT INTO inventory.inventory (sku, on_hand, reserved, version) "
                "VALUES (:sku, 10, 0, 1) ON CONFLICT (sku) DO NOTHING"
            ),
            {"sku": f"sku-{age}"},
        )
        await s.execute(
            text(
                "INSERT INTO inventory.reservations "
                "(id, sku, qty, order_id, status, expires_at, created_at, updated_at) "
                "VALUES (:id, :sku, 1, :oid, 'released', :ts, :ts, :ts)"
            ),
            {"id": uuid.uuid4(), "sku": f"sku-{age}", "oid": uuid.uuid4(), "ts": ts},
        )
        oid = uuid.uuid4()
        await s.execute(
            text(
                "INSERT INTO orders.orders (id, user_id, idempotency_key, status, total, created_at, updated_at) "
                "VALUES (:id, :uid, :key, 'paid', :total, :ts, :ts)"
            ),
            {"id": oid, "uid": uuid.uuid4(), "key": str(uuid.uuid4()), "total": Decimal("1.00"), "ts": ts},
        )
        await s.execute(
            text(
                "INSERT INTO orders.saga_log (id, order_id, step, status, created_at, updated_at) "
                "VALUES (:id, :oid, 'reserve', 'completed', :ts, :ts)"
            ),
            {"id": uuid.uuid4(), "oid": oid, "ts": ts},
        )
        await s.commit()


async def test_prune_deletes_old_terminal_rows_and_keeps_live_and_recent_ones(sessionmaker_factory):
    sessionmaker = sessionmaker_factory
    async with sessionmaker() as s:
        await _seed_outbox(s, "catalog", old=True, published=True)
        await _seed_outbox(s, "catalog", old=True, published=True)
        await _seed_outbox(s, "catalog", old=False, published=True)  # recent: keep
        await _seed_outbox(s, "catalog", old=True, published=False)  # unpublished: keep
        await _seed_outbox(s, "payments", old=True, published=True)
        await s.commit()
    await _seed_terminal_history(sessionmaker, old=True)
    await _seed_terminal_history(sessionmaker, old=False)

    counts = await prune_once(sessionmaker, _settings())

    assert counts["catalog.outbox"] == 2
    assert counts["payments.outbox"] == 1
    assert counts["inventory.reservations"] == 1
    assert counts["orders.saga_log"] == 1
    assert set(counts) == {
        "catalog.outbox",
        "inventory.outbox",
        "orders.outbox",
        "payments.outbox",
        "identity.outbox",
        "inventory.reservations",
        "orders.saga_log",
    }
    async with sessionmaker() as s:
        recent = (
            await s.execute(
                text(
                    "SELECT count(*) FROM catalog.outbox WHERE occurred_at >= now() - interval '10 days' "
                    "OR published_at IS NULL"
                )
            )
        ).scalar_one()
        reservations = (await s.execute(text("SELECT count(*) FROM inventory.reservations"))).scalar_one()
        saga_log = (await s.execute(text("SELECT count(*) FROM orders.saga_log"))).scalar_one()
    assert recent == 2  # recent published + unpublished
    assert reservations == 1  # the recent terminal one
    assert saga_log == 1


async def test_prune_rerun_is_a_no_op(sessionmaker_factory):
    sessionmaker = sessionmaker_factory
    async with sessionmaker() as s:
        await _seed_outbox(s, "identity", old=True, published=True)
        await s.commit()
    await _seed_terminal_history(sessionmaker, old=True)

    assert await prune_once(sessionmaker, _settings()) == {
        "catalog.outbox": 0,
        "inventory.outbox": 0,
        "orders.outbox": 0,
        "payments.outbox": 0,
        "identity.outbox": 1,
        "inventory.reservations": 1,
        "orders.saga_log": 1,
    }
    # Re-run: everything already gone — every table reports 0.
    assert sum((await prune_once(sessionmaker, _settings())).values()) == 0


async def test_prune_loops_batches_until_a_pass_comes_back_short(sessionmaker_factory):
    """A table bigger than one batch drains over several short DELETEs — the loop's
    ``deleted < batch`` exit is what keeps a pass from holding a long lock."""
    sessionmaker = sessionmaker_factory
    async with sessionmaker() as s:
        for _ in range(3):
            await _seed_outbox(s, "catalog", old=True, published=True)
        await s.commit()

    settings = _settings(retention_prune_batch_size=1)
    counts = await prune_once(sessionmaker, settings)
    assert counts["catalog.outbox"] == 3
    async with sessionmaker() as s:
        left = (await s.execute(text("SELECT count(*) FROM catalog.outbox"))).scalar_one()
    assert left == 0


async def test_prune_leaves_held_reservations_and_pending_orders_alone(sessionmaker_factory):
    """Only *terminal* rows are prunable: a held reservation and a pending order's
    saga_log are live settlement state, whatever their age."""
    sessionmaker = sessionmaker_factory
    ts = datetime.now(UTC) - timedelta(days=200)
    async with sessionmaker() as s:
        await s.execute(
            text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES ('held-sku', 10, 1, 1)")
        )
        await s.execute(
            text(
                "INSERT INTO inventory.reservations "
                "(id, sku, qty, order_id, status, expires_at, created_at, updated_at) "
                "VALUES (:id, 'held-sku', 1, :oid, 'held', :ts, :ts, :ts)"
            ),
            {"id": uuid.uuid4(), "oid": uuid.uuid4(), "ts": ts},
        )
        oid = uuid.uuid4()
        await s.execute(
            text(
                "INSERT INTO orders.orders (id, user_id, idempotency_key, status, total, created_at, updated_at) "
                "VALUES (:id, :uid, :key, 'pending', :total, :ts, :ts)"
            ),
            {"id": oid, "uid": uuid.uuid4(), "key": str(uuid.uuid4()), "total": Decimal("1.00"), "ts": ts},
        )
        await s.execute(
            text(
                "INSERT INTO orders.saga_log (id, order_id, step, status, created_at, updated_at) "
                "VALUES (:id, :oid, 'reserve', 'started', :ts, :ts)"
            ),
            {"id": uuid.uuid4(), "oid": oid, "ts": ts},
        )
        await s.commit()

    counts = await prune_once(sessionmaker, _settings())

    assert counts["inventory.reservations"] == 0
    assert counts["orders.saga_log"] == 0
