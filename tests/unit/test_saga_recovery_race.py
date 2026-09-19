"""Two concurrent saga-recovery pollers racing the same stuck order.

The crown-jewel composition: the recovery claim
(``FOR UPDATE SKIP LOCKED`` + the ``updated_at = now()`` lease touch), the
claim→settle gap guard (``has_recent_saga_activity``), and the guarded
``pending → paid`` flip must let exactly one poller settle a stuck order —
never two, never zero. Real Postgres (Testcontainers) + real Valkey, two
``SagaRecovery`` instances each sweeping on its own session, mirroring the
race style of ``test_inventory_reservations.py``.
"""

import asyncio
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.identity.adapters.db.repository import IdentityRepository
from src.identity.application.outbox import user_created_outbox
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.orders.adapters.db.repository import OrdersRepository
from src.orders.application.checkout_saga import payment_key_for
from src.shared.config.setting import AppSettings, get_settings
from src.shared.saga_recovery import SagaRecovery

_BATCH_TIMEOUT = 30  # generous ceiling; a wedged claim must fail the test, not hang it


def _settings() -> AppSettings:
    return AppSettings(
        _env_file=None,
        database_url=str(get_settings().database_url),
        checkout_saga_step_timeout_seconds=60,
    )


async def _stuck_paid_order(maker, session, *, sku: str) -> uuid.UUID:
    """A crashed checkout whose payment SUCCEEDED: a ``pending`` order with a
    held stock hold and a succeeded payment row — the shape the recovery poller
    must settle (commit + mark paid). The journal is backdated so the order is
    claimable (a live drive's fresh rows would block the claim)."""
    user = await IdentityRepository(session).get_or_create(
        str(uuid.uuid4()), "recovery-race@test.io", user_created_outbox
    )
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, 5, 0, 1)"), {"sku": sku}
    )
    await session.commit()
    # The hold + conditional decrement + StockReserved outbox row (real machinery).
    order_id = uuid.uuid4()
    await InventoryService(InventoryRepository(session), reservation_ttl_seconds=900).reserve(sku, 1, order_id)

    idempotency_key = f"stuck-{order_id.hex[:8]}"
    await session.execute(
        text(
            "INSERT INTO orders.orders (id, user_id, idempotency_key, idempotency_body_hash, "
            "status, total, updated_at) "
            "VALUES (:id, :user_id, :key, 'hash', 'pending', 9.99, now() - interval '1 hour')"
        ),
        {"id": order_id, "user_id": user.id, "key": idempotency_key},
    )
    await session.execute(
        text(
            "INSERT INTO orders.order_items (id, order_id, product_id, product_name, unit_price, quantity) "
            "VALUES (:id, :order_id, :product_id, 'widget', 9.99, 1)"
        ),
        {"id": uuid.uuid4(), "order_id": order_id, "product_id": uuid.uuid4()},
    )
    # The crash: the journal went quiet two hours ago (created_at strictly
    # increases along a live drive — a quiet journal means claimable).
    await session.execute(
        text(
            "INSERT INTO orders.saga_log (id, order_id, step, status, created_at, updated_at) "
            "VALUES (:id, :order_id, 'create', 'completed', now() - interval '2 hours', now() - interval '2 hours')"
        ),
        {"id": uuid.uuid4(), "order_id": order_id},
    )
    # The payment row that decides recovery (the token is never stored): succeeded.
    await session.execute(
        text(
            "INSERT INTO payments.payments (id, order_id, idempotency_key, amount, status, created_at, updated_at) "
            "VALUES (:id, :order_id, :ikey, 9.99, 'succeeded', now() - interval '1 hour', now() - interval '1 hour')"
        ),
        {"id": uuid.uuid4(), "order_id": order_id, "ikey": payment_key_for(user.id, idempotency_key)},
    )
    await session.commit()
    return order_id


async def test_two_concurrent_recovery_pollers_settle_the_same_stuck_order_exactly_once(
    async_engine, sessionmaker_factory, real_valkey, session
):
    """Two pollers start the same instant against one stuck order: the claim
    (``FOR UPDATE SKIP LOCKED`` + the lease touch) lets exactly one claim and
    settle it — the other claims nothing. Stock is consumed exactly once and
    exactly one ``OrderPlaced`` is announced ."""
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    order_id = await _stuck_paid_order(maker, session, sku="sku-recovery-race")
    settings = _settings()

    async def sweep() -> dict[str, int]:
        return await asyncio.wait_for(SagaRecovery(maker, real_valkey, settings).sweep_once(), _BATCH_TIMEOUT)

    outcomes = await asyncio.gather(sweep(), sweep())

    completed = sum(outcome["completed"] for outcome in outcomes)
    assert completed == 1, f"exactly one poller may settle the stuck order: {outcomes}"
    assert sum(outcome["deferred"] + outcome["compensated"] for outcome in outcomes) == 0

    async with maker() as check:
        status = (
            await check.execute(text("SELECT status FROM orders.orders WHERE id = :id"), {"id": order_id})
        ).scalar_one()
        stock = (
            await check.execute(
                text("SELECT on_hand, reserved FROM inventory.inventory WHERE sku = 'sku-recovery-race'")
            )
        ).one()
        placed = (
            await check.execute(text("SELECT count(*) FROM orders.outbox WHERE event_type = 'OrderPlaced'"))
        ).scalar_one()
    assert status == "paid"
    assert (stock.on_hand, stock.reserved) == (4, 0)  # consumed exactly once, never twice
    assert placed == 1  # the guarded flip announced it once


async def test_recovery_defers_when_a_client_retry_drives_in_the_claim_settle_gap(
    async_engine, sessionmaker_factory, real_valkey, session, monkeypatch
):
    """The claim→settle gap: between claiming a quiet order and settling it, a
    client retry can start driving the same order (its resume path journals
    immediately). ``has_recent_saga_activity`` must see the fresh journal row and
    defer — settling on top of a live drive would race it (and cancel a charge
    that may then succeed)."""
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    order_id = await _stuck_paid_order(maker, session, sku="sku-recovery-gap")
    settings = _settings()

    original = OrdersRepository.has_recent_saga_activity

    async def _retry_lands_first(self, oid, *, since):
        # Between the claim and this check, a client retry started driving the
        # same order — its resume path journals immediately. Simulate the fresh
        # row landing in the gap, then run the real check.
        async with maker() as retry_session:
            await retry_session.execute(
                text(
                    "INSERT INTO orders.saga_log (id, order_id, step, status) VALUES (:id, :oid, 'reserve', 'started')"
                ),
                {"id": uuid.uuid4(), "oid": oid},
            )
            await retry_session.commit()
        return await original(self, oid, since=since)

    monkeypatch.setattr(OrdersRepository, "has_recent_saga_activity", _retry_lands_first)

    outcome = await asyncio.wait_for(SagaRecovery(maker, real_valkey, settings).sweep_once(), _BATCH_TIMEOUT)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1}
    async with maker() as check:
        status = (
            await check.execute(text("SELECT status FROM orders.orders WHERE id = :id"), {"id": order_id})
        ).scalar_one()
    assert status == "pending"  # untouched — the live drive owns the outcome now


async def test_the_lease_touch_expires_and_a_later_pass_retries_a_deferred_order(
    async_engine, sessionmaker_factory, real_valkey, session
):
    """The claim's ``updated_at = now()`` touch IS the lease: a deferred order
    (payment still pending) stays claimable for the NEXT pass, and a poller that
    runs while we settle sees the fresh timestamp and skips the row."""
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    order_id = await _stuck_paid_order(maker, session, sku="sku-recovery-lease")
    # A payment still pending → the reconciler owns it; recovery defers.
    async with maker() as session_defer:
        await session_defer.execute(
            text("UPDATE payments.payments SET status = 'pending' WHERE order_id = :oid"), {"oid": order_id}
        )
        await session_defer.commit()
    settings = _settings()

    outcome = await asyncio.wait_for(SagaRecovery(maker, real_valkey, settings).sweep_once(), _BATCH_TIMEOUT)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1}
    async with maker() as check:
        status = (
            await check.execute(text("SELECT status FROM orders.orders WHERE id = :id"), {"id": order_id})
        ).scalar_one()
        stock = (
            await check.execute(
                text("SELECT on_hand, reserved FROM inventory.inventory WHERE sku = 'sku-recovery-lease'")
            )
        ).one()
    assert status == "pending"  # the reconciler owns it — recovery never guesses
    assert (stock.on_hand, stock.reserved) == (5, 1)  # the hold is untouched
