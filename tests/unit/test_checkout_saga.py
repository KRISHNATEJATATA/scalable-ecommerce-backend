"""Checkout saga + order ownership.

The crown-jewel flow end to end against real Postgres (never SQLite): the
guarantees under test live in Postgres semantics — the composite
``UNIQUE(user_id, idempotency_key)``, the guarded ``pending → paid/cancelled``
transitions, and the ``FOR UPDATE SKIP LOCKED`` recovery claim. Inventory and
payments run as the real services (stub gateway only); the basket and the
Valkey fast path are tiny fakes over the saga's own ports.

Uses the shared Testcontainers-Postgres fixtures from ``conftest.py``.
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from prometheus_client import REGISTRY
from sqlalchemy import text

from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.orders.adapters.db.repository import OrdersRepository
from src.orders.application.checkout_saga import CheckoutSaga, payment_key_for
from src.orders.application.service import OrdersService
from src.orders.domain.order import OrderStatus
from src.orders.ports.checkout import CheckoutLine
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.stub_gateway import StubPaymentGateway
from src.payments.application.service import PaymentsService
from src.shared.container import OrderCharges, OrderStockHolds
from src.shared.errors.exceptions import (
    AuthorizationError,
    CheckoutIdempotencyConflictError,
    InsufficientStockError,
    OrderStateConflictError,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

USER_A = uuid.uuid4()
USER_B = uuid.uuid4()


class _Basket:
    """In-memory basket keyed by user (the saga's BasketPort, no Valkey)."""

    def __init__(self) -> None:
        self.lines: dict[uuid.UUID, list[CheckoutLine]] = {}
        self.cleared: list[uuid.UUID] = []

    def stock(self, user_id: uuid.UUID, *lines: CheckoutLine) -> None:
        self.lines[user_id] = list(lines)

    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        return list(self.lines.get(user_id, []))

    async def clear(self, user_id: uuid.UUID) -> None:
        self.lines.pop(user_id, None)
        self.cleared.append(user_id)


class _Idempotency:
    """In-memory fast path (the saga's IdempotencyPort, no Valkey)."""

    def __init__(self) -> None:
        self.records: dict[tuple[uuid.UUID, str], dict[str, Any]] = {}

    async def get(self, user_id: uuid.UUID, key: str):
        from src.orders.ports.checkout import IdempotencyRecord

        record = self.records.get((user_id, key))
        if record is None:
            return None
        return IdempotencyRecord(body_hash=record["body_hash"], status=record["status"], response=record["response"])

    async def put(self, user_id, key, *, body_hash, status, response) -> None:
        self.records[(user_id, key)] = {
            "body_hash": body_hash,
            "status": status,
            "response": response.model_dump(mode="json"),
        }


def _line(product_no: int = 1, qty: int = 1, price: str = "19.99") -> CheckoutLine:
    return CheckoutLine(
        product_id=uuid.uuid5(uuid.NAMESPACE_DNS, f"product-{product_no}"),
        name=f"Product {product_no}",
        unit_price=Decimal(price),
        quantity=qty,
    )


def _saga(session, basket: _Basket, *, gateway: StubPaymentGateway | None = None) -> CheckoutSaga:
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(
        PaymentsRepository(session),
        gateway or StubPaymentGateway(),
        webhook_secret="test-secret",
        reconciliation_grace_seconds=30,
        reconciliation_max_age_seconds=604800,
    )
    return CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        step_timeout_seconds=60,
    )


def _orders_service(session) -> OrdersService:
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    return OrdersService(OrdersRepository(session), OrderStockHolds(inventory))


async def _seed(session, sku: str, on_hand: int) -> None:
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, :on_hand, 0, 1)"),
        {"sku": sku, "on_hand": on_hand},
    )
    await session.commit()


async def _stock(session, sku: str) -> tuple[int, int]:
    row = (
        await session.execute(text("SELECT on_hand, reserved FROM inventory.inventory WHERE sku = :sku"), {"sku": sku})
    ).one()
    return row.on_hand, row.reserved


async def _orders_count(session) -> int:
    return (await session.execute(text("SELECT count(*) FROM orders.orders"))).scalar_one()


async def _order_status(session, order_id: uuid.UUID) -> str:
    return (
        await session.execute(text("SELECT status FROM orders.orders WHERE id = :id"), {"id": order_id})
    ).scalar_one()


async def _saga_steps(session, order_id: uuid.UUID) -> list[str]:
    rows = (
        await session.execute(
            text("SELECT step FROM orders.saga_log WHERE order_id = :id ORDER BY created_at"), {"id": order_id}
        )
    ).all()
    return [row.step for row in rows]


async def _orders_outbox(session) -> list[str]:
    rows = (await session.execute(text("SELECT event_type FROM orders.outbox ORDER BY occurred_at"))).all()
    return [row.event_type for row in rows]


async def _backdate_pending(session, order_id: uuid.UUID, seconds: int = 3600) -> None:
    """Age the stuck order **and its journal**: the recovery claim treats a
    fresh ``saga_log`` row as a live drive (the liveness heartbeat) and skips
    the order — a true crash must be quiet in both places."""
    past = datetime.now(UTC) - timedelta(seconds=seconds)
    await session.execute(
        text("UPDATE orders.orders SET created_at = :past, updated_at = :past WHERE id = :id"),
        {"past": past, "id": order_id},
    )
    await session.execute(
        text("UPDATE orders.saga_log SET created_at = :past, updated_at = :past WHERE order_id = :id"),
        {"past": past, "id": order_id},
    )
    await session.commit()


def _counter(name: str, **labels: str) -> float:
    """One labeled Prometheus series' current value, 0 if never incremented.

    The registry is process-global and shared across tests, so assertions are
    always on deltas read around the action.
    """
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else value


# --- metrics ---------------------------------------------------------------


async def test_checkout_outcome_taxonomy_is_counted_once_per_call(session):
    """paid / replayed / conflict land on ``checkout_attempts_total``; the
    declined checkout's undo lands on ``checkout_compensation_total{step}`` —
    one increment per checkout, never two."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)

    before = {k: _counter("checkout_attempts_total", outcome=k) for k in ("paid", "replayed", "conflict")}
    comp_before = _counter("checkout_compensation_total", step="charge")

    await saga.checkout(user_id=USER_A, idempotency_key="m-paid", payment_token="tok_visa")
    await saga.checkout(user_id=USER_A, idempotency_key="m-paid", payment_token="tok_visa")  # exact replay
    basket.stock(USER_A, line)  # the paid checkout cleared the basket; a new checkout needs a cart
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="m-decline", payment_token="tok_decline_card")
    except OrderStateConflictError:
        pass

    assert _counter("checkout_attempts_total", outcome="paid") == before["paid"] + 1
    assert _counter("checkout_attempts_total", outcome="replayed") == before["replayed"] + 1
    assert _counter("checkout_attempts_total", outcome="conflict") == before["conflict"] + 1
    assert _counter("checkout_compensation_total", step="charge") == comp_before + 1


async def test_stock_refusal_counts_as_out_of_stock_not_conflict(session):
    """The shelves refusing is its own outcome — distinct from the lifecycle's
    409s — and its compensation is the reserve step."""
    line = _line(qty=3)
    await _seed(session, str(line.product_id), 1)
    basket = _Basket()
    basket.stock(USER_A, line)

    before = _counter("checkout_attempts_total", outcome="out_of_stock")
    comp_before = _counter("checkout_compensation_total", step="reserve")

    try:
        await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="m-short", payment_token="tok_visa")
    except InsufficientStockError:
        pass

    assert _counter("checkout_attempts_total", outcome="out_of_stock") == before + 1
    assert _counter("checkout_compensation_total", step="reserve") == comp_before + 1


async def test_recovery_settlements_are_counted_with_their_compensation(session):
    """The poller's process counts its own settlements; its compensation run is
    labeled ``crashed``, distinct from a live drive's ``reserve``/``charge``."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-m-recover",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    await _backdate_pending(session, order.id)

    before = _counter("checkout_recovery_total", outcome="compensated")
    comp_before = _counter("checkout_compensation_total", step="crashed")

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0}
    assert _counter("checkout_recovery_total", outcome="compensated") == before + 1
    assert _counter("checkout_compensation_total", step="crashed") == comp_before + 1


# --- happy path ----------------------------------------------------------


async def test_checkout_pays_the_order_and_consumes_stock(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    order, created = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-1", payment_token="tok_visa"
    )

    assert created is True
    assert order.status == OrderStatus.PAID
    assert order.total == Decimal("19.99")
    assert order.items[0].product_name == "Product 1"  # snapshot, not a live reference
    assert order.items[0].unit_price == Decimal("19.99")
    assert await _stock(session, str(line.product_id)) == (4, 0)  # committed sale, no hold left
    assert await _orders_outbox(session) == ["OrderPlaced"]
    assert USER_A in basket.cleared  # basket emptied only on success
    steps = await _saga_steps(session, order.id)
    assert {"create", "reserve", "charge", "commit", "mark_paid"} <= set(steps)  # journal is complete


# --- idempotency ----------------------------------------------------------


async def test_same_key_same_body_replays_one_order(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)

    first, _ = await saga.checkout(user_id=USER_A, idempotency_key="key-replay", payment_token="tok_visa")
    basket.stock(USER_A, line)  # a retry re-presents the same cart
    second, created = await saga.checkout(user_id=USER_A, idempotency_key="key-replay", payment_token="tok_visa")

    assert created is False
    assert second.id == first.id
    assert await _orders_count(session) == 1
    assert await _stock(session, str(line.product_id)) == (4, 0)  # no second reservation


async def test_same_key_different_body_is_409(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)

    await saga.checkout(user_id=USER_A, idempotency_key="key-clash", payment_token="tok_visa")

    basket.stock(USER_A, _line(qty=2))  # same key, different token
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-clash", payment_token="tok_other")
    except CheckoutIdempotencyConflictError:
        pass
    else:
        raise AssertionError("expected CheckoutIdempotencyConflictError")
    assert await _orders_count(session) == 1


async def test_db_backstop_replays_after_fast_path_loss(session):
    """Valkey evicted: a fresh saga (empty fast path) replays from the DB row —
    same body returns the order, different body is still 409."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    first, _ = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-evict", payment_token="tok_visa"
    )
    basket.stock(USER_A, line)

    second, created = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-evict", payment_token="tok_visa"
    )
    assert created is False
    assert second.id == first.id
    assert await _orders_count(session) == 1

    basket.stock(USER_A, _line(qty=2))
    try:
        await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="key-evict", payment_token="tok_other")
    except CheckoutIdempotencyConflictError:
        pass
    else:
        raise AssertionError("expected CheckoutIdempotencyConflictError")


async def test_crashed_checkout_resumes_from_stored_lines_with_empty_cart(session):
    """A retry after a crash drives the pending order home from its own
    snapshots — the cart may be empty (or changed) by then, and the retry must
    still complete, not 409 on "cart is empty"."""
    from src.orders.application.checkout_saga import body_hash_for

    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)

    await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-crash-resume",
        body_hash=body_hash_for("tok_visa"),
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    basket = _Basket()  # deliberately empty: the live cart is gone, the snapshots aren't

    order, created = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-crash-resume", payment_token="tok_visa"
    )

    assert created is False
    assert order.status == OrderStatus.PAID
    assert await _orders_count(session) == 1
    assert await _stock(session, str(line.product_id)) == (4, 0)


async def test_same_key_is_per_user_not_global(session):
    """The composite UNIQUE: two users may reuse the same client-supplied key."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    basket.stock(USER_B, line)
    saga = _saga(session, basket)

    first, _ = await saga.checkout(user_id=USER_A, idempotency_key="shared-key", payment_token="tok_visa")
    second, _ = await saga.checkout(user_id=USER_B, idempotency_key="shared-key", payment_token="tok_visa")

    assert first.id != second.id
    assert await _orders_count(session) == 2


# --- replay basket mop-up ------------------------------------


async def test_fast_path_replay_leaves_a_rebuilt_basket_alone(session):
    """a replay of a stored 201 must NOT clear a cart the user
    built after the original checkout — the mop-up clear fires only when the
    current basket still matches the order's lines."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)
    order, _ = await saga.checkout(user_id=USER_A, idempotency_key="key-rebuilt", payment_token="tok_visa")
    assert len(basket.cleared) == 1  # the drive's own clear

    rebuilt = _line(product_no=2)
    basket.stock(USER_A, rebuilt)
    replayed, created = await saga.checkout(user_id=USER_A, idempotency_key="key-rebuilt", payment_token="tok_visa")

    assert created is False
    assert replayed.id == order.id
    assert basket.lines[USER_A] == [rebuilt]  # the rebuilt basket survived
    assert len(basket.cleared) == 1  # no clear from the replay


async def test_fast_path_replay_clears_a_basket_still_matching_the_order(session):
    """The one scenario the mop-up serves: a crash between the drive's clear
    and the replay record left the cart uncleaned — the replay clears it, even
    when a catalog refresh drifted name/price (ids + quantities decide)."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)
    order, _ = await saga.checkout(user_id=USER_A, idempotency_key="key-mopup", payment_token="tok_visa")

    drifted = CheckoutLine(
        product_id=line.product_id,
        name="Renamed by ProductUpdated",
        unit_price=Decimal("24.99"),
        quantity=line.quantity,
    )
    basket.stock(USER_A, drifted)
    replayed, created = await saga.checkout(user_id=USER_A, idempotency_key="key-mopup", payment_token="tok_visa")

    assert created is False
    assert replayed.id == order.id
    assert len(basket.cleared) == 2  # drive's clear + the replay's mop-up
    assert basket.lines.get(USER_A) is None


async def test_db_backstop_replay_leaves_a_rebuilt_basket_alone(session):
    """DB backstop: with the fast path lost, a replay of
    the stored paid order must not clear a basket that no longer matches."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    order, _ = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-backstop-rebuilt", payment_token="tok_visa"
    )
    assert len(basket.cleared) == 1

    rebuilt = _line(product_no=2)
    basket.stock(USER_A, rebuilt)
    replayed, created = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-backstop-rebuilt", payment_token="tok_visa"
    )  # fresh fast path: the DB backstop answers

    assert created is False
    assert replayed.id == order.id
    assert basket.lines[USER_A] == [rebuilt]  # the rebuilt basket survived
    assert len(basket.cleared) == 1  # only the drive's clear, never the backstop's


# --- compensation ----------------------------------------------------------


async def test_declined_payment_cancels_and_releases_stock(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    try:
        await _saga(session, basket).checkout(
            user_id=USER_A, idempotency_key="key-decline", payment_token="tok_decline_card"
        )
    except OrderStateConflictError:
        pass
    else:
        raise AssertionError("expected OrderStateConflictError")

    assert await _orders_count(session) == 1
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (5, 0)  # hold released, sale not consumed
    assert USER_A not in basket.cleared  # a cancelled checkout keeps its cart


async def test_stock_shortage_cancels_without_charging(session):
    line = _line(qty=3)
    await _seed(session, str(line.product_id), 1)
    basket = _Basket()
    basket.stock(USER_A, line)

    try:
        await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="key-short", payment_token="tok_visa")
    except InsufficientStockError:
        pass
    else:
        raise AssertionError("expected InsufficientStockError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (1, 0)
    assert await _orders_outbox(session) == []  # cancelled orders announce nothing


async def test_empty_cart_is_409_without_an_order(session):
    basket = _Basket()
    try:
        await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="key-empty", payment_token="tok_visa")
    except OrderStateConflictError:
        pass
    else:
        raise AssertionError("expected OrderStateConflictError")
    assert await _orders_count(session) == 0


async def test_charge_timeout_with_unknown_outcome_leaves_pending_for_recovery(session):
    """A charge that times out with no recorded outcome must NOT compensate: the
    gateway may still succeed asynchronously, and cancelling would take money
    without an order. The order stays pending for the reconciler/recovery."""

    import asyncio

    from src.orders.ports.checkout import ChargeResult

    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _HangingCharges:
        async def charge(self, **kwargs):
            await asyncio.sleep(30)  # longer than any step timeout below

        async def find_by_idempotency_key(self, key: str):
            row = await payments.get_by_idempotency_key(key)
            return None if row is None else ChargeResult(status=row.status)

    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(inventory),
        _HangingCharges(),
        _Idempotency(),
        step_timeout_seconds=0.05,
    )
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-hang", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "outcome unknown" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"  # not cancelled
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept, not released


async def test_charge_timeout_with_pending_payment_leaves_pending_not_cancelled(session):
    """F1 regression: a charge timeout with a recorded **pending** payment row
    must NOT compensate — the gateway may still confirm a moment later and
    cancelling would take money without an order. Same handling as the
    unknown-outcome case."""
    import asyncio

    from src.orders.ports.checkout import ChargeResult

    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _HangingAfterRowCharges:
        """Commits the pending payment row (as PaymentsService.charge does
        before the gateway call), then hangs past the step timeout."""

        def __init__(self, saga_key: str) -> None:
            self._saga_key = saga_key

        async def charge(self, **kwargs):
            await PaymentsRepository(session).create_pending(
                order_id=kwargs["order_id"],
                idempotency_key=kwargs["idempotency_key"],
                amount=kwargs["amount"],
            )
            await asyncio.sleep(30)  # longer than any step timeout below

        async def find_by_idempotency_key(self, key: str):
            row = await payments.get_by_idempotency_key(key)
            return None if row is None else ChargeResult(status=row.status)

    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(inventory),
        _HangingAfterRowCharges("key-hang-pending"),
        _Idempotency(),
        step_timeout_seconds=0.05,
    )
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-hang-pending", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "outcome unknown" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"  # not cancelled
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept, not released


async def test_failure_after_payment_never_compensates(session):
    """Once money moved, even a later step failure leaves the order pending for
    recovery — unwinding would take payment without an order."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _BreakingCommitHolds(OrderStockHolds):
        async def commit_for_order(self, order_id):
            raise RuntimeError("commit transport blew up")

    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        _BreakingCommitHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        step_timeout_seconds=60,
    )
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-boom", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "settled automatically" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"  # not cancelled
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept for recovery


# --- cancel vs. the guarded mark_paid flip-------------------


async def test_cancel_wins_after_payment_counts_an_orphaned_paid_payment(session):
    """The cancel flips the order ``cancelled`` after the charge succeeded: the
    saga's ``mark_paid`` flip loses, the caller gets a 409, and the
    ``succeeded`` payment row is attached to a cancelled order — money taken,
    no order. Nothing reconciles that pair automatically, so the orphan
    counter is the alertable signal (the log line is not)."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _CancelWinsAfterCharge(OrdersRepository):
        """A user-facing cancel wins the guarded flip between the saga's commit
        and its ``mark_paid``; the saga's own ``pending → paid`` flip then
        loses (``None``)."""

        async def transition_status(self, order_id, *, expect, to_status, outbox=None):
            if to_status == OrderStatus.PAID:
                # The canceller's flip landed first — perform it as
                # OrdersService.cancel_order would, then lose our own.
                await super().transition_status(order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
                return None
            return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)

    saga = CheckoutSaga(
        _CancelWinsAfterCharge(session),
        basket,
        OrderStockHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        step_timeout_seconds=60,
    )

    before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-cancel-wins", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "reconciled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    assert _counter("checkout_orphaned_paid_payments_total") == before + 1
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    payment_status = (
        await session.execute(text("SELECT status FROM payments.payments WHERE order_id = :id"), {"id": order_id})
    ).scalar_one()
    assert payment_status == "succeeded"  # the orphaned pair: money taken, order cancelled


async def test_poller_settling_concurrently_does_not_count_an_orphan(session):
    """The benign arm of the lost flip: the recovery poller's own
    ``pending → paid`` won, the drive reads final state and returns it — a
    settled order, not an orphan, so the counter must stay put."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _PollerSettlesFirst(OrdersRepository):
        """The poller's guarded flip won the race: it marks the order ``paid``
        before this drive's flip, which then loses (``None``)."""

        async def transition_status(self, order_id, *, expect, to_status, outbox=None):
            if to_status == OrderStatus.PAID:
                await super().transition_status(order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.PAID)
                return None
            return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)

    saga = CheckoutSaga(
        _PollerSettlesFirst(session),
        basket,
        OrderStockHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        step_timeout_seconds=60,
    )

    before = _counter("checkout_orphaned_paid_payments_total")
    order, created = await saga.checkout(user_id=USER_A, idempotency_key="key-poller-first", payment_token="tok_visa")

    assert created is True
    assert order.status == OrderStatus.PAID
    assert _counter("checkout_orphaned_paid_payments_total") == before  # benign settle is not an orphan


# --- recovery --------------------------------------------------------------


async def test_recovery_skips_an_order_with_fresh_journal_activity(session):
    """F2 regression, both guards: (1) the claim skips orders whose journal
    shows fresh activity (a live drive), and (2) a retry that lands between
    claim and settle makes the settle re-check defer instead of racing it."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)

    async def _stuck(key: str):
        order, _ = await repo.create_pending_order(
            user_id=USER_A,
            idempotency_key=key,
            body_hash="hash",
            total=Decimal("19.99"),
            lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
        )
        await inventory.reserve(str(line.product_id), line.quantity, order.id)
        await _backdate_pending(session, order.id)  # a quiet, crashed checkout
        return order

    # (1) A live-looking drive (fresh journal row) is not claimable at all.
    live = await _stuck("key-live-drive")
    await repo.log_saga_step(live.id, "reserve", "started")  # the drive journals fresh

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )
    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0}
    assert await _order_status(session, live.id) == "pending"  # untouched
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept

    # (2) The claim→settle gap: the retry journals AFTER the claim, and the
    # settle re-check must defer rather than compensate under it.
    racing = await _stuck("key-mid-lease-retry")
    await _backdate_pending(session, racing.id)  # quiet again, but...

    class _RetryLandsMidLease(OrdersRepository):
        """Simulates the client retry arriving between the claim and the
        settle: after every claim, its resume path journals fresh activity."""

        def __init__(self, session, target_id: uuid.UUID) -> None:
            super().__init__(session)
            self._target = target_id
            self._claimed = False

        async def claim_stuck_pending(self, *, cutoff, batch_size):
            rows = await super().claim_stuck_pending(cutoff=cutoff, batch_size=batch_size)
            if not self._claimed:
                self._claimed = True
                await self.log_saga_step(self._target, "reserve", "started")
            return rows

    outcome = await CheckoutSaga(
        _RetryLandsMidLease(session, racing.id),
        _Basket(),
        OrderStockHolds(inventory),
        OrderCharges(PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")),
        _Idempotency(),
        step_timeout_seconds=60,
    ).recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1}
    assert await _order_status(session, racing.id) == "pending"  # untouched
    assert await _stock(session, str(line.product_id)) == (5, 2)  # both holds kept


async def test_recovery_rolls_back_a_poisoned_order_and_finishes_the_batch(session):
    """F7 regression: one order failing mid-statement (a not-null violation the
    repo does not roll back) aborts the session's transaction — without a
    rollback in the error boundary, every later order in the batch would fail
    with ``PendingRollbackError`` and never settle."""
    good_line = _line(product_no=2)
    bad_line = _line(product_no=3)
    await _seed(session, str(good_line.product_id), 5)
    await _seed(session, str(bad_line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)

    async def _stuck(key: str, line: CheckoutLine):
        order, _ = await repo.create_pending_order(
            user_id=USER_A,
            idempotency_key=key,
            body_hash="hash",
            total=Decimal("19.99"),
            lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
        )
        await inventory.reserve(str(line.product_id), line.quantity, order.id)
        await _backdate_pending(session, order.id)
        return order

    poisoned = await _stuck("key-poison", bad_line)
    healthy = await _stuck("key-healthy", good_line)
    poisoned_id, healthy_id = poisoned.id, healthy.id  # plain values: the recovery's rollback expires the rows

    class _PoisonOneSettle(OrdersRepository):
        """The poisoned order's settle dies mid-statement on a NOT NULL
        violation — the exact shape of a DB-level failure inside recovery."""

        def __init__(self, session, target_id: uuid.UUID) -> None:
            super().__init__(session)
            self._target = target_id

        async def transition_status(self, order_id, *, expect, to_status, outbox=None):
            if order_id == self._target:
                await self._session.execute(
                    text("UPDATE orders.orders SET total = NULL WHERE id = :id"), {"id": order_id}
                )
            return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)

    outcome = await CheckoutSaga(
        _PoisonOneSettle(session, poisoned_id),
        _Basket(),
        OrderStockHolds(inventory),
        OrderCharges(PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")),
        _Idempotency(),
        step_timeout_seconds=60,
    ).recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 1}
    assert await _order_status(session, healthy_id) == "cancelled"  # batch finished despite the poison
    assert await _order_status(session, poisoned_id) == "pending"  # deferred, retried next pass


async def test_recovery_completes_a_checkout_crashed_after_payment(session):
    """Crash between charge and commit: the true stuck state is pending order +
    held reservation + succeeded payment — settle it to paid with one event."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-crash-paid",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    payment = await payments.charge(
        order_id=order.id,
        idempotency_key=payment_key_for(USER_A, "key-crash-paid"),
        amount=Decimal("19.99"),
        payment_method_token="tok_visa",
    )
    assert payment.status == "succeeded"
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 1, "compensated": 0, "deferred": 0}
    assert await _order_status(session, order.id) == "paid"
    assert await _stock(session, str(line.product_id)) == (4, 0)
    assert await _orders_outbox(session) == ["OrderPlaced"]


async def test_recovery_compensates_a_checkout_crashed_before_payment(session):
    """Crash before any charge: no payment row, so release + cancel."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)

    order, created = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-crash-early",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    assert created is True
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0}
    assert await _order_status(session, order.id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (5, 0)


async def test_recovery_defers_while_payment_is_pending(session):
    """A still-pending payment belongs to the reconciler, not the recovery poller."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-crash-pending",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    # A `pending` payment row with no outcome yet (webhook never arrived).
    await PaymentsRepository(session).create_pending(
        order_id=order.id,
        idempotency_key=payment_key_for(USER_A, "key-crash-pending"),
        amount=Decimal("19.99"),
    )
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1}
    assert await _order_status(session, order.id) == "pending"


# --- ownership (13.1: orders half) ------------------------------------------


async def test_consumer_cannot_read_another_users_order(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="key-mine", payment_token="tok_visa")
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    service = _orders_service(session)

    try:
        await service.get_order_detail(user_id=USER_B, order_id=order_id, is_admin=False)
    except AuthorizationError:
        pass
    else:
        raise AssertionError("expected AuthorizationError")

    own = await service.get_order_detail(user_id=USER_A, order_id=order_id, is_admin=False)
    assert own is not None and own.id == order_id
    as_admin = await service.get_order_detail(user_id=USER_B, order_id=order_id, is_admin=True)
    assert as_admin is not None and as_admin.id == order_id


async def test_consumer_cancels_own_pending_order_but_not_paid_or_anothers(session):
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)
    paid, _ = await saga.checkout(user_id=USER_A, idempotency_key="key-paid", payment_token="tok_visa")

    repo = OrdersRepository(session)
    pending, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-pending-cancel",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    service = _orders_service(session)

    try:
        await service.cancel_order(user_id=USER_B, order_id=pending.id, is_admin=False)
    except AuthorizationError:
        pass
    else:
        raise AssertionError("expected AuthorizationError")

    cancelled = await service.cancel_order(user_id=USER_A, order_id=pending.id, is_admin=False)
    assert cancelled is not None and cancelled.status == OrderStatus.CANCELLED
    again = await service.cancel_order(user_id=USER_A, order_id=pending.id, is_admin=False)
    assert again is not None and again.status == OrderStatus.CANCELLED  # idempotent

    try:
        await service.cancel_order(user_id=USER_A, order_id=paid.id, is_admin=False)
    except OrderStateConflictError:
        pass
    else:
        raise AssertionError("expected OrderStateConflictError for a paid order")


async def test_cancel_refused_while_a_charge_may_be_in_flight(session):
    """F3 regression: a ``charge`` journal row stuck at ``started`` or
    ``completed`` on a still-pending order means money may be moving — the
    cancel must refuse instead of releasing holds a succeeding payment would
    need. The recovery poller owns those orders."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-charge-inflight",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    service = _orders_service(session)

    await repo.log_saga_step(order.id, "charge", "started")
    try:
        await service.cancel_order(user_id=USER_A, order_id=order.id, is_admin=False)
    except OrderStateConflictError as exc:
        assert "in progress" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError while charge in flight")
    assert await _order_status(session, order.id) == "pending"  # untouched
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept

    await repo.log_saga_step(order.id, "charge", "completed")  # completed-but-unsettled: same refusal
    try:
        await service.cancel_order(user_id=USER_A, order_id=order.id, is_admin=False)
    except OrderStateConflictError:
        pass
    else:
        raise AssertionError("expected OrderStateConflictError after charge completed")


async def test_cancel_finishes_release_after_a_crashed_cancel(session):
    """Re-cancel of an already-cancelled order idempotently finishes the
    cleanup a crash between flip and release left behind."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-crashed-cancel",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    # A crash after the guarded flip but before the release:
    await repo.transition_status(order.id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold still held

    service = _orders_service(session)
    result = await service.cancel_order(user_id=USER_A, order_id=order.id, is_admin=False)
    assert result is not None and result.status == OrderStatus.CANCELLED
    assert await _stock(session, str(line.product_id)) == (5, 0)  # orphaned hold now released


async def test_replaying_a_cancelled_checkout_is_409_not_201(session):
    """F8 regression: the DB backstop must re-raise a cancelled checkout's
    failure on replay (409), never return 201 with a cancelled body."""
    line = _line(qty=3)
    await _seed(session, str(line.product_id), 1)  # stock refusal → cancelled order
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket)
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-cancelled-replay", payment_token="tok_visa")
    except InsufficientStockError:
        pass
    else:
        raise AssertionError("expected InsufficientStockError on the first attempt")

    basket.stock(USER_A, line)  # re-present the same cart; the order row still exists
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-cancelled-replay", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "already cancelled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError on the cancelled replay")


# --- migrations --------------------------------------------------------------


async def test_orders_chain_round_trips_downgrade_base_to_head(session):
    """The recorded schema defects stay fixed: the initial downgrade drops the
    enum (``checkfirst=True``) so base → head works, and head carries the
    composite ``(user_id, idempotency_key)`` UNIQUE instead of the global one."""
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "src/orders/alembic.ini", "downgrade", "base"],
        cwd=REPO_ROOT,
        check=True,
    )
    try:
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "src/orders/alembic.ini", "upgrade", "head"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "src/orders/alembic.ini", "upgrade", "head"],
            cwd=REPO_ROOT,
            check=True,
        )
    assert result.returncode == 0
    constraints = (
        await session.execute(
            text(
                "SELECT conname FROM pg_constraint WHERE connamespace = 'orders'::regnamespace "
                "AND contype = 'u' ORDER BY conname"
            )
        )
    ).all()
    names = [row.conname for row in constraints]
    assert "uq_orders_user_id_idempotency_key" in names
    assert "uq_orders_idempotency_key" not in names
