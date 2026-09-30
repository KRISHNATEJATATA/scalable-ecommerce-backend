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

import asyncio
import json
import subprocess
import sys
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from prometheus_client import REGISTRY
from sqlalchemy import text

from src.bootstrap.container import OrderCharges, OrderStockHolds
from src.bootstrap.payment_refund import orders_repository_with_payment_guard
from src.bootstrap.saga_recovery import _WorkerBasket
from src.cart.adapters.valkey.repository import ValkeyCartRepository
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.outbox import stock_released_outbox
from src.inventory.application.service import InventoryService
from src.orders.adapters.db.repository import OrdersRepository
from src.orders.application.checkout_saga import CheckoutSaga, payment_key_for
from src.orders.application.service import OrdersService
from src.orders.domain.order import OrderStatus
from src.orders.ports.checkout import ChargeResult, CheckoutLine
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.resilient_gateway import ResilientPaymentGateway
from src.payments.adapters.stub_gateway import DeferredChargeWindow, StubPaymentGateway
from src.payments.application.service import PaymentsService, sign_webhook
from src.shared.errors.exceptions import (
    AuthorizationError,
    CartChangedError,
    CheckoutIdempotencyConflictError,
    InsufficientStockError,
    OrderStateConflictError,
)
from src.shared.resilience import CircuitOpenError, DependencyBudgetExhaustedError

REPO_ROOT = Path(__file__).resolve().parents[2]

USER_A = uuid.uuid4()
USER_B = uuid.uuid4()


class _Basket:
    """In-memory basket keyed by user (the saga's BasketPort, no Valkey)."""

    def __init__(self) -> None:
        self.lines: dict[uuid.UUID, list[CheckoutLine]] = {}
        self.cleared: list[uuid.UUID] = []
        self.consumed: list[tuple[uuid.UUID, tuple[tuple[uuid.UUID, int], ...]]] = []

    def stock(self, user_id: uuid.UUID, *lines: CheckoutLine) -> None:
        self.lines[user_id] = list(lines)

    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        return list(self.lines.get(user_id, []))

    async def clear(self, user_id: uuid.UUID) -> None:
        self.lines.pop(user_id, None)
        self.cleared.append(user_id)

    async def consume(self, user_id: uuid.UUID, lines: list[CheckoutLine]) -> None:
        """Subtract the purchased quantities (mirrors the Valkey consume script)."""
        purchased = Counter()
        for line in lines:
            purchased[line.product_id] += line.quantity
        remaining: list[CheckoutLine] = []
        for line in self.lines.get(user_id, []):
            sold = purchased.pop(line.product_id, 0)
            if line.quantity - sold > 0:
                remaining.append(
                    CheckoutLine(
                        product_id=line.product_id,
                        name=line.name,
                        unit_price=line.unit_price,
                        quantity=line.quantity - sold,
                    )
                )
        self.lines[user_id] = remaining
        self.consumed.append((user_id, tuple((line.product_id, line.quantity) for line in lines)))


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


class _BasketTruth:
    """PriceTruthPort mirroring a ``_Basket``'s live lines — the catalog that
    always stands behind the snapshots, so existing drives pass revalidation."""

    def __init__(self, basket: _Basket) -> None:
        self._basket = basket

    async def current_prices(self, product_ids: list[uuid.UUID]) -> dict[uuid.UUID, Decimal]:
        wanted = set(product_ids)
        return {
            line.product_id: line.unit_price
            for lines in self._basket.lines.values()
            for line in lines
            if line.product_id in wanted
        }


class _FixedTruth:
    """PriceTruthPort over an explicit map — a catalog the test edits at will
    (diverge it from the basket to exercise the cart-changed guard)."""

    def __init__(self, prices: dict[uuid.UUID, Decimal]) -> None:
        self.prices = prices

    async def current_prices(self, product_ids: list[uuid.UUID]) -> dict[uuid.UUID, Decimal]:
        return {pid: self.prices[pid] for pid in product_ids if pid in self.prices}


class _GatedCharges:
    """The saga's ChargePort with an intentionally delayed payment: ``charge``
    parks until the test releases it, holding the saga mid-drive while the test
    mutates the basket — the two-client race, deterministically."""

    def __init__(self) -> None:
        self.in_flight = asyncio.Event()
        self.release = asyncio.Event()

    async def charge(self, **kwargs: Any) -> ChargeResult:
        self.in_flight.set()
        await self.release.wait()
        return ChargeResult(status="succeeded")

    async def find_by_idempotency_key(self, idempotency_key: str) -> ChargeResult | None:
        return None


def _line(product_no: int = 1, qty: int = 1, price: str = "19.99") -> CheckoutLine:
    return CheckoutLine(
        product_id=uuid.uuid5(uuid.NAMESPACE_DNS, f"product-{product_no}"),
        name=f"Product {product_no}",
        unit_price=Decimal(price),
        quantity=qty,
    )


def _saga(
    session,
    basket: _Basket,
    *,
    gateway: StubPaymentGateway | None = None,
    prices: _BasketTruth | _FixedTruth | None = None,
) -> CheckoutSaga:
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(
        PaymentsRepository(session),
        gateway or StubPaymentGateway(),
        webhook_secret="test-secret",
        reconciliation_grace_seconds=30,
        reconciliation_max_age_seconds=604800,
        on_payment_succeeded=OrdersRepository(session).journal_refund_if_cancelled,
    )
    return CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        prices=prices or _BasketTruth(basket),
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


async def _payment_status(session, order_id: uuid.UUID) -> str:
    return (
        await session.execute(text("SELECT status FROM payments.payments WHERE order_id = :id"), {"id": order_id})
    ).scalar_one()


async def _refund_markers(session, order_id: uuid.UUID) -> list[str]:
    """The order's journaled refund markers, oldest first."""
    rows = (
        await session.execute(
            text("SELECT status FROM orders.saga_log WHERE order_id = :id AND step = 'refund' ORDER BY created_at"),
            {"id": order_id},
        )
    ).all()
    return [row.status for row in rows]


async def _unowned_paid(session) -> int:
    """Run the exporter query against the test ledger, not a copy of its SQL."""
    config = yaml.safe_load((REPO_ROOT / "ops/prometheus/postgres-exporter-queries.yaml").read_text())
    query = config["checkout_orphaned_payments"]["query"]
    return (await session.execute(text(query))).scalar_one()


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


async def _payments_outbox(session) -> list[str]:
    rows = (await session.execute(text("SELECT event_type FROM payments.outbox ORDER BY occurred_at"))).all()
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


# --- price revalidation -----------------------------------------


async def test_checkout_rejects_a_price_the_catalog_no_longer_stands_behind(session):
    """The cart snapshot says 19.99 but the merchant already committed 25.00 —
    the ``ProductUpdated`` event is still in flight. The checkout must refuse
    (``CartChangedError`` → 409), never order at the stale price. Raised before
    any order row exists, so the same Idempotency-Key retries cleanly once the
    cart converges."""
    line = _line(price="19.99")
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    truth = _FixedTruth({line.product_id: Decimal("25.00")})  # the edit landed; the event hasn't
    saga = _saga(session, basket, prices=truth)

    before = _counter("checkout_attempts_total", outcome="cart_changed")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-stale-price", payment_token="tok_visa")
    except CartChangedError:
        pass
    else:
        raise AssertionError("expected CartChangedError")

    assert _counter("checkout_attempts_total", outcome="cart_changed") == before + 1
    assert await _orders_count(session) == 0  # no order at the stale price
    assert await _stock(session, str(line.product_id)) == (5, 0)  # nothing reserved

    # The projection catches up (cart + truth agree on 25.00) — the SAME key succeeds.
    converged = CheckoutLine(product_id=line.product_id, name=line.name, unit_price=Decimal("25.00"), quantity=1)
    basket.stock(USER_A, converged)
    order, created = await saga.checkout(user_id=USER_A, idempotency_key="key-stale-price", payment_token="tok_visa")
    assert created is True
    assert order.status == OrderStatus.PAID
    assert order.items[0].unit_price == Decimal("25.00")  # the price the catalog stood behind


async def test_checkout_rejects_a_product_gone_since_the_snapshot(session):
    """``ProductDeleted`` is still in flight: the line's product is already
    soft-deleted in the catalog — absent from the truth map — so the checkout
    refuses instead of ordering a product that no longer exists."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket, prices=_FixedTruth({}))  # the catalog: gone

    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-gone", payment_token="tok_visa")
    except CartChangedError:
        pass
    else:
        raise AssertionError("expected CartChangedError")
    assert await _orders_count(session) == 0
    assert await _stock(session, str(line.product_id)) == (5, 0)


async def test_resumed_checkout_keeps_the_prices_it_was_created_with(session):
    """Revalidation guards order *creation* only: a crashed checkout resumed
    under the same key drives the order's stored lines home even though the
    catalog price moved in between — the price was accepted when the pending
    order was created, and re-checking now would trap the order forever."""
    from src.orders.application.checkout_saga import body_hash_for

    line = _line(price="19.99")
    await _seed(session, str(line.product_id), 5)
    await OrdersRepository(session).create_pending_order(
        user_id=USER_A,
        idempotency_key="key-resume-moved",
        body_hash=body_hash_for("tok_visa"),
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    truth = _FixedTruth({line.product_id: Decimal("25.00")})  # the merchant edited after the crash
    order, created = await _saga(session, _Basket(), prices=truth).checkout(
        user_id=USER_A, idempotency_key="key-resume-moved", payment_token="tok_visa"
    )

    assert created is False  # the row pre-existed: a replay, driven home
    assert order.status == OrderStatus.PAID
    assert order.items[0].unit_price == Decimal("19.99")  # the accepted price stands


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

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0, "refunded": 0, "refund_failed": 0}
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
    assert basket.consumed == [(USER_A, ((line.product_id, 1),))]  # purchased lines removed only on success
    steps = await _saga_steps(session, order.id)
    assert {"create", "reserve", "charge", "commit", "mark_paid"} <= set(steps)  # journal is complete


async def test_order_placed_carries_the_checkout_time_user_email(session):
    """The buyer's address rides the event (an order-row snapshot): the
    notification send path never depends on the user's UserCreated landing
    first. A checkout without one (legacy/tests) omits it — consumers fall
    back to the recipients table."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-email", payment_token="tok_visa", user_email="buyer@example.com"
    )
    payload = json.loads(
        (await session.execute(text("SELECT payload FROM orders.outbox WHERE event_type = 'OrderPlaced'"))).scalar_one()
    )
    assert payload["data"]["user_email"] == "buyer@example.com"

    # No email threaded (the default) → the field is null on the wire.
    basket.stock(USER_A, line)
    await _saga(session, basket).checkout(user_id=USER_A, idempotency_key="key-no-email", payment_token="tok_visa")
    payload = json.loads(
        (
            await session.execute(
                text("SELECT payload FROM orders.outbox WHERE event_type = 'OrderPlaced' ORDER BY occurred_at DESC")
            )
        )
        .first()
        .payload
    )
    assert payload["data"]["user_email"] is None


async def test_concurrent_add_survives_a_successful_checkout(session):
    """Two clients, one delayed payment: while the saga's charge is parked
    mid-drive, a second device adds B to the cart — the paid checkout must
    consume only the purchased A and leave B. (The old unconditional clear
    deleted B: actual data loss, audit P1.)"""
    a, b = _line(product_no=1), _line(product_no=2)
    await _seed(session, str(a.product_id), 5)
    await _seed(session, str(b.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, a)
    charges = _GatedCharges()
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(inventory),
        charges,
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    checkout = asyncio.create_task(
        saga.checkout(user_id=USER_A, idempotency_key="key-race-add", payment_token="tok_visa")
    )
    await charges.in_flight.wait()  # payment deliberately parked mid-saga
    basket.lines[USER_A].append(b)  # the second client's add lands now
    charges.release.set()
    order, created = await checkout

    assert created is True
    assert order.status == OrderStatus.PAID
    assert basket.lines[USER_A] == [b]  # B survived the checkout that never saw it
    assert await _stock(session, str(a.product_id)) == (4, 0)


async def test_concurrent_add_survives_checkout_over_the_real_cart(session, real_valkey):
    """The same race through the real Valkey cart repository: the consume
    script subtracts the purchased line and keeps the line a second client
    added mid-checkout — deleting the whole hash instead would eat it."""
    a, b = _line(product_no=1), _line(product_no=2)
    await _seed(session, str(a.product_id), 5)
    await _seed(session, str(b.product_id), 5)
    cart = ValkeyCartRepository(real_valkey, ttl_seconds=300)
    await cart.add_item(
        USER_A,
        product_id=a.product_id,
        name=a.name,
        unit_price=str(a.unit_price),
        image_url=None,
        quantity=1,
        max_items=10,
        max_per_line=10,
    )
    charges = _GatedCharges()
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    saga = CheckoutSaga(
        OrdersRepository(session),
        _WorkerBasket(real_valkey, ttl_seconds=300),
        OrderStockHolds(inventory),
        charges,
        _Idempotency(),
        prices=_FixedTruth({a.product_id: a.unit_price, b.product_id: b.unit_price}),
        step_timeout_seconds=60,
    )

    checkout = asyncio.create_task(
        saga.checkout(user_id=USER_A, idempotency_key="key-race-real", payment_token="tok_visa")
    )
    await charges.in_flight.wait()
    await cart.add_item(  # the second client's add, mid-checkout
        USER_A,
        product_id=b.product_id,
        name=b.name,
        unit_price=str(b.unit_price),
        image_url=None,
        quantity=2,
        max_items=10,
        max_per_line=10,
    )
    charges.release.set()
    order, created = await checkout

    assert created is True
    assert order.status == OrderStatus.PAID
    surviving = await cart.get_cart(USER_A)
    assert surviving is not None
    assert [(line.product_id, line.quantity) for line in surviving.items] == [(str(b.product_id), 2)]


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
    assert basket.lines.get(USER_A) == []  # the drive consumed the purchased line

    rebuilt = _line(product_no=2)
    basket.stock(USER_A, rebuilt)
    replayed, created = await saga.checkout(user_id=USER_A, idempotency_key="key-rebuilt", payment_token="tok_visa")

    assert created is False
    assert replayed.id == order.id
    assert basket.lines[USER_A] == [rebuilt]  # the rebuilt basket survived
    assert basket.cleared == []  # the replay cleared nothing


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
    assert basket.cleared == [USER_A]  # the replay's mop-up clear — the drive consumes, it never clears
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
    assert basket.lines.get(USER_A) == []  # the drive consumed the purchased line

    rebuilt = _line(product_no=2)
    basket.stock(USER_A, rebuilt)
    replayed, created = await _saga(session, basket).checkout(
        user_id=USER_A, idempotency_key="key-backstop-rebuilt", payment_token="tok_visa"
    )  # fresh fast path: the DB backstop answers

    assert created is False
    assert replayed.id == order.id
    assert basket.lines[USER_A] == [rebuilt]  # the rebuilt basket survived
    assert basket.cleared == []  # the backstop cleared nothing


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
    assert not basket.cleared and not basket.consumed  # a cancelled checkout keeps its cart


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
        prices=_BasketTruth(basket),
        # One timeout for every step — including the reserve step's own DB
        # commits. 50 ms raced them on slow CI runners: cancelling an in-flight
        # asyncpg statement poisoned the session and the drive died with
        # PendingRollbackError instead of the charge timeout under test.
        # 0.5 s gives the reserve 10× margin while the hanging charge still
        # blows straight past it (sleeps 30 s).
        step_timeout_seconds=0.5,
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
        prices=_BasketTruth(basket),
        # Same margin as the unknown-outcome test above: the timeout covers the
        # reserve step's and the payment row's own DB commits too — 50 ms raced
        # them on slow CI runners and poisoned the session mid-statement.
        step_timeout_seconds=0.5,
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


@pytest.mark.parametrize("lookup_fails", [False, True])
async def test_transport_timeout_after_capture_keeps_order_pending(session, lookup_fails):
    """The resilience wrapper translates a transport timeout into a 503 after capture."""

    class CapturedButTimedOut(StubPaymentGateway):
        async def charge(self, **kwargs):
            await super().charge(**kwargs)
            raise TimeoutError("response lost after capture")

    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    gateway = CapturedButTimedOut()
    resilient = ResilientPaymentGateway(gateway, max_attempts=1)
    payments = PaymentsService(PaymentsRepository(session), resilient)

    class Charges(OrderCharges):
        async def find_by_idempotency_key(self, key: str) -> ChargeResult | None:
            if lookup_fails:
                raise RuntimeError("lookup unavailable")
            return await super().find_by_idempotency_key(key)

    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        OrderStockHolds(InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)),
        Charges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )
    with pytest.raises(OrderStateConflictError, match="outcome unknown"):
        await saga.checkout(user_id=USER_A, idempotency_key="key-transport-timeout", payment_token="tok_visa")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"
    assert await _payment_status(session, order_id) == "pending"
    assert await _stock(session, str(line.product_id)) == (5, 1)
    assert (await OrdersRepository(session).latest_saga_step(order_id, "charge")) == "unknown"


@pytest.mark.parametrize("error", [CircuitOpenError, DependencyBudgetExhaustedError])
async def test_pre_provider_shed_still_compensates(session, error):
    class ShedGateway(StubPaymentGateway):
        async def charge(self, **kwargs):
            raise error("provider never called")

    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    saga = _saga(session, basket, gateway=ShedGateway())
    with pytest.raises(error):
        await saga.checkout(user_id=USER_A, idempotency_key="key-shed", payment_token="tok_visa")
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (5, 0)


@pytest.mark.parametrize("confirmation", ["reconcile", "webhook"])
async def test_late_capture_on_cancelled_order_journals_refund(session, confirmation):
    """A succeeded transition and its refund intent commit together on a dead order."""

    class CapturedButTimedOut(StubPaymentGateway):
        async def charge(self, **kwargs):
            await super().charge(**kwargs)
            raise TimeoutError("response lost after capture")

    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    gateway = CapturedButTimedOut()
    saga = _saga(session, basket, gateway=ResilientPaymentGateway(gateway, max_attempts=1))
    with pytest.raises(OrderStateConflictError, match="outcome unknown"):
        await saga.checkout(user_id=USER_A, idempotency_key="key-late-capture", payment_token="tok_visa")
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    await OrdersRepository(session).transition_status(
        order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED
    )
    key = payment_key_for(USER_A, "key-late-capture")
    payments = PaymentsService(
        PaymentsRepository(session),
        gateway,
        webhook_secret="test-secret",
        reconciliation_grace_seconds=30,
        on_payment_succeeded=OrdersRepository(session).journal_refund_if_cancelled,
    )
    if confirmation == "reconcile":
        await session.execute(
            text("UPDATE payments.payments SET created_at = now() - interval '60 seconds' WHERE order_id = :id"),
            {"id": order_id},
        )
        await session.commit()

        async def failed_journal(order_id: uuid.UUID) -> None:
            raise RuntimeError("journal unavailable")

        interrupted = PaymentsService(
            PaymentsRepository(session),
            gateway,
            reconciliation_grace_seconds=30,
            on_payment_succeeded=failed_journal,
        )
        with pytest.raises(RuntimeError, match="journal unavailable"):
            await interrupted.reconcile(batch_size=10)
        assert await _payment_status(session, order_id) == "pending"
        assert await _refund_markers(session, order_id) == []
        assert "PaymentSucceeded" not in await _payments_outbox(session)
        assert await payments.reconcile(batch_size=10) == 1
    else:
        captured = await gateway.lookup(key)
        assert captured is not None
        body = json.dumps({"type": "payment.succeeded", "idempotency_key": key, "gateway_ref": captured.ref}).encode()
        timestamp = int(datetime.now(UTC).timestamp())
        assert await payments.handle_webhook(
            body, sign_webhook(body, "test-secret", timestamp=timestamp), str(timestamp)
        )
    assert await _payment_status(session, order_id) == "succeeded"
    assert await _refund_markers(session, order_id) == ["requested"]
    assert await _unowned_paid(session) == 0
    assert "PaymentSucceeded" in await _payments_outbox(session)

    recovery = _saga(session, basket, gateway=gateway)
    outcome = await recovery.recover_stuck(cutoff=datetime.now(UTC) + timedelta(seconds=1), batch_size=10)
    assert outcome["refunded"] == 1
    assert await _payment_status(session, order_id) == "refunded"
    assert await _refund_markers(session, order_id) == ["requested", "completed"]
    assert await _unowned_paid(session) == 0


async def test_payment_confirmation_before_cancel_lock_journals_refund(session, sessionmaker_factory):
    """The payment hook sees pending, then cancellation waits for its order lock."""
    line = _line()
    order, _ = await OrdersRepository(session).create_pending_order(
        user_id=USER_A,
        idempotency_key="key-confirm-before-cancel",
        body_hash="test",
        total=line.unit_price,
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    key = payment_key_for(USER_A, order.idempotency_key)
    gateway = StubPaymentGateway()
    await PaymentsRepository(session).create_pending(order_id=order.id, idempotency_key=key, amount=line.unit_price)
    captured = await gateway.charge(amount=line.unit_price, idempotency_key=key, payment_method_token="tok_visa")
    locked = asyncio.Event()
    release = asyncio.Event()
    cancelling_started = asyncio.Event()

    class PausedOrderHook(OrdersRepository):
        async def journal_refund_if_cancelled(self, order_id: uuid.UUID) -> None:
            await super().journal_refund_if_cancelled(order_id)
            locked.set()
            await release.wait()

    async def confirm() -> bool:
        async with sessionmaker_factory() as confirming_session:
            service = PaymentsService(
                PaymentsRepository(confirming_session),
                webhook_secret="test-secret",
                on_payment_succeeded=PausedOrderHook(confirming_session).journal_refund_if_cancelled,
            )
            body = json.dumps(
                {"type": "payment.succeeded", "idempotency_key": key, "gateway_ref": captured.ref}
            ).encode()
            timestamp = int(datetime.now(UTC).timestamp())
            return await service.handle_webhook(
                body, sign_webhook(body, "test-secret", timestamp=timestamp), str(timestamp)
            )

    async def cancel() -> None:
        async with sessionmaker_factory() as cancelling_session:
            cancelling_started.set()
            result = await orders_repository_with_payment_guard(cancelling_session).transition_status(
                order.id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED
            )
            assert result is not None

    confirming = asyncio.create_task(confirm())
    cancelling: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(locked.wait(), timeout=10)
        cancelling = asyncio.create_task(cancel())
        try:
            await asyncio.wait_for(cancelling_started.wait(), timeout=10)
            await asyncio.sleep(0.05)
            assert not cancelling.done()  # waiting on the payment hook's order lock
        finally:
            release.set()
        assert await asyncio.wait_for(confirming, timeout=10)
        await asyncio.wait_for(cancelling, timeout=10)
    finally:
        release.set()
        for task in (confirming, cancelling):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    assert await _order_status(session, order.id) == "cancelled"
    assert await _payment_status(session, order.id) == "succeeded"
    assert await _refund_markers(session, order.id) == ["requested"]
    assert await _unowned_paid(session) == 0
    recovered = await _saga(session, _Basket(), gateway=gateway).recover_stuck(
        cutoff=datetime.now(UTC) + timedelta(seconds=1), batch_size=10
    )
    assert recovered["refunded"] == 1
    assert await _payment_status(session, order.id) == "refunded"


async def test_failed_cancellation_refund_check_rolls_back_order(session):
    order, _ = await OrdersRepository(session).create_pending_order(
        user_id=USER_A, idempotency_key="key-cancel-check-failed", body_hash="test", total=Decimal("1"), lines=[]
    )
    order_id = order.id

    async def unavailable(order_id: uuid.UUID) -> bool:
        raise RuntimeError("payment ledger unavailable")

    with pytest.raises(RuntimeError, match="payment ledger unavailable"):
        await OrdersRepository(session, has_succeeded_payment=unavailable).transition_status(
            order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED
        )
    assert await _order_status(session, order_id) == "pending"


async def test_processing_payment_leaves_pending_without_compensating(session, real_valkey):
    """the stub accepts the charge but answers ``pending``
    (processing). The saga must refuse to unwind — the gateway may still settle
    it — so the order stays pending with its holds, exactly like the timeout
    arm: the reconciler/recovery poller own it from here."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    gateway = StubPaymentGateway(
        "decline",
        pending_token_substring="pending",
        pending_settle_seconds=3600,  # undecided for the whole test
        deferred_window=DeferredChargeWindow(real_valkey, settle_seconds=3600),
    )
    saga = _saga(session, basket, gateway=gateway)

    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-processing", payment_token="tok_pending_demo")
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
        prices=_BasketTruth(basket),
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


async def test_cancel_wins_after_payment_refunds_automatically(session):
    """The cancel flips the order ``cancelled`` after the charge succeeded: the
    saga's ``mark_paid`` flip loses (``None``), and the money is **refunded
    automatically** — the payment row lands ``refunded`` and ``PaymentRefunded``
    rides the outbox, so no manual reconciliation is needed. The caller still
    gets the 409 (their checkout did not become an order)."""
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
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-cancel-wins", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "refunded" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    assert _counter("checkout_orphaned_paid_payments_total") == before  # the refund succeeded: not an orphan
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    payment_status = (
        await session.execute(text("SELECT status FROM payments.payments WHERE order_id = :id"), {"id": order_id})
    ).scalar_one()
    assert payment_status == "refunded"  # money returned — no orphan pair
    assert await _payments_outbox(session) == ["PaymentSucceeded", "PaymentRefunded"]  # announced on the bus


async def test_cancel_wins_after_payment_counts_an_orphan_when_the_refund_fails(session):
    """The exceptional arm: the cancel wins after the charge succeeded **and**
    the refund leg fails — the orphan pair survives (money taken, order
    cancelled) and the counter increments, keeping manual reconciliation
    alertable for exactly the case the auto-refund cannot resolve."""
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
                await super().transition_status(order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
                return None
            return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)

    class _RefundRefusingCharges(OrderCharges):
        """The provider refund leg fails (gateway outage, unknown charge)."""

        def __init__(self, payments: PaymentsService) -> None:
            super().__init__(payments)
            self.refund_calls = 0

        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            self.refund_calls += 1
            return False

    saga = CheckoutSaga(
        _CancelWinsAfterCharge(session),
        basket,
        OrderStockHolds(inventory),
        charges := _RefundRefusingCharges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-cancel-wins", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "reconciled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    assert charges.refund_calls == 1  # the refusal answers through the refund leg, not the pre-fix no-call shape
    assert _counter("checkout_orphaned_paid_payments_total") == before + 1
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    payment_status = (
        await session.execute(text("SELECT status FROM payments.payments WHERE order_id = :id"), {"id": order_id})
    ).scalar_one()
    assert payment_status == "succeeded"  # the orphan pair: money taken, order cancelled
    # No refund event either: an announcement without the flip would be a lie.
    assert await _payments_outbox(session) == ["PaymentSucceeded"]


async def test_cancel_racing_a_parked_payment_refunds_not_orphans(session, sessionmaker_factory):
    """The barrier race from the bug report, driven by the real actors on
    separate sessions (one per concurrent task, per the repo's concurrency-test
    rule). The interleave is forced with events, mirroring the F3 guard's real
    check-then-act window: the drive parks just before its ``charge: started``
    journal row; the canceller's guard reads the still-quiet journal (the race
    window) and passes; the drive journals the charge and parks inside the
    provider call; the cancel's guarded ``pending → cancelled`` flip wins and
    its hold release runs while the provider holds the money. The charge then
    returns ``succeeded`` into a cancelled order: the drive cannot commit the
    released holds (shortfall) and cannot pay, and because the order is terminal
    no poller will ever claim it — so the drive refunds the charge itself.
    The end state has no orphan pair and nothing for the poller."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    before = _counter("checkout_orphaned_paid_payments_total")
    order_created = asyncio.Event()
    guard_read_done = asyncio.Event()
    charge_parked = asyncio.Event()
    flip_landed = asyncio.Event()

    class _DriveRepo(OrdersRepository):
        """The drive's repository: parks the ``charge: started`` journal write
        until the canceller's guard has read a quiet journal (the race window),
        and signals once the order row exists."""

        async def create_pending_order(self, *args: Any, **kwargs: Any):
            order, created = await super().create_pending_order(*args, **kwargs)
            order_created.set()
            return order, created

        async def log_saga_step(self, order_id, step, status):
            if step == "charge" and status == "started":
                await guard_read_done.wait()
            await super().log_saga_step(order_id, step, status)

    class _CancelRepo(OrdersRepository):
        """The canceller's repository: its guard read is the race's winner
        (quiet journal → allowed), but the flip itself waits for the charge to
        be parked in-flight — cancel lands *during* the provider call."""

        async def latest_saga_step(self, order_id, step):
            state = await super().latest_saga_step(order_id, step)
            if step == "charge" and state is None:
                guard_read_done.set()
            return state

        async def transition_status(self, order_id, *, expect, to_status, outbox=None):
            if to_status == OrderStatus.CANCELLED:
                await charge_parked.wait()
            return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)

    class _BarrierCharges(OrderCharges):
        """Parks the saga while the provider holds the charge; the money lands
        only after the cancel has fully won (flip + release)."""

        async def charge(self, **kwargs: Any) -> ChargeResult:
            charge_parked.set()
            await flip_landed.wait()
            return await super().charge(**kwargs)

    async with sessionmaker_factory() as drive_session, sessionmaker_factory() as cancel_session:
        drive_inventory = InventoryService(InventoryRepository(drive_session), reservation_ttl_seconds=900)
        drive_payments = PaymentsService(
            PaymentsRepository(drive_session), StubPaymentGateway(), webhook_secret="test-secret"
        )
        saga = CheckoutSaga(
            _DriveRepo(drive_session),
            basket,
            OrderStockHolds(drive_inventory),
            _BarrierCharges(drive_payments),
            _Idempotency(),
            prices=_BasketTruth(basket),
            step_timeout_seconds=60,
        )
        drive = asyncio.create_task(
            saga.checkout(user_id=USER_A, idempotency_key="key-barrier-race", payment_token="tok_visa")
        )
        await order_created.wait()  # the order row is committed; the cancel may look it up

        cancel_inventory = InventoryService(InventoryRepository(cancel_session), reservation_ttl_seconds=900)
        orders_service = OrdersService(_CancelRepo(cancel_session), OrderStockHolds(cancel_inventory))
        cancel = asyncio.create_task(
            orders_service.cancel_order(
                user_id=USER_A,
                is_admin=False,
                order_id=((await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()),
            )
        )
        await charge_parked.wait()  # the provider holds the charge; the cancel's flip is unblocked
        cancelled = await cancel
        assert cancelled is not None and cancelled.status == OrderStatus.CANCELLED

        # The charge now succeeds into the cancelled order; the drive cannot
        # commit the released holds (shortfall) and must not pay. A cancel left
        # the order terminal, so no poller will ever claim it: the drive has to
        # reverse the charge itself, and it answers 409 saying so.
        flip_landed.set()
        with pytest.raises(OrderStateConflictError) as drive_exc:
            await drive
        assert "refunded" in drive_exc.value.detail

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"  # the canceller won the race
    assert await _stock(session, str(line.product_id)) == (5, 0)  # the cancel put the unit back
    payment_status = (
        await session.execute(text("SELECT status FROM payments.payments WHERE order_id = :id"), {"id": order_id})
    ).scalar_one()
    assert payment_status == "refunded"  # money out and back — no orphan pair survives the race

    # Nothing is left for the poller, and no pair for manual reconciliation.
    assert _counter("checkout_orphaned_paid_payments_total") == before
    assert await _payments_outbox(session) == ["PaymentSucceeded", "PaymentRefunded"]
    assert await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    ) == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}


class _CancelWinsAfterCharge(OrdersRepository):
    """A user-facing cancel wins the guarded flip between the saga's commit and
    its ``mark_paid``; the saga's own ``pending → paid`` flip then loses
    (``None``) — the terminal-order frame the refund arm runs in."""

    async def transition_status(self, order_id, *, expect, to_status, outbox=None):
        if to_status == OrderStatus.PAID:
            # The canceller's flip landed first — perform it as
            # OrdersService.cancel_order would, then lose our own.
            await super().transition_status(order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
            return None
        return await super().transition_status(order_id, expect=expect, to_status=to_status, outbox=outbox)


async def test_drive_refund_raise_journals_intent_and_the_poller_retries(session):
    """The cancel wins after the charge landed **and** the provider
    refund raises (retries exhausted / open breaker). The drive journals
    ``refund: requested`` before the attempt, so the raise answers 409 "refund
    in progress" — no orphan — and the recovery poller's refund claim retries
    the pair to ``refunded``: a transient provider fault on a terminal order
    never becomes manual reconciliation."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _FlakyRefundCharges(OrderCharges):
        """The first refund attempt raises (provider down); later ones run for real."""

        def __init__(self, payments: PaymentsService) -> None:
            super().__init__(payments)
            self.refund_calls = 0

        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            self.refund_calls += 1
            if self.refund_calls == 1:
                raise RuntimeError("refund provider unavailable")
            return await super().refund(idempotency_key=idempotency_key, reason=reason)

    saga = CheckoutSaga(
        _CancelWinsAfterCharge(session),
        basket,
        OrderStockHolds(inventory),
        _FlakyRefundCharges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-refund-retry", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "refund is in progress" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "cancelled"
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before  # a transient raise is not an orphan
    assert await _refund_markers(session, order_id) == ["requested"]  # the durable intent the poller reconciles from
    assert await _payment_status(session, order_id) == "succeeded"  # money still out — for now

    await _backdate_pending(session, order_id)
    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 1, "refund_failed": 0}
    assert await _payment_status(session, order_id) == "refunded"  # the retry returned the money
    assert await _refund_markers(session, order_id) == ["requested", "completed"]
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before  # closed automatically, never an orphan
    assert await _payments_outbox(session) == ["PaymentSucceeded", "PaymentRefunded"]


async def test_drive_refund_refusal_is_terminal_and_never_retried(session):
    """The provider's definitive **no** stays human-owned: the drive journals
    ``refund: refused`` (terminal), counts the orphan once, and the poller's
    refund claim must not re-ask a provider that already refused — no retry,
    no second orphan increment."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _RefundRefusingCharges(OrderCharges):
        def __init__(self, payments: PaymentsService) -> None:
            super().__init__(payments)
            self.refund_calls = 0

        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            self.refund_calls += 1
            return False

    charges = _RefundRefusingCharges(payments)
    saga = CheckoutSaga(
        _CancelWinsAfterCharge(session),
        basket,
        OrderStockHolds(inventory),
        charges,
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-refund-refused", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "reconciled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert charges.refund_calls == 1
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1
    assert await _refund_markers(session, order_id) == ["requested", "refused"]  # closed terminally
    assert await _payment_status(session, order_id) == "succeeded"  # the orphan pair, for RUNBOOK §9
    assert await _unowned_paid(session) == 1

    await _backdate_pending(session, order_id)
    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    # The terminal marker keeps the poller out: nothing claimed, nothing retried,
    # nothing re-counted.
    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert charges.refund_calls == 1
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1


async def test_drive_refund_raise_without_journaled_intent_counts_an_orphan(session):
    """The degraded arm: the refund raises **and** even the intent marker cannot
    be journaled (the DB refused the write) — nothing will retry the pair, so
    the honest answer is the orphan count and the "reconciled" 409."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _UnjournaledRefunds(_CancelWinsAfterCharge):
        """Every refund journal write fails before touching the session."""

        def __init__(self, session) -> None:
            super().__init__(session)
            self.refund_journal_attempts: list[str] = []

        async def log_saga_step(self, order_id, step, status):
            if step == "refund":
                self.refund_journal_attempts.append(status)
                raise RuntimeError("journal write failed")
            return await super().log_saga_step(order_id, step, status)

    class _RaisingRefundCharges(OrderCharges):
        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            raise RuntimeError("refund provider unavailable")

    repo = _UnjournaledRefunds(session)
    saga = CheckoutSaga(
        repo,
        basket,
        OrderStockHolds(inventory),
        _RaisingRefundCharges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-refund-unjournaled", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "reconciled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    # The intent was *attempted* and lost —
    # which is exactly why this arm must count the orphan: nothing retries it.
    assert repo.refund_journal_attempts == ["requested"]
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1  # no marker → no retry → orphan
    assert await _refund_markers(session, order_id) == []  # the intent never landed
    assert await _payment_status(session, order_id) == "succeeded"


async def test_drive_refusal_with_lost_terminal_marker_defers_the_count_to_the_poller(session):
    """The count rides the terminal marker: the provider refuses,
    the intent is journaled, but the ``refused`` marker write fails — the drive
    must NOT count the orphan (the open ``requested`` keeps the pair
    claimable), and the poller's retry refuses again, journals the marker, and
    counts it — exactly once across both frames."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _RefusedMarkerLost(_CancelWinsAfterCharge):
        """Only the terminal ``refused`` marker write fails (the intent lands)."""

        async def log_saga_step(self, order_id, step, status):
            if step == "refund" and status == "refused":
                raise RuntimeError("journal write failed")
            return await super().log_saga_step(order_id, step, status)

    class _RefundRefusingCharges(OrderCharges):
        def __init__(self, payments: PaymentsService) -> None:
            super().__init__(payments)
            self.refund_calls = 0

        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            self.refund_calls += 1
            return False

    saga = CheckoutSaga(
        _RefusedMarkerLost(session),
        basket,
        OrderStockHolds(inventory),
        _RefundRefusingCharges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )

    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-refund-count-rides-marker", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "reconciled" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")

    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    # Not counted here: the terminal marker never landed, so counting would
    # double with the poller's retry. The pair stays claimable instead.
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before
    assert await _refund_markers(session, order_id) == ["requested"]

    # The poller's retry (healthy journal, still-refusing provider) refuses
    # again, lands the terminal marker, and counts — once, total.
    payments2 = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")
    recovery = CheckoutSaga(
        OrdersRepository(session),
        _Basket(),
        OrderStockHolds(inventory),
        _RefundRefusingCharges(payments2),
        _Idempotency(),
        prices=_BasketTruth(_Basket()),
        step_timeout_seconds=60,
    )
    await _backdate_pending(session, order_id)
    outcome = await recovery.recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 1}
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1  # exactly once across both frames
    assert await _refund_markers(session, order_id) == ["requested", "refused"]
    assert await _payment_status(session, order_id) == "succeeded"  # the orphan pair, human-owned


async def test_poller_retries_a_journaled_refund_to_completion(session):
    """The refund-retry claim itself: a cancelled order whose drive journaled
    ``refund: requested`` and then crashed before the provider answered is
    claimed by the poller, refunded under the charge's own key, and closed
    ``completed`` — with ``PaymentRefunded`` riding the outbox."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-refund-claim",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await repo.transition_status(order.id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
    payments_repo = PaymentsRepository(session)
    payment, _ = await payments_repo.create_pending(
        order_id=order.id, idempotency_key=payment_key_for(USER_A, "key-refund-claim"), amount=Decimal("19.99")
    )
    await payments_repo.transition(payment.id, to_status="succeeded", gateway_ref="stub_charge_ref")
    await repo.log_saga_step(order.id, "refund", "requested")  # the drive's durable intent, then the crash
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 1, "refund_failed": 0}
    assert await _payment_status(session, order.id) == "refunded"
    assert await _refund_markers(session, order.id) == ["requested", "completed"]
    assert await _payments_outbox(session) == ["PaymentRefunded"]


async def test_poller_closes_the_marker_when_the_refund_already_landed(session):
    """The crash window between the provider's yes and the `completed` marker:
    the row is already ``refunded``, so the retry must not refund again — it
    just closes the journal (the payments service short-circuits a
    not-``succeeded`` row, and the stub would dedupe the key anyway)."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-refund-landed",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await repo.transition_status(order.id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
    payments_repo = PaymentsRepository(session)
    payment, _ = await payments_repo.create_pending(
        order_id=order.id, idempotency_key=payment_key_for(USER_A, "key-refund-landed"), amount=Decimal("19.99")
    )
    await payments_repo.transition(payment.id, to_status="succeeded", gateway_ref="stub_charge_ref")
    await payments_repo.transition(payment.id, to_status="refunded", gateway_ref="stub_charge_ref", expect="succeeded")
    await repo.log_saga_step(order.id, "refund", "requested")  # still open: the `completed` write was lost
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 1, "refund_failed": 0}
    assert await _payment_status(session, order.id) == "refunded"  # unchanged — no second refund
    assert await _refund_markers(session, order.id) == ["requested", "completed"]
    assert await _payments_outbox(session) == []  # the refund leg did not re-run, so no duplicate event


async def test_poller_refund_retry_refusal_is_counted_once_and_closed(session):
    """The retry meeting a definitive refusal: the marker closes ``refused``,
    the orphan counter increments exactly once, and the next pass finds a
    terminal marker — no re-ask, no re-count."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-refund-refused-poll",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await repo.transition_status(order.id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
    payments_repo = PaymentsRepository(session)
    payment, _ = await payments_repo.create_pending(
        order_id=order.id, idempotency_key=payment_key_for(USER_A, "key-refund-refused-poll"), amount=Decimal("19.99")
    )
    await payments_repo.transition(payment.id, to_status="succeeded", gateway_ref="stub_charge_ref")
    await repo.log_saga_step(order.id, "refund", "requested")
    await _backdate_pending(session, order.id)

    class _RefusingCharges(OrderCharges):
        def __init__(self, payments: PaymentsService) -> None:
            super().__init__(payments)
            self.refund_calls = 0

        async def refund(self, *, idempotency_key: str, reason: str) -> bool:
            self.refund_calls += 1
            return False

    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")
    charges = _RefusingCharges(payments)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    saga = CheckoutSaga(
        OrdersRepository(session),
        _Basket(),
        OrderStockHolds(inventory),
        charges,
        _Idempotency(),
        prices=_BasketTruth(_Basket()),
        step_timeout_seconds=60,
    )

    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    failed_before = _counter("checkout_recovery_total", outcome="refund_failed")
    outcome = await saga.recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 1}
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1
    assert _counter("checkout_recovery_total", outcome="refund_failed") == failed_before + 1
    assert await _refund_markers(session, order.id) == ["requested", "refused"]
    assert await _payment_status(session, order.id) == "succeeded"  # the orphan pair survives for RUNBOOK §9

    # The terminal marker closes the claim: a second pass asks nothing.
    outcome = await saga.recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)
    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert charges.refund_calls == 1
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before + 1


async def test_recovery_settles_a_refunded_payment_by_compensating(session):
    """A payment already ``refunded`` on a still-``pending`` order (a previous
    pass's paid-without-consume refund, or the live drive's orphan arm) has no
    money standing behind it: recovery must compensate (release + cancel), not
    pay and not defer forever."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-refunded-recovery",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)
    # A succeeded charge that a prior refund leg already reversed:
    payments_repo = PaymentsRepository(session)
    payment, _ = await payments_repo.create_pending(
        order_id=order.id, idempotency_key=payment_key_for(USER_A, "key-refunded-recovery"), amount=Decimal("19.99")
    )
    await payments_repo.transition(payment.id, to_status="succeeded", gateway_ref="stub_refunded_leg")
    await payments_repo.transition(
        payment.id, to_status="refunded", gateway_ref="stub_refunded_leg", expect="succeeded"
    )
    await _backdate_pending(session, order.id)

    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert await _order_status(session, order.id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (5, 0)  # hold released, not re-deducted


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
        prices=_BasketTruth(basket),
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
    assert outcome == {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
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
        prices=_FixedTruth({}),
        step_timeout_seconds=60,
    ).recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1, "refunded": 0, "refund_failed": 0}
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
        prices=_FixedTruth({}),
        step_timeout_seconds=60,
    ).recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 1, "refunded": 0, "refund_failed": 0}
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

    assert outcome == {"completed": 1, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
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

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert await _order_status(session, order.id) == "cancelled"
    assert await _stock(session, str(line.product_id)) == (5, 0)


async def test_recovery_compensates_a_paid_order_whose_holds_were_reaped(session):
    """P0 regression (paid-without-consume): holds expire + the reaper releases
    them, then the payment confirms days later — the poller must NOT mark the
    order paid (its stock was already sold back into the pool). It reverses the
    money first (durable + idempotent refund), then compensates: no
    orphan pair reaches manual reconciliation for a routine reaper race."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-reaped-confirm",
        body_hash="hash",
        total=Decimal("19.99"),
        lines=[(line.product_id, line.name, line.unit_price, line.quantity)],
    )
    await inventory.reserve(str(line.product_id), line.quantity, order.id)

    # The drive stalls past the reservation TTL: the reaper gives the unit back.
    await session.execute(text("UPDATE inventory.reservations SET expires_at = now() - interval '1 hour'"))
    released = await InventoryRepository(session).release_expired(batch_size=10, outbox_factory=stock_released_outbox)
    assert released == 1
    assert await _stock(session, str(line.product_id)) == (5, 0)  # unit is back in the pool

    # Days later the (unknown-outcome) payment confirms — order still pending.
    payment = await payments.charge(
        order_id=order.id,
        idempotency_key=payment_key_for(USER_A, "key-reaped-confirm"),
        amount=Decimal("19.99"),
        payment_method_token="tok_visa",
    )
    assert payment.status == "succeeded"
    await _backdate_pending(session, order.id)

    before = _counter("checkout_paid_without_consume_total")
    orphan_before = _counter("checkout_orphaned_paid_payments_total")
    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert _counter("checkout_paid_without_consume_total") == before + 1
    assert _counter("checkout_orphaned_paid_payments_total") == orphan_before  # refunded: not an orphan
    assert await _order_status(session, order.id) == "cancelled"  # never paid
    assert await _stock(session, str(line.product_id)) == (5, 0)  # no second decrement
    pair = (
        await session.execute(
            text(
                "SELECT o.status, p.status FROM orders.orders o "
                "JOIN payments.payments p ON p.order_id = o.id WHERE o.id = :id"
            ),
            {"id": order.id},
        )
    ).one()
    assert pair == ("cancelled", "refunded")  # money out and back — nothing left to reconcile
    assert await _payments_outbox(session) == ["PaymentSucceeded", "PaymentRefunded"]


async def test_recovery_compensates_when_only_some_holds_survive_until_confirm(session):
    """Partial variant: of two lines, one hold was reaped before the payment
    confirmed. Committing the surviving line alone must not pay the order —
    a partially-consumed paid order would be the same invariant hole."""
    line_a = _line(product_no=1)
    line_b = _line(product_no=2)
    await _seed(session, str(line_a.product_id), 5)
    await _seed(session, str(line_b.product_id), 5)
    repo = OrdersRepository(session)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    order, _ = await repo.create_pending_order(
        user_id=USER_A,
        idempotency_key="key-partial-reaped",
        body_hash="hash",
        total=Decimal("39.98"),
        lines=[
            (line_a.product_id, line_a.name, line_a.unit_price, line_a.quantity),
            (line_b.product_id, line_b.name, line_b.unit_price, line_b.quantity),
        ],
    )
    await inventory.reserve(str(line_a.product_id), line_a.quantity, order.id)
    await inventory.reserve(str(line_b.product_id), line_b.quantity, order.id)

    # Only line A's hold expires; the reaper releases it. Line B is still held.
    await session.execute(
        text("UPDATE inventory.reservations SET expires_at = now() - interval '1 hour' WHERE sku = :sku"),
        {"sku": str(line_a.product_id)},
    )
    released = await InventoryRepository(session).release_expired(batch_size=10, outbox_factory=stock_released_outbox)
    assert released == 1

    payment = await payments.charge(
        order_id=order.id,
        idempotency_key=payment_key_for(USER_A, "key-partial-reaped"),
        amount=Decimal("39.98"),
        payment_method_token="tok_visa",
    )
    assert payment.status == "succeeded"
    await _backdate_pending(session, order.id)

    before = _counter("checkout_paid_without_consume_total")
    outcome = await _saga(session, _Basket()).recover_stuck(
        cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50
    )

    assert outcome == {"completed": 0, "compensated": 1, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert _counter("checkout_paid_without_consume_total") == before + 1
    assert await _order_status(session, order.id) == "cancelled"
    assert await _stock(session, str(line_a.product_id)) == (5, 0)  # was already in the pool
    assert await _stock(session, str(line_b.product_id)) == (4, 0)  # committed once, not released again


async def test_drive_commit_shortfall_leaves_pending_for_recovery(session):
    """Drive-side guard: a commit that consumed fewer holds than the order has
    lines must not reach mark_paid even on the live path — the order stays
    pending for the recovery poller (never a cancel after money moved)."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)
    payments = PaymentsService(PaymentsRepository(session), StubPaymentGateway(), webhook_secret="test-secret")

    class _VanishedHolds(OrderStockHolds):
        """Reports nothing to consume — the holds were reaped out from under
        the drive between reserve and commit."""

        async def commit_for_order(self, order_id):
            return 0

    saga = CheckoutSaga(
        OrdersRepository(session),
        basket,
        _VanishedHolds(inventory),
        OrderCharges(payments),
        _Idempotency(),
        prices=_BasketTruth(basket),
        step_timeout_seconds=60,
    )
    before = _counter("checkout_paid_without_consume_total")
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-blind-commit", payment_token="tok_visa")
    except OrderStateConflictError as exc:
        assert "settled automatically" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError on commit shortfall")

    assert _counter("checkout_paid_without_consume_total") == before + 1
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"  # left for recovery, not paid
    assert await _stock(session, str(line.product_id)) == (5, 1)  # hold kept, nothing consumed


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

    assert outcome == {"completed": 0, "compensated": 0, "deferred": 1, "refunded": 0, "refund_failed": 0}
    assert await _order_status(session, order.id) == "pending"


async def test_bcr_002_deferred_charge_settles_to_paid_via_the_workers(session, real_valkey):
    """a checkout with the pending
    trigger ends 409 with a real pending order; a reconciler pass (fresh stub
    instance, shared window) resolves the deferred charge; the recovery pass
    settles the order paid — no manual intervention anywhere."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)

    def _gateway() -> StubPaymentGateway:
        # settle_seconds=0: the charge still ANSWERS pending (the answer is
        # decided at return time), while the shared window resolves at once —
        # the same order of events as a short settle delay, without waiting.
        return StubPaymentGateway(
            "decline",
            pending_token_substring="pending",
            pending_settle_seconds=0,
            deferred_window=DeferredChargeWindow(real_valkey, settle_seconds=0),
        )

    saga = _saga(session, basket, gateway=_gateway())
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-bcr002", payment_token="tok_pending_demo")
    except OrderStateConflictError as exc:
        assert "outcome unknown" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"

    # Reconciler pass: age the payment past the grace window, then a fresh
    # PaymentsService over a fresh stub (the worker's own map) resolves it.
    past = datetime.now(UTC) - timedelta(seconds=120)
    await session.execute(text("UPDATE payments.payments SET created_at = :past, updated_at = :past"), {"past": past})
    await session.commit()
    payments = PaymentsService(
        PaymentsRepository(session),
        _gateway(),
        webhook_secret="test-secret",
        reconciliation_grace_seconds=30,
        reconciliation_max_age_seconds=604800,
    )
    assert await payments.reconcile(batch_size=10) == 1
    status = (await session.execute(text("SELECT status FROM payments.payments"))).scalar_one()
    assert status == "succeeded"

    # Recovery pass: the now-succeeded payment settles the order paid, like a
    # crashed-after-payment checkout.
    await _backdate_pending(session, order_id)
    outcome = await saga.recover_stuck(cutoff=datetime.now(UTC) - timedelta(seconds=60), batch_size=50)
    assert outcome == {"completed": 1, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
    assert await _order_status(session, order_id) == "paid"
    assert await _stock(session, str(line.product_id)) == (4, 0)
    assert await _orders_outbox(session) == ["OrderPlaced"]
    assert basket.consumed and basket.lines.get(USER_A) == []  # the demo basket consumed like a live checkout


async def test_bcr_002_retry_with_the_same_key_settles_the_deferred_charge(session, real_valkey):
    """The other documented settling path: retrying the checkout with the same
    Idempotency-Key after the deferred charge resolved drives the saga home —
    the gateway replays its one (succeeded) answer, no workers needed."""
    line = _line()
    await _seed(session, str(line.product_id), 5)
    basket = _Basket()
    basket.stock(USER_A, line)
    gateway = StubPaymentGateway(
        "decline",
        pending_token_substring="pending",
        pending_settle_seconds=0,  # answers pending, resolves immediately
        deferred_window=DeferredChargeWindow(real_valkey, settle_seconds=0),
    )
    saga = _saga(session, basket, gateway=gateway)
    try:
        await saga.checkout(user_id=USER_A, idempotency_key="key-retry-settle", payment_token="tok_pending_demo")
    except OrderStateConflictError as exc:
        assert "outcome unknown" in exc.detail
    else:
        raise AssertionError("expected OrderStateConflictError")
    order_id = (await session.execute(text("SELECT id FROM orders.orders"))).scalar_one()
    assert await _order_status(session, order_id) == "pending"

    response, created = await saga.checkout(
        user_id=USER_A, idempotency_key="key-retry-settle", payment_token="tok_pending_demo"
    )

    assert created is False  # the row pre-existed; this call drove it home
    assert response.status == OrderStatus.PAID
    assert await _order_status(session, order_id) == "paid"
    assert await _stock(session, str(line.product_id)) == (4, 0)  # one hold, committed once
    assert await _orders_outbox(session) == ["OrderPlaced"]
    assert basket.consumed and basket.lines.get(USER_A) == []  # the resumed drive consumed the cart


# --- ownership (orders half) ------------------------------------------


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
