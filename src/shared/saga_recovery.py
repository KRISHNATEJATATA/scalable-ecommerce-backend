"""Saga recovery poller — the `service`-role worker that settles crashed checkouts.
A checkout that dies between steps (process crash, deploy restart) leaves a
`pending` order with holds against it: stock nobody can buy and an order nobody
owns the outcome of. The reaper would eventually release the holds, but the
order itself would sit `pending` forever. This poller closes that window: every
pass claims `pending` orders older than the saga step timeout (``FOR UPDATE
SKIP LOCKED``, so N replicas split the batch) and settles each from its
payment row — commit + mark paid when the charge succeeded, release + cancel
otherwise. Still-`pending` payments are left for the payment reconciler.

Safe to run continuously (as in docker-compose, mirroring the reaper) or as a
scheduled one-shot in prod (EventBridge → ECS task with ``--once``).

This module is a **scheduling shell**: it owns the loop, the signal handling
and the sessions, and delegates settling to
:meth:`~src.orders.application.checkout_saga.CheckoutSaga.recover_stuck`. Like
every other worker it builds repositories only to hand them to services and
ports — Route/Worker → Service → Repository is never short-circuited.

Lives in ``shared`` (not ``orders``) deliberately: settling composes four
modules' services, and the module-independence contract lets only shared code
do that — the same reason the outbox relay lives in ``shared.bus``. The
saga's decision logic stays in ``orders.application``; this is transport.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.cart.adapters.valkey.repository import ValkeyCartRepository
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.orders.adapters.db.repository import OrdersRepository
from src.orders.adapters.idempotency import ValkeyIdempotencyStore
from src.orders.application.checkout_saga import CheckoutSaga
from src.orders.ports.checkout import BasketPort, ChargePort, ChargeResult, CheckoutLine
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.stub_gateway import StubPaymentGateway
from src.payments.application.service import PaymentsService
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class _WorkerBasket(BasketPort):
    """The saga's basket over the Valkey cart repository (no catalog needed).

    Recovery only reads lines to rebuild hashes (which it doesn't need — the
    crashed path never re-presents the token) and clears baskets on success, so
    the product-snapshot port the request path carries is unnecessary here.
    """

    def __init__(self, valkey: object, *, ttl_seconds: int) -> None:
        self._repo = ValkeyCartRepository(valkey, ttl_seconds=ttl_seconds)

    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        cart = await self._repo.get_cart(user_id)
        if cart is None:
            return []
        return [
            CheckoutLine(
                product_id=uuid.UUID(line.product_id),
                name=line.name,
                unit_price=Decimal(line.unit_price),
                quantity=line.quantity,
            )
            for line in cart.items
        ]

    async def clear(self, user_id: uuid.UUID) -> None:
        await self._repo.clear_cart(user_id)


class _WorkerCharges(ChargePort):
    """The saga's charges over the payments service (read-only for recovery)."""

    def __init__(self, payments: PaymentsService) -> None:
        self._payments = payments

    async def charge(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal, payment_token: str
    ) -> ChargeResult:  # pragma: no cover - recovery never charges (no token stored)
        raise RuntimeError("recovery must never charge: the payment token is never stored")

    async def find_by_idempotency_key(self, idempotency_key: str) -> ChargeResult | None:
        payment = await self._payments.get_by_idempotency_key(idempotency_key)
        if payment is None:
            return None
        return ChargeResult(status=payment.status)


class _WorkerHolds:
    """The saga's holds over the inventory service (same calls as the live path)."""

    def __init__(self, inventory: InventoryService) -> None:
        self._inventory = inventory

    async def reserve(self, sku: str, qty: int, order_id: uuid.UUID) -> uuid.UUID:
        return (await self._inventory.reserve(sku, qty, order_id)).id

    async def release_for_order(self, order_id: uuid.UUID) -> int:
        return await self._inventory.release_for_order(order_id)

    async def commit_for_order(self, order_id: uuid.UUID) -> int:
        return await self._inventory.commit_for_order(order_id)


class SagaRecovery:
    """Claims and settles crashed checkouts in batches until stopped."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        valkey: object,
        settings: AppSettings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._valkey = valkey
        self._settings = settings

    def _saga(self, session) -> CheckoutSaga:
        inventory = InventoryService(
            InventoryRepository(session), reservation_ttl_seconds=self._settings.reservation_ttl_seconds
        )
        payments = PaymentsService(
            PaymentsRepository(session),
            StubPaymentGateway(self._settings.payment_stub_fail_token_substring),
            webhook_secret=self._settings.payment_webhook_secret,
            reconciliation_grace_seconds=self._settings.payment_reconciliation_grace_seconds,
            reconciliation_max_age_seconds=self._settings.payment_reconciliation_max_age_seconds,
        )
        return CheckoutSaga(
            OrdersRepository(session),
            _WorkerBasket(self._valkey, ttl_seconds=self._settings.cart_ttl_seconds),
            _WorkerHolds(inventory),
            _WorkerCharges(payments),
            ValkeyIdempotencyStore(self._valkey, ttl_seconds=self._settings.checkout_idempotency_ttl_seconds),
            step_timeout_seconds=self._settings.checkout_saga_step_timeout_seconds,
        )

    async def sweep_once(self) -> dict[str, int]:
        """One pass: settle every stuck checkout, return the outcome counts."""
        async with self._sessionmaker() as session:
            cutoff = datetime.now(UTC) - timedelta(seconds=self._settings.checkout_saga_step_timeout_seconds)
            return await self._saga(session).recover_stuck(
                cutoff=cutoff, batch_size=self._settings.checkout_saga_recovery_batch_size
            )

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle.

        The idle sleep wakes the moment ``stop`` is set, so SIGTERM never waits
        out a full interval (bounded shutdown: finish the current sweep, exit).
        """
        while stop is None or not stop.is_set():
            try:
                settled = await self.sweep_once()
            except Exception:  # boundary: one bad pass must not kill recovery
                log.exception("saga recovery pass failed; retrying after interval")
                settled = {"completed": 0, "compensated": 0, "deferred": 0}
            if sum(settled.values()) == 0:
                if stop is None:
                    await asyncio.sleep(poll_interval)
                    continue
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval)


async def run_recovery(
    settings: AppSettings,
    sessionmaker: async_sessionmaker,
    valkey: object,
    *,
    stop: asyncio.Event | None = None,
    once: bool = False,
) -> dict[str, int]:
    """Build recovery from settings and run it (one sweep with ``once=True``)."""
    recovery = SagaRecovery(sessionmaker, valkey, settings)
    if once:
        return await recovery.sweep_once()
    await recovery.run(settings.checkout_saga_recovery_poll_interval_seconds, stop=stop)
    return {"completed": 0, "compensated": 0, "deferred": 0}


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.shared.saga_recovery [--once]` — the `service`-role saga recovery."""
    from src.shared.clients import valkey_client
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

    parser = argparse.ArgumentParser(description="Settle crashed checkout sagas.")
    parser.add_argument("--once", action="store_true", help="run a single sweep and exit (scheduled-task mode)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    if not args.once:
        serve_worker_metrics(settings, job="saga-recovery")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    valkey = valkey_client.create_client(settings)
    log.info("saga recovery starting (once=%s)", args.once)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_recovery(settings, sessionmaker, valkey, stop=stop, once=args.once)
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(_run())
    if args.once:
        push_worker_metrics(settings, job="saga-recovery")


if __name__ == "__main__":  # pragma: no cover
    main()
