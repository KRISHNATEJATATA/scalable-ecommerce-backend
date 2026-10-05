"""Single factory for the checkout saga — shared by the request path and the recovery worker.

The saga composes four modules' services, and the module-independence contract
lets only the composition root do that — so this module lives in ``bootstrap``.
Both :func:`~src.bootstrap.container.get_checkout_saga` (the request path) and
:meth:`~src.bootstrap.saga_recovery.SagaRecovery._saga` (the recovery worker)
delegate here, so a provider swap or a port-behavior fix reaches both callers.

The adapters are unified over the lowest port that serves every caller:

* basket over :class:`~src.cart.ports.repository.CartRepositoryPort` — the saga
  only reads lines, clears, and consumes purchased quantities, all of which the
  repository answers directly (the ``CartService`` snapshot wrapper the request
  path used to carry adds nothing here);
* holds over :class:`~src.inventory.application.service.InventoryService`;
* charges over :class:`~src.payments.application.service.PaymentsService` (the
  real charge implementation — recovery never charges only because it never
  holds a payment token, so the raising stub was dead weight);
* price truth over the catalog **repository** (never the cache-aside service:
  the service's cache is invalidated by the same asynchronous events whose
  propagation lag this guard exists to catch).
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.bootstrap.payment_gateway import make_payment_gateway
from src.bootstrap.payment_refund import cancelled_order_refund_hook, orders_repository_with_payment_guard
from src.cart.adapters.valkey.repository import ValkeyCartRepository
from src.cart.ports.repository import CartRepositoryPort
from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.ports.repository import CatalogRepositoryPort
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.orders.adapters.idempotency import ValkeyIdempotencyStore
from src.orders.application.checkout_saga import CheckoutSaga
from src.orders.ports.checkout import (
    BasketPort,
    ChargePort,
    ChargeResult,
    CheckoutLine,
    PriceTruthPort,
    StockHoldsPort,
)
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.application.service import PaymentsService
from src.payments.ports.gateway import PaymentGatewayPort
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import DependencyUnavailableError


class SagaBasket(BasketPort):
    """The saga's basket over the cart repository (no catalog needed).

    The cart's price/name snapshots become the order lines verbatim: later
    catalog edits never rewrite order history. Consuming subtracts only the
    purchased quantities, so lines added while the saga ran always survive.
    """

    def __init__(self, repo: CartRepositoryPort) -> None:
        self._repo = repo

    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        """The user's current cart lines as checkout lines (``[]`` when empty)."""
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
        """Empty the basket wholesale (the replay mop-up's exact-match clear)."""
        await self._repo.clear_cart(user_id)

    async def consume(self, user_id: uuid.UUID, lines: list[CheckoutLine]) -> None:
        """Subtract the purchased quantities; concurrent adds always survive."""
        await self._repo.consume_lines(user_id, lines=[(line.product_id, line.quantity) for line in lines])


class SagaStockHolds(StockHoldsPort):
    """The saga's stock holds over the inventory service.

    SKU mapping ``str(product.id)`` is the composition seam. Built without the
    ownership gate: the saga never upserts stock (the only path that consults
    ownership), so wiring catalog into the worker would buy nothing.
    """

    def __init__(self, inventory: InventoryService) -> None:
        self._inventory = inventory

    async def reserve_many(self, lines: list[tuple[str, int]], order_id: uuid.UUID) -> None:
        """Hold every ``(sku, qty)`` line for ``order_id`` in one all-or-nothing transaction."""
        await self._inventory.reserve_many(lines, order_id)

    async def release_for_order(self, order_id: uuid.UUID) -> int:
        """Release every still-held reservation of one order (compensation)."""
        return await self._inventory.release_for_order(order_id)

    async def restock_for_order(self, order_id: uuid.UUID) -> int:
        """Reverse the order's committed reservations (cancelled after a full commit)."""
        return await self._inventory.restock_for_order(order_id)

    async def commit_for_order(self, order_id: uuid.UUID, *, expected: int) -> int:
        """Consume the order's still-held reservations, all-or-nothing (success).

        Returns the order's committed total after the call — the retry-safe
        end-state the saga's paid-implies-consumed invariant checks against,
        not the per-call row count. A shortfall against ``expected`` consumes
        nothing.
        """
        return await self._inventory.commit_for_order(order_id, expected=expected)


class SagaCharges(ChargePort):
    """The saga's charges over the payments service.

    Terminal states map to the saga's vocabulary; a still-``pending`` attempt
    is reported as-is so the recovery poller defers to the payment reconciler.
    Recovery never calls :meth:`charge` (the payment token is never stored) —
    it only looks charges up and refunds them — but it shares this class so a
    gateway-behavior fix reaches both callers.
    """

    def __init__(self, payments: PaymentsService) -> None:
        self._payments = payments

    async def charge(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal, payment_token: str
    ) -> ChargeResult:
        """Charge through the gateway (idempotent on ``idempotency_key``)."""
        payment = await self._payments.charge(
            order_id=order_id,
            idempotency_key=idempotency_key,
            amount=amount,
            payment_method_token=payment_token,
        )
        return ChargeResult(status=payment.status)

    async def find_by_idempotency_key(self, idempotency_key: str) -> ChargeResult | None:
        """The recorded charge outcome, or ``None`` if never charged."""
        payment = await self._payments.get_by_idempotency_key(idempotency_key)
        if payment is None:
            return None
        return ChargeResult(status=payment.status)

    async def refund(self, *, idempotency_key: str, reason: str) -> bool:
        """Reverse the charge through the payments service (idempotent per key)."""
        return await self._payments.refund(idempotency_key=idempotency_key, reason=reason)


class SagaPriceTruth(PriceTruthPort):
    """The saga's price truth over the catalog repository.

    Deliberately the **repository**, not ``CatalogService``: the service's
    cache-aside is invalidated by the same asynchronous events whose
    propagation lag this guard exists to catch, so only the DB read is
    authoritative enough to revalidate checkout prices against. Recovery never
    calls it (a pending order keeps the prices it was created with) but shares
    the class so there is exactly one price-truth implementation.
    """

    def __init__(self, catalog: CatalogRepositoryPort) -> None:
        self._catalog = catalog

    async def current_prices(self, product_ids: list[uuid.UUID]) -> dict[uuid.UUID, Decimal]:
        """Live price per still-sellable id; gone products are absent."""
        products = await self._catalog.get_products_by_ids(product_ids)
        return {product.id: product.price for product in products}


def build_checkout_saga(
    session: AsyncSession,
    valkey: Any | None,
    settings: AppSettings,
    *,
    gateway: PaymentGatewayPort | None = None,
) -> CheckoutSaga:
    """Build the checkout saga over one session, Valkey client, and settings.

    ``gateway`` is the process-shared gateway when the caller has one (the
    request path caches it on ``app.state`` because the stub's deferred-charge
    window is process-local — a per-request instance would give the webhook a
    different window than the charge); ``None`` builds it from the same factory
    the API uses, so a provider swap reaches the recovery worker too.
    ``valkey=None`` (bare test app) keeps the DB UNIQUE backstop for
    idempotency but raises for the cart — like the product read-cache there is
    no DB to degrade to, so a missing client is 503, not a silent fallback.
    """
    if valkey is None:
        raise DependencyUnavailableError("cart storage is not configured")
    inventory = InventoryService(InventoryRepository(session), reservation_ttl_seconds=settings.reservation_ttl_seconds)
    payments = PaymentsService(
        PaymentsRepository(session),
        gateway if gateway is not None else make_payment_gateway(settings, valkey),
        webhook_secret=settings.payment_webhook_secret,
        webhook_tolerance_seconds=settings.payment_webhook_tolerance_seconds,
        reconciliation_grace_seconds=settings.payment_reconciliation_grace_seconds,
        reconciliation_max_age_seconds=settings.payment_reconciliation_max_age_seconds,
        on_payment_succeeded=cancelled_order_refund_hook(session),
    )
    return CheckoutSaga(
        orders_repository_with_payment_guard(session),
        SagaBasket(ValkeyCartRepository(valkey, ttl_seconds=settings.cart_ttl_seconds)),
        SagaStockHolds(inventory),
        SagaCharges(payments),
        ValkeyIdempotencyStore(valkey, ttl_seconds=settings.checkout_idempotency_ttl_seconds),
        prices=SagaPriceTruth(CatalogRepository(session)),
        step_timeout_seconds=settings.checkout_saga_step_timeout_seconds,
    )
