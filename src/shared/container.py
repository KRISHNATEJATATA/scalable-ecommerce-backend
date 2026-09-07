"""Dependency-injection wiring.

One provider chain per DB-backed module: ``get_session`` → ``get_<m>_repository``
→ ``get_<m>_service``. Repository/service providers are annotated to the **port**
(the abstraction), so routes depend on the contract, not the concrete adapter —
and tests inject fakes via ``app.dependency_overrides`` with no internal patching.

Reads only this phase; write services (create/checkout/reserve/JIT) and their
storage/payment/bus ports land with their feature tickets.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Annotated, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.cart.adapters.valkey.repository import ValkeyCartRepository
from src.cart.application.service import CartService
from src.cart.ports.products import CartProductPort, ProductSnapshot
from src.cart.ports.repository import CartRepositoryPort
from src.catalog.adapters.cache import ValkeyProductCache
from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.adapters.s3_images import ImageStore
from src.catalog.application.service import CatalogService
from src.catalog.ports.availability import StockAvailabilityPort
from src.catalog.ports.cache import ProductCachePort
from src.catalog.ports.repository import CatalogRepositoryPort
from src.catalog.ports.storage import ImageStorePort
from src.identity.adapters.db.repository import IdentityRepository
from src.identity.application.dto import UserResponse
from src.identity.application.service import IdentityAdminService, IdentityService
from src.identity.ports.admin import IdentityAdminPort
from src.identity.ports.repository import IdentityRepositoryPort
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.inventory.ports.repository import InventoryRepositoryPort
from src.orders.adapters.db.repository import OrdersRepository
from src.orders.adapters.idempotency import ValkeyIdempotencyStore
from src.orders.application.checkout_saga import CheckoutSaga
from src.orders.application.service import OrdersService
from src.orders.ports.checkout import (
    BasketPort,
    ChargePort,
    ChargeResult,
    CheckoutLine,
    IdempotencyPort,
    StockHoldsPort,
)
from src.orders.ports.repository import OrdersRepositoryPort
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.stub_gateway import StubPaymentGateway
from src.payments.application.service import PaymentsService
from src.payments.ports.gateway import PaymentGatewayPort
from src.payments.ports.repository import PaymentsRepositoryPort
from src.shared.auth.dependencies import PrincipalDep
from src.shared.db.session import get_session
from src.shared.errors.exceptions import (
    AuthenticationError,
    AuthorizationError,
    DependencyUnavailableError,
)

SessionDep = Annotated[AsyncSession, Depends(get_session)]


# --- inventory ------------------------------------------------------------
def get_inventory_repository(session: SessionDep) -> InventoryRepositoryPort:
    """Provide the inventory repository bound to the request session (port-typed)."""
    return InventoryRepository(session)


def get_inventory_service(
    request: Request,
    repo: Annotated[InventoryRepositoryPort, Depends(get_inventory_repository)],
) -> InventoryService:
    """Provide the inventory service over its repository port."""
    return InventoryService(repo, reservation_ttl_seconds=request.app.state.settings.reservation_ttl_seconds)


class InventoryStockAvailability(StockAvailabilityPort):
    """Catalog's :class:`StockAvailabilityPort` built over the inventory service.

    Lives here — the one place allowed to touch every module — so catalog never
    names inventory (the ``module-independence`` contract forbids even
    application-layer imports between them). Read-only: a missing stock row is
    absent from the map (unknown), never zero.
    """

    def __init__(self, inventory: InventoryService) -> None:
        self._inventory = inventory

    async def available_for(self, skus: list[str]) -> dict[str, int]:
        """Purchasable units per stocked SKU (``max(on_hand - reserved, 0)``)."""
        rows = await self._inventory.get_many_by_skus(skus)
        return {sku: max(row.on_hand - row.reserved, 0) for sku, row in rows.items()}


# --- catalog --------------------------------------------------------------
def get_catalog_repository(session: SessionDep) -> CatalogRepositoryPort:
    """Provide the catalog repository bound to the request session (port-typed).

    The cast is structural, not a bypass: ``CatalogRepository`` satisfies
    ``CatalogRepositoryPort`` at runtime (the catalog tests assert
    ``isinstance`` for it), but SQLAlchemy's ``Mapped[...]`` descriptors make
    the ORM's ``Product`` fail the checker's structural match against the
    protocol's ``ProductRecord`` view of the same attributes.
    """
    return cast(CatalogRepositoryPort, CatalogRepository(session))


def get_image_store(request: Request) -> ImageStorePort | None:
    """Provide the shared S3 image store (entered once in the app lifespan)."""
    s3 = getattr(request.app.state, "s3", None)
    if s3 is None:
        return None
    settings = request.app.state.settings
    return ImageStore(s3, settings.s3_bucket, presign_public_base_url=settings.s3_presign_public_base_url)


def get_product_cache(request: Request) -> ProductCachePort | None:
    """Provide the Valkey product read-cache, or ``None`` if disabled/unavailable.

    ``None`` (feature flag off, or no Valkey on a bare test app) makes the catalog
    service fall straight through to the DB, so cache wiring never breaks reads.
    """
    settings = request.app.state.settings
    if not settings.product_cache_enabled:
        return None
    valkey = getattr(request.app.state, "valkey", None)
    if valkey is None:
        return None
    return ValkeyProductCache(
        valkey,
        ttl_seconds=settings.product_cache_ttl_seconds,
        ttl_jitter_seconds=settings.product_cache_ttl_jitter_seconds,
        lock_ttl_seconds=settings.product_cache_lock_ttl_seconds,
        negative_ttl_seconds=settings.product_cache_negative_ttl_seconds,
    )


def get_catalog_service(
    request: Request,
    repo: Annotated[CatalogRepositoryPort, Depends(get_catalog_repository)],
    image_store: Annotated[ImageStorePort | None, Depends(get_image_store)],
    cache: Annotated[ProductCachePort | None, Depends(get_product_cache)],
    inventory: Annotated[InventoryService, Depends(get_inventory_service)],
) -> CatalogService:
    """Provide the catalog service over its repository + image-store + cache ports."""
    settings = request.app.state.settings
    return CatalogService(
        repo,
        image_store,
        cache,
        availability=InventoryStockAvailability(inventory),
        lock_ttl_seconds=settings.product_cache_lock_ttl_seconds,
        max_fill_wait_seconds=settings.product_cache_max_fill_wait_seconds,
        image_base_url=settings.image_public_base_url,
        image_max_upload_bytes=settings.image_max_upload_bytes,
        image_upload_ttl_seconds=settings.image_upload_ttl_seconds,
    )


# --- orders ---------------------------------------------------------------
def get_orders_repository(session: SessionDep) -> OrdersRepositoryPort:
    """Provide the orders repository bound to the request session (port-typed)."""
    return OrdersRepository(session)


class OrderBaskets(BasketPort):
    """Orders' :class:`BasketPort` built over the cart service.

    Lives here — the one place allowed to touch every module — so orders never
    names cart. The cart's price/name snapshots become the order lines verbatim:
    later catalog edits never rewrite order history.
    """

    def __init__(self, cart: CartService) -> None:
        self._cart = cart

    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        """The user's current cart lines as checkout lines (``[]`` when empty)."""
        cart = await self._cart.get_cart(user_id)
        return [
            CheckoutLine(
                product_id=item.product_id,
                name=item.name,
                unit_price=item.unit_price,
                quantity=item.quantity,
            )
            for item in cart.items
        ]

    async def clear(self, user_id: uuid.UUID) -> None:
        """Empty the basket after a successful checkout."""
        await self._cart.clear_cart(user_id)


class OrderStockHolds(StockHoldsPort):
    """Orders' :class:`StockHoldsPort` built over the inventory service.

    Lives here so orders never names inventory. SKU mapping ``str(product.id)``
    is the composition seam from ADR 0011 — the saga is its production caller.
    """

    def __init__(self, inventory: InventoryService) -> None:
        self._inventory = inventory

    async def reserve(self, sku: str, qty: int, order_id: uuid.UUID) -> uuid.UUID:
        """Hold ``qty`` of ``sku`` for ``order_id``; returns the reservation id."""
        return (await self._inventory.reserve(sku, qty, order_id)).id

    async def release_for_order(self, order_id: uuid.UUID) -> int:
        """Release every still-held reservation of one order (compensation)."""
        return await self._inventory.release_for_order(order_id)

    async def commit_for_order(self, order_id: uuid.UUID) -> int:
        """Consume every still-held reservation of one order (success)."""
        return await self._inventory.commit_for_order(order_id)


class OrderCharges(ChargePort):
    """Orders' :class:`ChargePort` built over the payments service.

    Lives here so orders never names payments. Terminal states map to the
    saga's vocabulary; a still-``pending`` attempt is reported as-is so the
    recovery poller defers to the payment reconciler.
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


# The saga's provider functions live at the bottom of this file (after the
# cart/payments providers they depend on); the port-adapter classes above are
# import-only and safe anywhere.


# --- identity -------------------------------------------------------------
def get_identity_repository(session: SessionDep) -> IdentityRepositoryPort:
    """Provide the identity repository bound to the request session (port-typed)."""
    return IdentityRepository(session)


def get_identity_service(repo: Annotated[IdentityRepositoryPort, Depends(get_identity_repository)]) -> IdentityService:
    """Provide the identity service over its repository port."""
    return IdentityService(repo)


def get_identity_admin(request: Request) -> IdentityAdminPort:
    """Provide the shared Keycloak admin adapter (built once in the app lifespan)."""
    return request.app.state.identity_admin


def get_identity_admin_service(
    repo: Annotated[IdentityRepositoryPort, Depends(get_identity_repository)],
    admin: Annotated[IdentityAdminPort, Depends(get_identity_admin)],
) -> IdentityAdminService:
    """Provide the admin identity service (Keycloak role/enablement management)."""
    return IdentityAdminService(repo, admin)


async def get_current_db_user(
    principal: PrincipalDep,
    service: Annotated[IdentityService, Depends(get_identity_service)],
) -> UserResponse:
    """Resolve the caller's local ``users`` row, JIT-provisioning on first sight.

    Depends on :func:`get_current_user` (token already verified) and is wired
    only into routes that need the local ``users.id`` (writes/ownership). A
    disabled local row is rejected with 403 — short token TTL + this flip is the
    revocation story (no Valkey denylist).
    """
    if principal.email is None:
        raise AuthenticationError("token is missing the required 'email' claim")
    user = await service.get_or_create_by_sub(principal.sub, principal.email)
    if not user.is_active:
        raise AuthorizationError("account disabled")
    return user


CurrentUserDep = Annotated[UserResponse, Depends(get_current_db_user)]


# --- cart -------------------------------------------------------------------
def get_cart_repository(request: Request) -> CartRepositoryPort:
    """Provide the Valkey cart repository (rolling TTL from settings).

    Valkey is required for carts — unlike the product read-cache there is no
    DB to degrade to — so a missing client is 503, not a silent fallback.
    """
    valkey = getattr(request.app.state, "valkey", None)
    if valkey is None:
        raise DependencyUnavailableError("cart storage is not configured")
    return ValkeyCartRepository(valkey, ttl_seconds=request.app.state.settings.cart_ttl_seconds)


class CatalogCartProducts(CartProductPort):
    """Cart's :class:`CartProductPort` built over the catalog service.

    Lives here — the one place allowed to touch every module — so cart never
    names catalog. Read-only: ``None`` means unknown or soft-deleted.
    """

    def __init__(self, catalog: CatalogService) -> None:
        self._catalog = catalog

    async def get_snapshot(self, product_id: uuid.UUID) -> ProductSnapshot | None:
        """The product's current snapshot, or ``None`` if unknown/soft-deleted."""
        product = await self._catalog.get_product(product_id)
        if product is None:
            return None
        return ProductSnapshot(
            product_id=product.id,
            name=product.name,
            unit_price=product.price,
            image_url=product.image_url,
        )


def get_cart_products(
    catalog: Annotated[CatalogService, Depends(get_catalog_service)],
) -> CartProductPort:
    """Provide the catalog-backed product snapshots for cart lines."""
    return CatalogCartProducts(catalog)


def get_cart_service(
    request: Request,
    repo: Annotated[CartRepositoryPort, Depends(get_cart_repository)],
    products: Annotated[CartProductPort, Depends(get_cart_products)],
) -> CartService:
    """Provide the cart service over its repository + product-snapshot ports."""
    settings = request.app.state.settings
    return CartService(
        repo,
        products,
        max_items=settings.cart_max_items,
        max_qty_per_line=settings.cart_max_qty_per_line,
    )


# --- payments -------------------------------------------------------------
def get_payments_repository(session: SessionDep) -> PaymentsRepositoryPort:
    """Provide the payments repository bound to the request session (port-typed)."""
    return PaymentsRepository(session)


def get_payment_gateway(request: Request) -> PaymentGatewayPort:
    """Provide the gateway behind the Strategy port — swap the stub for a real
    provider by replacing this one provider; no caller changes."""
    # One instance per process, lazily built on app.state: the stub's
    # idempotency map is process-local, so a per-request instance would give
    # the webhook and the reconciler a *different* window than the charge —
    # late confirmations would resolve to nothing.
    gateway = getattr(request.app.state, "payment_gateway", None)
    if gateway is None:
        gateway = StubPaymentGateway(request.app.state.settings.payment_stub_fail_token_substring)
        request.app.state.payment_gateway = gateway
    return gateway


def get_payments_service(
    repo: Annotated[PaymentsRepositoryPort, Depends(get_payments_repository)],
    request: Request,
) -> PaymentsService:
    """Provide the payments service over its repository + gateway ports."""
    settings = request.app.state.settings
    return PaymentsService(
        repo,
        get_payment_gateway(request),
        webhook_secret=settings.payment_webhook_secret,
        reconciliation_grace_seconds=settings.payment_reconciliation_grace_seconds,
        reconciliation_max_age_seconds=settings.payment_reconciliation_max_age_seconds,
    )


# --- checkout saga (orders) ------------------------------------------------
# Kept after the cart/payments/inventory providers: these functions reference
# them, and defined-after-use in source reads like a bug to the type checker
# even though FastAPI resolves the strings lazily.
def get_order_basket(
    cart: Annotated[CartService, Depends(get_cart_service)],
) -> BasketPort:
    """Provide the cart-backed basket lines for checkout."""
    return OrderBaskets(cart)


def get_order_stock_holds(
    inventory: Annotated[InventoryService, Depends(get_inventory_service)],
) -> StockHoldsPort:
    """Provide the inventory-backed stock holds for the saga."""
    return OrderStockHolds(inventory)


def get_order_charges(
    payments: Annotated[PaymentsService, Depends(get_payments_service)],
) -> ChargePort:
    """Provide the payments-backed charges for the saga."""
    return OrderCharges(payments)


def get_order_idempotency(request: Request) -> IdempotencyPort | None:
    """Provide the Valkey idempotency fast path, or ``None`` if Valkey is down/absent.

    ``None`` only loses the fast path — the DB UNIQUE backstop still prevents
    duplicate orders, degrading to re-reading the stored row.
    """
    valkey = getattr(request.app.state, "valkey", None)
    if valkey is None:
        return None
    return ValkeyIdempotencyStore(valkey, ttl_seconds=request.app.state.settings.checkout_idempotency_ttl_seconds)


def get_checkout_saga(
    repo: Annotated[OrdersRepositoryPort, Depends(get_orders_repository)],
    basket: Annotated[BasketPort, Depends(get_order_basket)],
    holds: Annotated[StockHoldsPort, Depends(get_order_stock_holds)],
    charges: Annotated[ChargePort, Depends(get_order_charges)],
    idempotency: Annotated[IdempotencyPort | None, Depends(get_order_idempotency)],
    request: Request,
) -> CheckoutSaga:
    """Provide the checkout saga orchestrator over its ports."""
    return CheckoutSaga(
        repo,
        basket,
        holds,
        charges,
        idempotency,
        step_timeout_seconds=request.app.state.settings.checkout_saga_step_timeout_seconds,
    )


def get_orders_service(
    repo: Annotated[OrdersRepositoryPort, Depends(get_orders_repository)],
    holds: Annotated[StockHoldsPort, Depends(get_order_stock_holds)],
) -> OrdersService:
    """Provide the orders service over its repository + stock-holds ports (cancel needs both)."""
    return OrdersService(repo, holds)
