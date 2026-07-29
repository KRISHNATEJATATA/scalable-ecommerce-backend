"""Dependency-injection wiring.

One provider chain per DB-backed module: ``get_session`` → ``get_<m>_repository``
→ ``get_<m>_service``. Repository/service providers are annotated to the **port**
(the abstraction), so routes depend on the contract, not the concrete adapter —
and tests inject fakes via ``app.dependency_overrides`` with no internal patching.

Reads only this phase; write services (create/checkout/reserve/JIT) and their
storage/payment/bus ports land with their feature tickets.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.catalog.adapters.cache import ValkeyProductCache
from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.adapters.s3_images import ImageStore
from src.catalog.application.service import CatalogService
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
from src.orders.application.service import OrdersService
from src.orders.ports.repository import OrdersRepositoryPort
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.application.service import PaymentsService
from src.payments.ports.repository import PaymentsRepositoryPort
from src.shared.auth.dependencies import PrincipalDep
from src.shared.db.session import get_session
from src.shared.errors.exceptions import AuthenticationError, AuthorizationError

SessionDep = Annotated[AsyncSession, Depends(get_session)]


# --- catalog --------------------------------------------------------------
def get_catalog_repository(session: SessionDep) -> CatalogRepositoryPort:
    """Provide the catalog repository bound to the request session (port-typed)."""
    return CatalogRepository(session)


def get_image_store(request: Request) -> ImageStorePort | None:
    """Provide the shared S3 image store (entered once in the app lifespan)."""
    s3 = getattr(request.app.state, "s3", None)
    if s3 is None:
        return None
    return ImageStore(s3, request.app.state.settings.s3_bucket)


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
) -> CatalogService:
    """Provide the catalog service over its repository + image-store + cache ports."""
    return CatalogService(
        repo,
        image_store,
        cache,
        lock_ttl_seconds=request.app.state.settings.product_cache_lock_ttl_seconds,
    )


# --- orders ---------------------------------------------------------------
def get_orders_repository(session: SessionDep) -> OrdersRepositoryPort:
    """Provide the orders repository bound to the request session (port-typed)."""
    return OrdersRepository(session)


def get_orders_service(repo: Annotated[OrdersRepositoryPort, Depends(get_orders_repository)]) -> OrdersService:
    """Provide the orders service over its repository port."""
    return OrdersService(repo)


# --- inventory ------------------------------------------------------------
def get_inventory_repository(session: SessionDep) -> InventoryRepositoryPort:
    """Provide the inventory repository bound to the request session (port-typed)."""
    return InventoryRepository(session)


def get_inventory_service(
    repo: Annotated[InventoryRepositoryPort, Depends(get_inventory_repository)],
) -> InventoryService:
    """Provide the inventory service over its repository port."""
    return InventoryService(repo)


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


# --- payments -------------------------------------------------------------
def get_payments_repository(session: SessionDep) -> PaymentsRepositoryPort:
    """Provide the payments repository bound to the request session (port-typed)."""
    return PaymentsRepository(session)


def get_payments_service(repo: Annotated[PaymentsRepositoryPort, Depends(get_payments_repository)]) -> PaymentsService:
    """Provide the payments service over its repository port."""
    return PaymentsService(repo)
