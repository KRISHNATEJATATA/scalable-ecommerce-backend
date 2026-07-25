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

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.application.service import CatalogService
from src.catalog.ports.repository import CatalogRepositoryPort
from src.identity.adapters.db.repository import IdentityRepository
from src.identity.application.service import IdentityService
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
from src.shared.db.session import get_session

SessionDep = Annotated[AsyncSession, Depends(get_session)]


# --- catalog --------------------------------------------------------------
def get_catalog_repository(session: SessionDep) -> CatalogRepositoryPort:
    """Provide the catalog repository bound to the request session (port-typed)."""
    return CatalogRepository(session)


def get_catalog_service(repo: Annotated[CatalogRepositoryPort, Depends(get_catalog_repository)]) -> CatalogService:
    """Provide the catalog service over its repository port."""
    return CatalogService(repo)


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


# --- payments -------------------------------------------------------------
def get_payments_repository(session: SessionDep) -> PaymentsRepositoryPort:
    """Provide the payments repository bound to the request session (port-typed)."""
    return PaymentsRepository(session)


def get_payments_service(repo: Annotated[PaymentsRepositoryPort, Depends(get_payments_repository)]) -> PaymentsService:
    """Provide the payments service over its repository port."""
    return PaymentsService(repo)
