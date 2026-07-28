"""Service + mapper unit tests (ticket 03) — DB-free, fake repos.

Exercises the application seam without a database: a fake repo (a plain object
implementing the port) returns canned ORM-shaped rows / ``ProductRow`` / a
``Page``, and we assert the service maps them through the domain to the right
Pydantic response schema — with no ORM/``ProductRow`` leak. Each module's
``to_domain`` mapper is also exercised directly against an ORM-shaped row. The
HTTP ``dependency_overrides`` round-trip rides ticket 07.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from src.catalog.adapters.db.repository import ProductRow
from src.catalog.api.schemas import ProductResponse
from src.catalog.application.mappers import to_domain as product_to_domain
from src.catalog.application.service import CatalogService
from src.catalog.domain.product import Product
from src.identity.api.schemas import UserResponse
from src.identity.application.mappers import to_domain as user_to_domain
from src.identity.application.service import IdentityService
from src.identity.domain.user import User
from src.inventory.api.schemas import InventoryResponse
from src.inventory.application.mappers import to_domain as inventory_to_domain
from src.inventory.application.service import InventoryService
from src.inventory.domain.inventory import Inventory
from src.orders.api.schemas import OrderResponse
from src.orders.application.mappers import to_domain as order_to_domain
from src.orders.application.service import OrdersService
from src.orders.domain.order import Order, OrderItem, OrderStatus
from src.payments.api.schemas import PaymentResponse
from src.payments.application.mappers import to_domain as payment_to_domain
from src.payments.application.service import PaymentsService
from src.payments.domain.payment import Payment
from src.shared.db.pagination import Page, PageParams, PageResponse

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


# --- fakes ----------------------------------------------------------------


class _FakeCatalogRepo:
    def __init__(self, product=None, page=None):
        self._product = product
        self._page = page

    async def get_product(self, product_id):
        return self._product

    async def list_products(self, params, filters=None):
        return self._page


class _FakeOrdersRepo:
    def __init__(self, order=None, page=None):
        self._order = order
        self._page = page

    async def get_order(self, order_id):
        return self._order

    async def list_orders(self, user_id, params, status=None):
        return self._page


class _FakeSingleRepo:
    """Covers identity (get_by_*) and inventory (get_by_sku)."""

    def __init__(self, row=None):
        self._row = row

    async def get_by_oidc_sub(self, oidc_sub):
        return self._row

    async def get_by_id(self, user_id):
        return self._row

    async def get_by_sku(self, sku):
        return self._row


class _FakePaymentsRepo:
    def __init__(self, page):
        self._page = page

    async def get_by_order_id(self, order_id, params):
        return self._page


# --- row builders (ORM-shaped, duck-typed) --------------------------------


def _product_row_orm():
    return SimpleNamespace(
        id=uuid.uuid4(),
        merchant_id=uuid.uuid4(),
        name="widget",
        description="d",
        category="c",
        price=Decimal("9.99"),
        image_key=None,
        image_status="none",
        created_at=_NOW,
        updated_at=_NOW,
    )


def _order_row(item_count=2):
    items = [
        SimpleNamespace(
            id=uuid.uuid4(),
            product_id=uuid.uuid4(),
            product_name=f"line-{i}",
            unit_price=Decimal("5.00"),
            quantity=1,
        )
        for i in range(item_count)
    ]
    return SimpleNamespace(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        status="paid",
        total=Decimal("10.00"),
        items=items,
        created_at=_NOW,
        updated_at=_NOW,
    )


# --- catalog --------------------------------------------------------------


async def test_catalog_get_returns_pydantic_no_orm_leak():
    row = _product_row_orm()
    svc = CatalogService(_FakeCatalogRepo(product=row))
    result = await svc.get_product(row.id)
    assert isinstance(result, ProductResponse)
    assert result.id == row.id


async def test_catalog_list_maps_items_and_passes_cursor_through():
    rows = [_product_row_orm(), _product_row_orm()]
    svc = CatalogService(_FakeCatalogRepo(page=Page(items=rows, next_cursor="CURSOR")))
    page = await svc.list_products(PageParams())
    assert isinstance(page, PageResponse)
    assert all(isinstance(item, ProductResponse) for item in page.items)
    assert page.next_cursor == "CURSOR"


async def test_catalog_get_missing_returns_none():
    svc = CatalogService(_FakeCatalogRepo(product=None))
    assert await svc.get_product(uuid.uuid4()) is None


def test_catalog_mapper_orm_and_productrow_map_equal():
    orm = _product_row_orm()
    raw = ProductRow(
        id=orm.id,
        merchant_id=orm.merchant_id,
        name=orm.name,
        description=orm.description,
        category=orm.category,
        price=orm.price,
        image_key=orm.image_key,
        image_status=orm.image_status,
        created_at=orm.created_at,
        updated_at=orm.updated_at,
    )
    assert product_to_domain(orm) == product_to_domain(raw)
    assert isinstance(product_to_domain(raw), Product)


# --- orders ---------------------------------------------------------------


async def test_orders_get_embeds_mapped_items():
    row = _order_row(item_count=3)
    svc = OrdersService(_FakeOrdersRepo(order=row))
    result = await svc.get_order(row.id)
    assert isinstance(result, OrderResponse)
    assert result.status is OrderStatus.PAID
    assert len(result.items) == 3
    assert result.items[0].product_name == "line-0"


async def test_orders_list_passes_cursor_through():
    rows = [_order_row(item_count=1)]
    svc = OrdersService(_FakeOrdersRepo(page=Page(items=rows, next_cursor=None)))
    page = await svc.list_orders(uuid.uuid4(), PageParams())
    assert isinstance(page, PageResponse)
    assert page.next_cursor is None
    assert isinstance(page.items[0], OrderResponse)


async def test_orders_get_missing_returns_none():
    svc = OrdersService(_FakeOrdersRepo(order=None))
    assert await svc.get_order(uuid.uuid4()) is None


# --- internal services now return Pydantic response schemas ---------------


def _user_row():
    return SimpleNamespace(
        id=uuid.uuid4(), oidc_sub="sub-1", email="u@example.com", is_active=True, created_at=_NOW, updated_at=_NOW
    )


def _payment_row():
    return SimpleNamespace(
        id=uuid.uuid4(),
        order_id=uuid.uuid4(),
        status="succeeded",
        amount=Decimal("10.00"),
        gateway_ref="ref-1",
        created_at=_NOW,
        updated_at=_NOW,
    )


async def test_identity_returns_pydantic_user_not_orm():
    row = _user_row()
    svc = IdentityService(_FakeSingleRepo(row=row))
    result = await svc.get_by_oidc_sub("sub-1")
    assert isinstance(result, UserResponse)
    assert result.oidc_sub == "sub-1"
    assert await IdentityService(_FakeSingleRepo(row=None)).get_by_id(uuid.uuid4()) is None


async def test_inventory_returns_pydantic_schema_not_orm():
    row = SimpleNamespace(sku="sku-1", on_hand=5, reserved=2, version=3)
    svc = InventoryService(_FakeSingleRepo(row=row))
    result = await svc.get_by_sku("sku-1")
    assert isinstance(result, InventoryResponse)
    assert (result.on_hand, result.reserved, result.version) == (5, 2, 3)
    assert await InventoryService(_FakeSingleRepo(row=None)).get_by_sku("nope") is None


async def test_payments_returns_page_of_pydantic_payments():
    rows = [_payment_row()]
    svc = PaymentsService(_FakePaymentsRepo(page=Page(items=rows, next_cursor="C")))
    page = await svc.get_by_order_id(uuid.uuid4(), PageParams())
    assert isinstance(page, PageResponse)
    assert page.next_cursor == "C"
    assert isinstance(page.items[0], PaymentResponse)
    assert page.items[0].status == "succeeded"


# --- mappers exercised directly against ORM-shaped rows -------------------


def test_identity_mapper_maps_orm_row_to_domain_user():
    row = _user_row()
    result = user_to_domain(row)
    assert isinstance(result, User)
    assert (result.id, result.oidc_sub, result.email, result.is_active) == (
        row.id,
        row.oidc_sub,
        row.email,
        row.is_active,
    )


def test_inventory_mapper_maps_orm_row_to_domain_entity():
    row = SimpleNamespace(sku="sku-1", on_hand=5, reserved=2, version=3)
    result = inventory_to_domain(row)
    assert isinstance(result, Inventory)
    assert (result.sku, result.on_hand, result.reserved, result.version) == ("sku-1", 5, 2, 3)


def test_orders_mapper_maps_orm_row_and_lines_to_domain():
    row = _order_row(item_count=2)
    result = order_to_domain(row)
    assert isinstance(result, Order)
    assert result.status is OrderStatus.PAID
    assert isinstance(result.items, tuple)
    assert all(isinstance(item, OrderItem) for item in result.items)
    assert result.items[0].product_name == "line-0"


def test_payments_mapper_maps_orm_row_to_domain_payment():
    row = _payment_row()
    result = payment_to_domain(row)
    assert isinstance(result, Payment)
    assert (result.id, result.order_id, result.status, result.amount, result.gateway_ref) == (
        row.id,
        row.order_id,
        row.status,
        row.amount,
        row.gateway_ref,
    )
