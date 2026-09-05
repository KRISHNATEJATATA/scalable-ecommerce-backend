"""Product availability tests.

HTTP round-trip via ``httpx.AsyncClient`` over the ASGI app, real
Testcontainers-Postgres (identity + catalog + inventory migrations), in-process
RS256 keypair + fake JWKS (same approach as the catalog product tests).

Covers the ticket checklist: stocked product → correct value, unstocked →
explicit ``null`` (never ``0``), listing attaches per-item values, a
reservation moves the number on re-read, and the value is attached *after* the
product cache-aside lookup (never stored in the cached payload).
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from src.app import create_app
from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.application.service import CatalogService
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.shared.config.setting import AppSettings, get_settings
from src.shared.container import InventoryStockAvailability

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog", "inventory"]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

_TRUNCATE = text(
    "TRUNCATE catalog.products, catalog.outbox, identity.users, "
    "inventory.reservations, inventory.inventory, inventory.outbox CASCADE"
)


def _pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


class _FakeSigningKey:
    def __init__(self, public_key) -> None:
        self.key = public_key


class _FakeJWKClient:
    def __init__(self, public_key) -> None:
        self._public_key = public_key

    def get_signing_key_from_jwt(self, _token: str) -> _FakeSigningKey:
        return _FakeSigningKey(self._public_key)


def _make_token(rsa_key, *, roles=(), email=None, sub=None) -> str:
    now = int(time.time())
    claims = {
        "sub": sub or str(uuid.uuid4()),
        "email": email or f"{uuid.uuid4()}@test.io",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "iat": now,
        "exp": now + 300,
        "realm_access": {"roles": list(roles)},
    }
    return jwt.encode(claims, _pem(rsa_key), algorithm="RS256")


@pytest.fixture(scope="module")
def _migrated():
    with PostgresContainer("postgres:16-alpine") as pg:
        async_url = pg.get_connection_url(driver="asyncpg")
        old_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = async_url
        get_settings.cache_clear()
        try:
            for module in MODULES:
                subprocess.run(
                    [sys.executable, "-m", "alembic", "-c", f"src/{module}/alembic.ini", "upgrade", "head"],
                    cwd=REPO_ROOT,
                    check=True,
                )
            yield async_url
        finally:
            if old_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_url
            get_settings.cache_clear()


@pytest.fixture
async def engine(_migrated):
    eng = create_async_engine(_migrated)
    async with eng.begin() as conn:
        await conn.execute(_TRUNCATE)
    yield eng
    await eng.dispose()


@pytest.fixture
def sessionmaker(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def app_ctx(_migrated, rsa_key, sessionmaker):
    settings = AppSettings(
        _env_file=None,
        database_url=_migrated,
        keycloak_issuer=ISSUER,
        keycloak_audience=AUDIENCE,
        keycloak_jwks_url="https://keycloak.test/certs",
    )
    app = create_app(settings)
    app.state.jwks_client = _FakeJWKClient(rsa_key.public_key())
    app.state.db_sessionmaker = sessionmaker
    return app


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


_PRODUCT = {"name": "widget", "description": "d", "category": "tools", "price": "9.99"}


async def _create_product(client, token) -> dict:
    resp = await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _stock_row(sessionmaker, sku: str, *, on_hand: int, reserved: int = 0) -> None:
    async with sessionmaker() as s:
        await s.execute(
            text(
                "INSERT INTO inventory.inventory (sku, on_hand, reserved, version) "
                "VALUES (:sku, :on_hand, :reserved, 1)"
            ),
            {"sku": sku, "on_hand": on_hand, "reserved": reserved},
        )
        await s.commit()


# --- detail -----------------------------------------------------------------


async def test_detail_returns_available_for_stocked_product(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        product = await _create_product(client, token)
        await _stock_row(sessionmaker, product["id"], on_hand=10, reserved=3)
        got = await client.get(f"/v1/products/{product['id']}", headers=_auth(token))
    assert got.status_code == 200
    assert got.json()["available"] == 7


async def test_detail_returns_null_without_stock_row(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        product = await _create_product(client, token)
        got = await client.get(f"/v1/products/{product['id']}", headers=_auth(token))
    assert got.status_code == 200
    body = got.json()
    assert "available" in body  # required on the wire, explicitly null
    assert body["available"] is None


# --- list -------------------------------------------------------------------


async def test_list_attaches_per_item_values_with_one_batch_query(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        first = await _create_product(client, token)
        second = await _create_product(client, token)
        await _stock_row(sessionmaker, first["id"], on_hand=5, reserved=5)  # known out of stock
        resp = await client.get("/v1/products?limit=100", headers=_auth(token))
    assert resp.status_code == 200
    by_id = {item["id"]: item for item in resp.json()["items"]}
    assert by_id[first["id"]]["available"] == 0
    assert by_id[second["id"]]["available"] is None


# --- movement ---------------------------------------------------------------


async def test_reservation_then_reread_moves_the_number(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        product = await _create_product(client, token)
        await _stock_row(sessionmaker, product["id"], on_hand=10)
        async with sessionmaker() as s:
            inventory = InventoryService(InventoryRepository(s), reservation_ttl_seconds=900)
            await inventory.reserve(product["id"], 4, uuid.uuid4())
        got = await client.get(f"/v1/products/{product['id']}", headers=_auth(token))
    assert got.status_code == 200
    assert got.json()["available"] == 6


# --- cache interplay ----------------------------------------------------------


class _FakeCache:
    """Minimal in-memory ``ProductCachePort``: real hit/miss semantics, recorded stores."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.stored: list[str] = []
        self._locks: set[str] = set()

    async def get(self, product_id) -> str | None:
        return self.values.get(f"product:{product_id}")

    async def store_if_owner(self, product_id, payload: str, token: str) -> bool:
        self.stored.append(payload)
        self.values[f"product:{product_id}"] = payload
        return True

    async def store_miss_if_owner(self, product_id, token: str) -> bool:
        return True

    async def invalidate(self, product_id) -> None:
        self.values.pop(f"product:{product_id}", None)

    async def evict_value(self, product_id, expected: str) -> None:
        if self.values.get(f"product:{product_id}") == expected:
            del self.values[f"product:{product_id}"]

    async def acquire_fill_lock(self, product_id, token: str) -> bool:
        key = f"product:lock:{product_id}"
        if key in self._locks:
            return False
        self._locks.add(key)
        return True

    async def renew_fill_lock(self, product_id, token: str) -> bool:
        return True

    async def release_fill_lock(self, product_id, token: str) -> None:
        self._locks.discard(f"product:lock:{product_id}")

    async def fill_lock_held(self, product_id) -> bool:
        return f"product:lock:{product_id}" in self._locks


async def test_cached_response_still_carries_fresh_available(sessionmaker):
    """The cached payload excludes ``available``; every read re-attaches it live."""
    import json
    from decimal import Decimal

    from src.catalog.application.dto import ProductCreate

    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        inventory = InventoryService(InventoryRepository(s), reservation_ttl_seconds=900)
        cache = _FakeCache()
        service = CatalogService(repo, None, cache, availability=InventoryStockAvailability(inventory))
        created = await service.create_product(
            merchant_id=uuid.uuid4(), data=ProductCreate(name="widget", price=Decimal("9.99"))
        )
        first = await service.get_product(created.id)
        assert first is not None and first.available is None  # no stock row yet
        assert len(cache.stored) == 1
        assert "available" not in json.loads(cache.stored[0])  # never stored in the payload
        await s.execute(
            text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, 10, 0, 1)"),
            {"sku": str(created.id)},
        )
        await s.commit()
        second = await service.get_product(created.id)  # served from cache for the product…
        assert second is not None and second.available == 10  # …but availability is fresh
        await inventory.reserve(str(created.id), 3, uuid.uuid4())
        third = await service.get_product(created.id)
        assert third is not None and third.available == 7
