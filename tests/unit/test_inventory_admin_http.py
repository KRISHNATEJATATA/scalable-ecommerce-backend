"""Inventory admin stock-upsert HTTP contract.

Full round-trip via ``httpx.AsyncClient`` over the ASGI app: real
Testcontainers-Postgres, real Valkey, in-process RS256 keypair + fake JWKS —
the same harness shape as ``test_checkout_http``. The endpoint is the contract
path that replaces direct-SQL stock seeding: a merchant declares
``PUT /v1/admin/inventory/{sku}`` and checkout then succeeds against it.
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
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer
from valkey.asyncio import Valkey

from src.app import create_app
from src.shared.config.setting import AppSettings, get_settings

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog", "inventory", "orders", "payments"]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

_TRUNCATE = text(
    "TRUNCATE catalog.products, catalog.outbox, identity.users, "
    "inventory.reservations, inventory.inventory, inventory.outbox, "
    "orders.order_items, orders.orders, orders.outbox, "
    "payments.payments, payments.outbox CASCADE"
)


# --- keypair + token helpers (mirrors test_checkout_http) ---------------------


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


# --- containers + app ----------------------------------------------------------


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


@pytest.fixture(scope="module")
def _valkey_url():
    with DockerContainer("valkey/valkey:8").with_exposed_ports(6379) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def app_ctx(_migrated, _valkey_url, rsa_key):
    engine = create_async_engine(_migrated)
    async with engine.begin() as conn:
        await conn.execute(_TRUNCATE)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    valkey = Valkey.from_url(_valkey_url)
    await valkey.flushall()
    settings = AppSettings(
        _env_file=None,
        database_url=_migrated,
        valkey_url=_valkey_url,
        keycloak_issuer=ISSUER,
        keycloak_audience=AUDIENCE,
        keycloak_jwks_url="https://keycloak.test/certs",
    )
    app = create_app(settings)
    app.state.jwks_client = _FakeJWKClient(rsa_key.public_key())
    app.state.db_sessionmaker = sessionmaker
    app.state.valkey = valkey
    yield app, sessionmaker
    await valkey.flushall()
    await valkey.aclose()
    await engine.dispose()


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _create_product(app, rsa_key, *, merchant=None) -> tuple[str, str]:
    """One product via the API; returns (product_id, merchant_token)."""
    merchant = merchant or _make_token(rsa_key, roles=["merchant"])
    async with _client(app) as client:
        resp = await client.post(
            "/v1/products",
            headers=_auth(merchant),
            json={"name": "widget", "description": "d", "category": "tools", "price": "9.99"},
        )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"], merchant


# --- the contract --------------------------------------------------------------


async def test_upsert_creates_row_and_is_idempotent(app_ctx, rsa_key):
    app, _sessionmaker = app_ctx
    sku, merchant = await _create_product(app, rsa_key)
    admin = _make_token(rsa_key, roles=["admin"])

    async with _client(app) as client:
        resp = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 25})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["sku"] == sku
        assert body["on_hand"] == 25
        assert body["reserved"] == 0
        assert body["version"] == 1

        # Idempotent: same value re-lands the same state.
        again = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 25})
        assert again.status_code == 200
        assert again.json() == body

        # Re-point: on_hand moves, version bumps.
        repoint = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 30})
        assert repoint.status_code == 200
        assert repoint.json()["on_hand"] == 30
        assert repoint.json()["version"] == 2

        # Admin passes the same gate (any-of merchant/admin).
        as_admin = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(admin), json={"on_hand": 10})
        assert as_admin.status_code == 200
        assert as_admin.json()["on_hand"] == 10


async def test_upsert_rejects_wrong_roles_and_anonymous(app_ctx, rsa_key):
    app, _sessionmaker = app_ctx
    sku, merchant = await _create_product(app, rsa_key)
    consumer = _make_token(rsa_key, roles=["consumer"])
    service = _make_token(rsa_key, roles=["service"])

    async with _client(app) as client:
        forbidden = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(consumer), json={"on_hand": 5})
        assert forbidden.status_code == 403
        machine = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(service), json={"on_hand": 5})
        assert machine.status_code == 403
        anon = await client.put(f"/v1/admin/inventory/{sku}", json={"on_hand": 5})
        assert anon.status_code == 401
        # A well-authenticated merchant sending a bad body reaches the 422 boundary.
        bad_body = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": -1})
        assert bad_body.status_code == 422


async def test_upsert_below_reserved_is_409(app_ctx, rsa_key):
    app, sessionmaker = app_ctx
    sku, merchant = await _create_product(app, rsa_key)

    async with _client(app) as client:
        ok = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 10})
        assert ok.status_code == 200

    # Place a live hold directly (the saga path reserves via the service; the
    # upsert's guard must refuse a shrink below it either way).
    async with sessionmaker() as session:
        await session.execute(
            text(
                "INSERT INTO inventory.reservations (id, sku, qty, order_id, status, expires_at) "
                "VALUES (:id, :sku, 3, :oid, 'held', now() + interval '1 hour')"
            ),
            {"id": uuid.uuid4(), "sku": sku, "oid": uuid.uuid4()},
        )
        await session.execute(
            text("UPDATE inventory.inventory SET reserved = 3 WHERE sku = :sku"),
            {"sku": sku},
        )
        await session.commit()

    async with _client(app) as client:
        refused = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 2})
        assert refused.status_code == 409
        assert refused.headers["content-type"] == "application/problem+json"
        assert "below" in refused.json()["detail"]

        # A shrink that still covers the holds is fine.
        smaller = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 3})
        assert smaller.status_code == 200
        assert smaller.json()["on_hand"] == 3

        # Raising above the holds always works.
        raised = await client.put(f"/v1/admin/inventory/{sku}", headers=_auth(merchant), json={"on_hand": 50})
        assert raised.status_code == 200


async def test_api_only_seeded_checkout_succeeds(app_ctx, rsa_key):
    """The issue's acceptance shape: product + stock + cart + checkout, all API, zero SQL seeding."""
    app, _sessionmaker = app_ctx
    merchant = _make_token(rsa_key, roles=["merchant"])
    consumer = _make_token(rsa_key, roles=["consumer"])

    async with _client(app) as client:
        product_id, _merchant = await _create_product(app, rsa_key, merchant=merchant)
        seeded = await client.put(f"/v1/admin/inventory/{product_id}", headers=_auth(merchant), json={"on_hand": 5})
        assert seeded.status_code == 200, seeded.text
        added = await client.post(
            "/v1/cart/items", headers=_auth(consumer), json={"product_id": product_id, "quantity": 2}
        )
        assert added.status_code == 200, added.text
        checked_out = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": f"inv-{uuid.uuid4()}"},
            json={"payment_token": "tok_visa"},
        )
    assert checked_out.status_code == 201, checked_out.text
    assert checked_out.json()["status"] == "paid"

    # The paid order's hold committed: on_hand dropped by the purchased qty.
    async with _client(app) as client:
        stock = await client.get(f"/v1/products/{product_id}", headers=_auth(consumer))
    assert stock.status_code == 200
    assert stock.json()["available"] == 3
