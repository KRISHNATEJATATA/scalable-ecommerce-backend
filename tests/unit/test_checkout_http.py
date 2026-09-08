"""Checkout + order-history HTTP contract.

Full round-trip via ``httpx.AsyncClient`` over the ASGI app: real
Testcontainers-Postgres (identity + catalog + inventory + orders + payments
migrations), real Valkey (the cart lives there), in-process RS256 keypair +
fake JWKS. Asserts the frozen contract exactly — paths, verbs, status codes,
the ``Order`` shape, and the ``Idempotency-Key`` semantics — so a drift that
breaks the SPA fails here, not in review.
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


# --- keypair + token helpers (mirrors test_catalog_products) -----------------


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


async def _seed_stock(sessionmaker, sku: str, on_hand: int) -> None:
    async with sessionmaker() as session:
        await session.execute(
            text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, :o, 0, 1)"),
            {"sku": sku, "o": on_hand},
        )
        await session.commit()


async def _setup_cart(app, sessionmaker, rsa_key, *, consumer_token: str | None = None) -> tuple[str, str, str]:
    """Merchant creates a product; consumer puts 1 unit in their cart. Returns (consumer, product_id, merchant)."""
    merchant = _make_token(rsa_key, roles=["merchant"])
    consumer = consumer_token or _make_token(rsa_key, roles=["consumer"])
    async with _client(app) as client:
        resp = await client.post(
            "/v1/products",
            headers=_auth(merchant),
            json={"name": "widget", "description": "d", "category": "tools", "price": "9.99"},
        )
    assert resp.status_code == 201, resp.text
    product_id = resp.json()["id"]
    await _seed_stock(sessionmaker, product_id, 5)
    async with _client(app) as client:
        resp = await client.post(
            "/v1/cart/items", headers=_auth(consumer), json={"product_id": product_id, "quantity": 1}
        )
    assert resp.status_code == 200, resp.text
    return consumer, product_id, merchant


# --- the contract --------------------------------------------------------------


async def test_checkout_happy_path_shape_and_replay(app_ctx, rsa_key):
    app, sessionmaker = app_ctx
    consumer, _product_id, _merchant = await _setup_cart(app, sessionmaker, rsa_key)

    async with _client(app) as client:
        resp = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-1"},
            json={"payment_token": "tok_visa"},
        )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert set(body) == {"id", "user_id", "status", "total", "items", "created_at", "updated_at"}
    assert body["status"] == "paid"
    assert body["total"] == "9.99"  # decimal-as-string, never float
    assert body["items"][0]["product_name"] == "widget"
    assert body["items"][0]["unit_price"] == "9.99"
    assert body["items"][0]["quantity"] == 1

    async with _client(app) as client:
        # Same key + same body replays the stored 201 (cart was cleared; the
        # replay must not need it).
        replay = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-1"},
            json={"payment_token": "tok_visa"},
        )
        assert replay.status_code == 201
        assert replay.json()["id"] == body["id"]
        # Same key + different body is 409.
        clash = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-1"},
            json={"payment_token": "tok_other"},
        )
        assert clash.status_code == 409
        # Missing key is 422 (checkout refuses to run unguarded).
        missing = await client.post("/v1/checkout", headers=_auth(consumer), json={"payment_token": "tok_visa"})
        assert missing.status_code == 422
        # An oversized key is 422 (a 4xx the client fixes), never a DB DataError 500.
        oversized = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "k" * 201},
            json={"payment_token": "tok_visa"},
        )
        assert oversized.status_code == 422


async def test_checkout_stock_failure_is_409(app_ctx, rsa_key):
    app, sessionmaker = app_ctx
    consumer, product_id, _merchant = await _setup_cart(app, sessionmaker, rsa_key)
    async with sessionmaker() as session:
        await session.execute(text("UPDATE inventory.inventory SET on_hand = 0 WHERE sku = :sku"), {"sku": product_id})
        await session.commit()

    async with _client(app) as client:
        resp = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-short"},
            json={"payment_token": "tok_visa"},
        )
    assert resp.status_code == 409
    assert resp.headers["content-type"] == "application/problem+json"

    # Replaying a cancelled (stock-refused) checkout under the same key is a
    # 409 re-raise, never a 201 with a cancelled body — a retry needs a new key.
    async with _client(app) as client:
        replay = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-short"},
            json={"payment_token": "tok_visa"},
        )
    assert replay.status_code == 409
    assert replay.json()["status"] == 409


async def test_order_history_detail_cancel_and_ownership(app_ctx, rsa_key):
    app, sessionmaker = app_ctx
    consumer, _product_id, _merchant = await _setup_cart(app, sessionmaker, rsa_key)
    other = _make_token(rsa_key, roles=["consumer"])
    admin = _make_token(rsa_key, roles=["admin"])

    async with _client(app) as client:
        paid = await client.post(
            "/v1/checkout",
            headers={**_auth(consumer), "Idempotency-Key": "http-key-2"},
            json={"payment_token": "tok_visa"},
        )
        assert paid.status_code == 201
        order_id = paid.json()["id"]

        history = await client.get("/v1/orders", headers=_auth(consumer))
        assert history.status_code == 200
        assert [item["id"] for item in history.json()["items"]] == [order_id]
        assert "next_cursor" in history.json()

        filtered = await client.get("/v1/orders?status=paid", headers=_auth(consumer))
        assert filtered.status_code == 200 and len(filtered.json()["items"]) == 1
        empty = await client.get("/v1/orders?status=shipped", headers=_auth(consumer))
        assert empty.status_code == 200 and empty.json()["items"] == []
        unknown = await client.get("/v1/orders?bogus=1", headers=_auth(consumer))
        assert unknown.status_code == 400

        detail = await client.get(f"/v1/orders/{order_id}", headers=_auth(consumer))
        assert detail.status_code == 200
        forbidden = await client.get(f"/v1/orders/{order_id}", headers=_auth(other))
        assert forbidden.status_code == 403
        as_admin = await client.get(f"/v1/orders/{order_id}", headers=_auth(admin))
        assert as_admin.status_code == 200  # admin bypasses ownership

        # A paid order never cancels (refunds are future scope).
        cancel_paid = await client.post(f"/v1/orders/{order_id}/cancel", headers=_auth(consumer))
        assert cancel_paid.status_code == 409
        cancel_other = await client.post(f"/v1/orders/{order_id}/cancel", headers=_auth(other))
        assert cancel_other.status_code == 403


async def test_cancel_pending_order_releases_its_hold(app_ctx, rsa_key):
    app, sessionmaker = app_ctx
    consumer = _make_token(rsa_key, roles=["consumer"], sub="cancel-consumer", email="cancel@test.io")
    consumer, _product_id, _merchant = await _setup_cart(app, sessionmaker, rsa_key, consumer_token=consumer)
    async with sessionmaker() as session:
        user_id = (
            await session.execute(text("SELECT id FROM identity.users WHERE oidc_sub = 'cancel-consumer'"))
        ).scalar_one()
        order_id = uuid.uuid4()
        await session.execute(
            text(
                "INSERT INTO orders.orders (id, user_id, idempotency_key, idempotency_body_hash, status, total) "
                "VALUES (:id, :user_id, 'http-pending', 'hash', 'pending', 9.99)"
            ),
            {"id": order_id, "user_id": user_id},
        )
        await session.commit()

    async with _client(app) as client:
        resp = await client.post(f"/v1/orders/{order_id}/cancel", headers=_auth(consumer))
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "cancelled"
        again = await client.post(f"/v1/orders/{order_id}/cancel", headers=_auth(consumer))
        assert again.status_code == 200  # idempotent
        missing = await client.post(f"/v1/orders/{uuid.uuid4()}/cancel", headers=_auth(consumer))
        assert missing.status_code == 404
