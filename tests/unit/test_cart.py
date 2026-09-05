"""Cart module tests.

Route round-trips via ``httpx.AsyncClient`` over the ASGI app with
``dependency_overrides`` injecting an in-memory ``CartRepositoryPort`` fake
(the container docstring's advertised seam) — no Valkey needed. The fake
mirrors the Valkey adapter's Lua semantics Clamp-on-increment, cart-full,
absent-line, and the ``should_apply_update`` version gate, so the contract
tests pin the behavior both implementations must share.

The product-event consumer (``make_cart_handler``) is tested directly against
the same fake: refresh/prune, stale-version drops, and no-resurrect.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal
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
from src.cart.adapters.cart_consumer import make_cart_handler
from src.cart.application.service import CartService
from src.cart.domain.cart import Cart, CartLine, should_apply_update
from src.cart.ports.products import ProductSnapshot
from src.shared.config.setting import AppSettings, get_settings
from src.shared.container import get_cart_service
from src.shared.errors.exceptions import InvalidCartOperationError

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity"]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

_TRUNCATE = text("TRUNCATE identity.users CASCADE")

MAX_ITEMS = 2
MAX_PER_LINE = 3


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


# --- fakes ------------------------------------------------------------------


class FakeProducts:
    """In-memory ``CartProductPort``: known products snapshot, the rest 404."""

    def __init__(self, snapshots: dict[uuid.UUID, ProductSnapshot]) -> None:
        self.snapshots = snapshots

    async def get_snapshot(self, product_id: uuid.UUID) -> ProductSnapshot | None:
        return self.snapshots.get(product_id)


class FakeRepo:
    """In-memory ``CartRepositoryPort`` mirroring the Valkey Lua semantics."""

    def __init__(self) -> None:
        self.carts: dict[str, dict[str, dict]] = {}

    def _cart(self, user_id: uuid.UUID) -> Cart | None:
        lines = self.carts.get(str(user_id), {})
        if not lines:
            return None
        items = tuple(
            sorted(
                (
                    CartLine(
                        product_id=pid,
                        name=line["name"],
                        unit_price=line["unit_price"],
                        image_url=line["image_url"],
                        quantity=line["quantity"],
                        product_version=line["product_version"],
                    )
                    for pid, line in lines.items()
                ),
                key=lambda line: line.product_id,
            )
        )
        return Cart(user_id=str(user_id), items=items, updated_at="2026-01-01T00:00:00+00:00")

    async def get_cart(self, user_id: uuid.UUID) -> Cart | None:
        return self._cart(user_id)

    async def add_item(
        self, user_id, *, product_id, name, unit_price, image_url, quantity, max_items, max_per_line
    ) -> Cart:
        lines = self.carts.setdefault(str(user_id), {})
        pid = str(product_id)
        if pid in lines:
            lines[pid]["quantity"] = min(lines[pid]["quantity"] + quantity, max_per_line)
        else:
            if len(lines) >= max_items:
                raise InvalidCartOperationError(f"cart holds the maximum of {max_items} lines")
            lines[pid] = {
                "name": name,
                "unit_price": unit_price,
                "image_url": image_url,
                "quantity": min(quantity, max_per_line),
                "product_version": None,
            }
        result = self._cart(user_id)
        assert result is not None
        return result

    async def set_quantity(self, user_id, *, product_id, quantity, max_per_line) -> Cart | None:
        lines = self.carts.get(str(user_id), {})
        pid = str(product_id)
        if pid not in lines:
            return None
        if quantity == 0:
            del lines[pid]
        else:
            lines[pid]["quantity"] = quantity
        result = self._cart(user_id)
        if result is None:  # the op landed but emptied the cart — success, not absence
            return Cart(user_id=str(user_id), items=(), updated_at="2026-01-01T00:00:00+00:00")
        return result

    async def remove_item(self, user_id, *, product_id) -> Cart | None:
        self.carts.get(str(user_id), {}).pop(str(product_id), None)
        return self._cart(user_id)

    async def clear_cart(self, user_id) -> None:
        self.carts.pop(str(user_id), None)

    async def refresh_product(self, product_id, *, name, unit_price, product_version) -> int:
        touched = 0
        for lines in self.carts.values():
            line = lines.get(str(product_id))
            if line is None:
                continue
            if not should_apply_update(line["product_version"], product_version):
                continue
            line["name"] = name
            line["unit_price"] = unit_price
            if product_version is not None:
                line["product_version"] = product_version
            touched += 1
        return touched

    async def prune_product(self, product_id) -> int:
        touched = 0
        for lines in self.carts.values():
            if lines.pop(str(product_id), None) is not None:
                touched += 1
        return touched


@pytest.fixture
def fakes():
    pid = uuid.uuid4()
    products = FakeProducts(
        {pid: ProductSnapshot(product_id=pid, name="widget", unit_price=Decimal("9.99"), image_url="http://img/w.webp")}
    )
    return products, FakeRepo(), pid


@pytest.fixture
def app_ctx(_migrated, rsa_key, sessionmaker, fakes):
    products, repo, _ = fakes
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
    app.dependency_overrides[get_cart_service] = lambda: CartService(
        repo, products, max_items=MAX_ITEMS, max_qty_per_line=MAX_PER_LINE
    )
    return app


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# --- routes -----------------------------------------------------------------


async def test_empty_cart_is_200_never_404(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/cart", headers=_auth(token))
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == [] and "updated_at" in body


async def test_add_unknown_product_is_404(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.post(
            "/v1/cart/items", headers=_auth(token), json={"product_id": str(uuid.uuid4()), "quantity": 1}
        )
    assert resp.status_code == 404


async def test_add_out_of_range_quantity_is_400(app_ctx, rsa_key, fakes):
    _, _, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        for qty in (0, -1, MAX_PER_LINE + 1):
            resp = await client.post(
                "/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": qty}
            )
            assert resp.status_code == 400, qty


async def test_add_round_trips_snapshot_and_increments(app_ctx, rsa_key, fakes):
    _, _, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        first = await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        assert first.status_code == 200
        (line,) = first.json()["items"]
        assert line["product_id"] == str(pid) and line["quantity"] == 1
        assert line["name"] == "widget" and line["unit_price"] == "9.99"
        assert line["image_url"] == "http://img/w.webp"
        second = await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        assert second.json()["items"][0]["quantity"] == 2


async def test_add_increment_clamps_to_cap(app_ctx, rsa_key, fakes):
    _, _, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        await client.post(
            "/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": MAX_PER_LINE}
        )
        resp = await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 2})
        assert resp.status_code == 200
        assert resp.json()["items"][0]["quantity"] == MAX_PER_LINE


async def test_add_past_max_items_is_400(app_ctx, rsa_key, fakes):
    products, _, _ = fakes
    extra = [uuid.uuid4() for _ in range(MAX_ITEMS)]
    for pid in extra:
        products.snapshots[pid] = ProductSnapshot(product_id=pid, name="x", unit_price=Decimal("1.00"), image_url=None)
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        for pid in extra:
            resp = await client.post(
                "/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1}
            )
            assert resp.status_code == 200
        overflow = uuid.uuid4()
        products.snapshots[overflow] = ProductSnapshot(
            product_id=overflow, name="y", unit_price=Decimal("1.00"), image_url=None
        )
        resp = await client.post(
            "/v1/cart/items", headers=_auth(token), json={"product_id": str(overflow), "quantity": 1}
        )
        assert resp.status_code == 400


async def test_patch_sets_removes_and_404s(app_ctx, rsa_key, fakes):
    _, _, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        resp = await client.patch(f"/v1/cart/items/{pid}", headers=_auth(token), json={"quantity": 3})
        assert resp.status_code == 200
        assert resp.json()["items"][0]["quantity"] == 3
        bad = await client.patch(f"/v1/cart/items/{pid}", headers=_auth(token), json={"quantity": MAX_PER_LINE + 1})
        assert bad.status_code == 400
        missing = await client.patch(f"/v1/cart/items/{uuid.uuid4()}", headers=_auth(token), json={"quantity": 1})
        assert missing.status_code == 404
        removed = await client.patch(f"/v1/cart/items/{pid}", headers=_auth(token), json={"quantity": 0})
        assert removed.status_code == 200 and removed.json()["items"] == []
        zero_absent = await client.patch(f"/v1/cart/items/{pid}", headers=_auth(token), json={"quantity": 0})
        assert zero_absent.status_code == 404  # removing an absent line is 404, not a no-op 200


async def test_patch_product_gone_is_404_and_prunes(app_ctx, rsa_key, fakes):
    products, repo, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"], sub="gone-test")
    async with _client(app_ctx) as client:
        await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        del products.snapshots[pid]  # product soft-deleted after the add
        resp = await client.patch(f"/v1/cart/items/{pid}", headers=_auth(token), json={"quantity": 2})
        assert resp.status_code == 404
        got = await client.get("/v1/cart", headers=_auth(token))
        assert got.json()["items"] == []


async def test_delete_item_is_idempotent_and_clear_empties(app_ctx, rsa_key, fakes):
    _, _, pid = fakes
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        first = await client.delete(f"/v1/cart/items/{pid}", headers=_auth(token))
        assert first.status_code == 200 and first.json()["items"] == []
        second = await client.delete(f"/v1/cart/items/{pid}", headers=_auth(token))
        assert second.status_code == 200
        await client.post("/v1/cart/items", headers=_auth(token), json={"product_id": str(pid), "quantity": 1})
        cleared = await client.delete("/v1/cart", headers=_auth(token))
        assert cleared.status_code == 204
        got = await client.get("/v1/cart", headers=_auth(token))
        assert got.json()["items"] == []


# --- consumer ---------------------------------------------------------------


def _event(event_type: str, product_id: uuid.UUID, *, version: int | None = 2, **data) -> dict:
    payload: dict = {"product_id": str(product_id), "merchant_id": str(uuid.uuid4())}
    if event_type == "ProductUpdated":
        payload |= {"name": "widget-v2", "price": "12.50", "category": "tools"}
        if version is not None:
            payload["product_version"] = version
    elif version is not None:
        payload["product_version"] = version
    payload |= data
    return {"type": event_type, "data": payload}


async def test_consumer_refreshes_stale_guarded_and_prunes():
    repo, user, pid = FakeRepo(), uuid.uuid4(), uuid.uuid4()
    handler = make_cart_handler(repo)
    await repo.add_item(
        user,
        product_id=pid,
        name="widget",
        unit_price="9.99",
        image_url=None,
        quantity=1,
        max_items=10,
        max_per_line=10,
    )
    await handler(_event("ProductUpdated", pid, version=2))
    assert repo.carts[str(user)][str(pid)]["unit_price"] == "12.50"
    await handler(_event("ProductUpdated", pid, version=1))  # stale: dropped
    assert repo.carts[str(user)][str(pid)]["unit_price"] == "12.50"
    await handler(_event("ProductUpdated", pid, version=None))  # legacy v1: dropped, version known
    assert repo.carts[str(user)][str(pid)]["unit_price"] == "12.50"
    await handler(_event("ProductUpdated", pid, version=2))  # duplicate: dropped
    assert repo.carts[str(user)][str(pid)]["unit_price"] == "12.50"
    await handler(_event("ProductDeleted", pid, version=2))  # tombstone always wins
    assert str(pid) not in repo.carts[str(user)]
    await handler(_event("ProductUpdated", pid, version=3))  # never resurrects
    assert str(pid) not in repo.carts.get(str(user), {})
    await handler(_event("ProductDeleted", pid, version=2))  # replay: no-op
    assert str(pid) not in repo.carts.get(str(user), {})


async def test_consumer_v1_applies_to_versionless_line():
    repo, user, pid = FakeRepo(), uuid.uuid4(), uuid.uuid4()
    handler = make_cart_handler(repo)
    await repo.add_item(
        user,
        product_id=pid,
        name="widget",
        unit_price="9.99",
        image_url=None,
        quantity=1,
        max_items=10,
        max_per_line=10,
    )
    await handler(_event("ProductUpdated", pid, version=None))
    assert repo.carts[str(user)][str(pid)]["name"] == "widget-v2"
