"""Catalog product CRUD + product-event tests (ticket 07).

HTTP round-trip via ``httpx.AsyncClient`` over the ASGI app, real
Testcontainers-Postgres (identity + catalog migrations), in-process RS256 keypair
+ fake JWKS (reused from the auth-test approach). Asserts the four acceptance
criteria: merchant-scoped CRUD (cross-merchant → 403), keyset/filter listing, the
``CHECK(price>0)`` guard, and a product event on the outbox per mutation.
"""

from __future__ import annotations

import json
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from src.app import create_app
from src.shared.config.setting import AppSettings, get_settings
from src.shared.container import get_image_store

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog"]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

_TRUNCATE = text("TRUNCATE catalog.products, catalog.outbox, identity.users CASCADE")


# --- keypair + token helpers (mirrors test_auth) --------------------------


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


# --- Postgres (identity + catalog) ----------------------------------------


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


async def _outbox(sessionmaker) -> list[dict]:
    async with sessionmaker() as s:
        rows = (await s.execute(text("SELECT event_type, payload FROM catalog.outbox ORDER BY occurred_at"))).all()
    return [{"event_type": r.event_type, "payload": json.loads(r.payload)} for r in rows]


_PRODUCT = {"name": "widget", "description": "d", "category": "tools", "price": "9.99"}


# --- create ---------------------------------------------------------------


async def test_merchant_create_persists_and_emits_event(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        resp = await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert Decimal(body["price"]) == Decimal("9.99")
        got = await client.get(f"/v1/products/{body['id']}", headers=_auth(token))
    assert got.status_code == 200
    events = await _outbox(sessionmaker)
    assert [e["event_type"] for e in events] == ["ProductCreated"]
    assert events[0]["payload"]["data"]["product_id"] == body["id"]
    assert events[0]["payload"]["data"]["merchant_id"] == body["merchant_id"]


async def test_create_rejects_non_positive_price(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        resp = await client.post("/v1/products", headers=_auth(token), json={**_PRODUCT, "price": "0"})
    assert resp.status_code == 422, resp.text


async def test_db_check_rejects_non_positive_price(sessionmaker):
    """Prove the ``CHECK(price>0)`` DB guard directly — the Pydantic 422 gate
    (``test_create_rejects_non_positive_price``) short-circuits before the DB, so
    the constraint itself is otherwise never exercised."""
    async with sessionmaker() as s:
        with pytest.raises(IntegrityError):
            await s.execute(
                text(
                    "INSERT INTO catalog.products (id, merchant_id, name, price) "
                    "VALUES (:id, :merchant_id, :name, :price)"
                ),
                {"id": uuid.uuid4(), "merchant_id": uuid.uuid4(), "name": "bad", "price": Decimal("0")},
            )
            await s.commit()


async def test_consumer_cannot_create_403(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)
    assert resp.status_code == 403


# --- listing --------------------------------------------------------------


async def test_listing_is_paginated_and_filterable(app_ctx, rsa_key):
    a = _make_token(rsa_key, roles=["merchant"])
    b = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        r1 = (await client.post("/v1/products", headers=_auth(a), json={**_PRODUCT, "name": "a1"})).json()
        await client.post("/v1/products", headers=_auth(a), json={**_PRODUCT, "name": "a2"})
        await client.post("/v1/products", headers=_auth(b), json={**_PRODUCT, "name": "b1"})
        mid = r1["merchant_id"]
        # filter by merchant → only merchant a's two products
        filtered = (await client.get(f"/v1/products?merchant_id={mid}", headers=_auth(a))).json()
        assert {p["name"] for p in filtered["items"]} == {"a1", "a2"}
        # keyset paging: limit 1 hands back a cursor that walks to the rest
        page1 = (await client.get(f"/v1/products?merchant_id={mid}&limit=1", headers=_auth(a))).json()
        assert len(page1["items"]) == 1 and page1["next_cursor"]
        page2 = (
            await client.get(f"/v1/products?merchant_id={mid}&limit=1&cursor={page1['next_cursor']}", headers=_auth(a))
        ).json()
    assert len(page2["items"]) == 1
    assert page1["items"][0]["id"] != page2["items"][0]["id"]


async def test_bad_sort_field_is_400(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/products?sort=secret_column", headers=_auth(token))
    assert resp.status_code == 400, resp.text


# --- update ---------------------------------------------------------------


async def test_owner_updates_bumps_version_and_emits(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        upd = await client.patch(f"/v1/products/{created['id']}", headers=_auth(token), json={"price": "12.50"})
    assert upd.status_code == 200, upd.text
    assert Decimal(upd.json()["price"]) == Decimal("12.50")
    async with sessionmaker() as s:
        version = (
            await s.execute(text("SELECT version_id FROM catalog.products WHERE id = :id"), {"id": created["id"]})
        ).scalar_one()
    assert version == 2  # ORM optimistic-lock column bumped by the update
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated", "ProductUpdated"]


async def test_cross_merchant_update_403(app_ctx, rsa_key):
    owner = _make_token(rsa_key, roles=["merchant"])
    other = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(owner), json=_PRODUCT)).json()
        resp = await client.patch(f"/v1/products/{created['id']}", headers=_auth(other), json={"price": "1.00"})
    assert resp.status_code == 403


async def test_admin_bypasses_ownership_on_update(app_ctx, rsa_key):
    owner = _make_token(rsa_key, roles=["merchant"])
    admin = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(owner), json=_PRODUCT)).json()
        resp = await client.patch(f"/v1/products/{created['id']}", headers=_auth(admin), json={"name": "renamed"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["name"] == "renamed"


async def test_update_missing_is_404(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        resp = await client.patch(f"/v1/products/{uuid.uuid4()}", headers=_auth(token), json={"price": "1.00"})
    assert resp.status_code == 404


# --- delete ---------------------------------------------------------------


async def test_owner_soft_deletes_and_emits(app_ctx, rsa_key, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        deleted = await client.delete(f"/v1/products/{created['id']}", headers=_auth(token))
        assert deleted.status_code == 204
        gone = await client.get(f"/v1/products/{created['id']}", headers=_auth(token))
    assert gone.status_code == 404  # soft-deleted rows are filtered out
    async with sessionmaker() as s:
        deleted_at = (
            await s.execute(text("SELECT deleted_at FROM catalog.products WHERE id = :id"), {"id": created["id"]})
        ).scalar_one()
    assert deleted_at is not None
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated", "ProductDeleted"]


async def test_cross_merchant_delete_403(app_ctx, rsa_key):
    owner = _make_token(rsa_key, roles=["merchant"])
    other = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(owner), json=_PRODUCT)).json()
        resp = await client.delete(f"/v1/products/{created['id']}", headers=_auth(other))
    assert resp.status_code == 403


# --- image upload presign -------------------------------------


class _FakeImageStore:
    """Records the presign call and returns a canned POST envelope (no real S3)."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def presign_upload(self, product_id, *, content_type, max_bytes, ttl_seconds):
        self.calls.append({"content_type": content_type, "max_bytes": max_bytes, "ttl": ttl_seconds})
        token = "deadbeef"
        key = f"uploads/{product_id}/{token}.bin"
        return {
            "url": "http://s3.test/ecommerce-uploads",
            "fields": {"key": key, "Content-Type": content_type, "policy": "x", "x-amz-signature": "y"},
            "key": key,
            "token": token,
        }


@pytest.fixture
def image_store(app_ctx):
    store = _FakeImageStore()
    app_ctx.dependency_overrides[get_image_store] = lambda: store
    yield store
    app_ctx.dependency_overrides.pop(get_image_store, None)


_PRESIGN = {"content_type": "image/jpeg", "content_length": 100_000}


async def test_presign_issued_after_validation_and_marks_pending(app_ctx, rsa_key, image_store, sessionmaker):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        resp = await client.post(f"/v1/products/{created['id']}/image:presign", headers=_auth(token), json=_PRESIGN)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["url"] and body["fields"] and body["key"].startswith(f"uploads/{created['id']}/")
    assert body["expires_in"] > 0
    assert image_store.calls == [
        {"content_type": "image/jpeg", "max_bytes": 5 * 1024 * 1024, "ttl": body["expires_in"]}
    ]
    async with sessionmaker() as s:
        status = (
            await s.execute(text("SELECT image_status FROM catalog.products WHERE id = :id"), {"id": created["id"]})
        ).scalar_one()
    assert status == "pending"  # worker flips to ready/failed later


async def test_presign_rejects_disallowed_content_type_400(app_ctx, rsa_key, image_store):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        resp = await client.post(
            f"/v1/products/{created['id']}/image:presign",
            headers=_auth(token),
            json={"content_type": "application/pdf", "content_length": 100},
        )
    assert resp.status_code == 400, resp.text
    assert image_store.calls == []  # rejected BEFORE any URL is minted (not an open uploader)


async def test_presign_rejects_oversize_400(app_ctx, rsa_key, image_store):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        resp = await client.post(
            f"/v1/products/{created['id']}/image:presign",
            headers=_auth(token),
            json={"content_type": "image/png", "content_length": 50 * 1024 * 1024},
        )
    assert resp.status_code == 400
    assert image_store.calls == []


async def test_presign_consumer_forbidden_403(app_ctx, rsa_key, image_store):
    owner = _make_token(rsa_key, roles=["merchant"])
    consumer = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(owner), json=_PRODUCT)).json()
        resp = await client.post(f"/v1/products/{created['id']}/image:presign", headers=_auth(consumer), json=_PRESIGN)
    assert resp.status_code == 403


async def test_presign_cross_merchant_403(app_ctx, rsa_key, image_store):
    owner = _make_token(rsa_key, roles=["merchant"])
    other = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(owner), json=_PRODUCT)).json()
        resp = await client.post(f"/v1/products/{created['id']}/image:presign", headers=_auth(other), json=_PRESIGN)
    assert resp.status_code == 403


async def test_presign_missing_product_404(app_ctx, rsa_key, image_store):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        resp = await client.post(f"/v1/products/{uuid.uuid4()}/image:presign", headers=_auth(token), json=_PRESIGN)
    assert resp.status_code == 404


# --- image worker state (token-guarded, stale-event safe) -----------------


async def _seed_pending(sessionmaker, upload_token: str) -> uuid.UUID:
    pid = uuid.uuid4()
    async with sessionmaker() as s:
        await s.execute(
            text(
                "INSERT INTO catalog.products "
                "(id, merchant_id, name, price, version_id, image_status, image_upload_token) "
                "VALUES (:id, :m, 'p', 9.99, 1, 'pending', :tok)"
            ),
            {"id": pid, "m": uuid.uuid4(), "tok": upload_token},
        )
        await s.commit()
    return pid


async def test_mark_image_ready_is_token_guarded(sessionmaker):
    """A stale event (superseded upload token) updates zero rows; the current one wins."""
    from src.catalog.adapters.db.repository import CatalogRepository

    pid = await _seed_pending(sessionmaker, "tokB")
    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        assert await repo.mark_image_ready(pid, "tokA", "public/stale.webp") is False  # stale → no-op
        assert await repo.mark_image_ready(pid, "tokB", "public/current.webp") is True  # current → applied
    async with sessionmaker() as s:
        row = (
            await s.execute(text("SELECT image_status, image_key FROM catalog.products WHERE id = :id"), {"id": pid})
        ).one()
    assert row.image_status == "ready"
    assert row.image_key == "public/current.webp"  # the stale event never clobbered it


async def test_mark_image_failed_is_token_guarded(sessionmaker):
    from src.catalog.adapters.db.repository import CatalogRepository

    pid = await _seed_pending(sessionmaker, "tokB")
    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        assert await repo.mark_image_failed(pid, "tokA") is False  # stale failure ignored
        assert await repo.mark_image_failed(pid, "tokB") is True
    async with sessionmaker() as s:
        status = (
            await s.execute(text("SELECT image_status FROM catalog.products WHERE id = :id"), {"id": pid})
        ).scalar_one()
    assert status == "failed"
