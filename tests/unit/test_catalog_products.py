"""Catalog product CRUD + product-event tests.

HTTP round-trip via ``httpx.AsyncClient`` over the ASGI app, real
Testcontainers-Postgres (identity + catalog migrations), in-process RS256 keypair
+ fake JWKS (reused from the auth-test approach). Asserts the four acceptance
criteria: merchant-scoped CRUD (cross-merchant → 403), keyset/filter listing, the
``CHECK(price>0)`` guard, and a product event on the outbox per mutation.
"""

from __future__ import annotations

import asyncio
import base64
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
from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.application.outbox import product_updated_outbox
from src.catalog.ports.repository import PendingUpload, ProductRecord
from src.shared.config.setting import AppSettings, get_settings
from src.shared.container import get_image_store
from src.shared.errors.exceptions import ConcurrentUpdateError

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES = ["identity", "catalog", "inventory"]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

# Inventory rides along: product reads compose `available` through the inventory
# service, so the stock table must exist (prod migrates every module together).
_TRUNCATE = text(
    "TRUNCATE catalog.products, catalog.outbox, identity.users, "
    "inventory.reservations, inventory.inventory, inventory.outbox CASCADE"
)


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


async def test_unsupported_filter_is_rejected_not_ignored(app_ctx, rsa_key):
    """An undeclared query param would otherwise be silently ignored, answering an
    unfiltered page to a filtered question."""
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/products?price=5", headers=_auth(token))
    assert resp.status_code == 400, resp.text


async def test_structurally_valid_but_uncastable_cursor_is_400(app_ctx, rsa_key):
    """A cursor that decodes cleanly but carries values no column can cast must be a
    purpose-named 400, not a 500 from a failed Postgres CAST."""
    token = _make_token(rsa_key, roles=["consumer"])
    bad = base64.urlsafe_b64encode(json.dumps(["invalid", "invalid"]).encode()).decode()
    async with _client(app_ctx) as client:
        resp = await client.get(f"/v1/products?cursor={bad}", headers=_auth(token))
    assert resp.status_code == 400, resp.text


# --- search ----------------------------------------------------


async def test_search_matches_name_and_description_case_insensitively(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        await client.post(
            "/v1/products",
            headers=_auth(token),
            json={**_PRODUCT, "name": "Silk Scarf", "description": "hand-rolled edges"},
        )
        await client.post(
            "/v1/products",
            headers=_auth(token),
            json={**_PRODUCT, "name": "Wool Coat", "description": "waterfall SILK lining"},
        )
        await client.post(
            "/v1/products",
            headers=_auth(token),
            json={**_PRODUCT, "name": "Leather Bag", "description": "pebbled hide"},
        )
        resp = await client.get("/v1/products?search=silk", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    assert {p["name"] for p in resp.json()["items"]} == {"Silk Scarf", "Wool Coat"}


async def test_search_wildcards_match_literally(app_ctx, rsa_key):
    """``%``/``_`` in the term are escaped, so they match themselves — ``_``
    never degrades into the one-char wildcard (else ``a_b`` would match ``axb``)."""
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        await client.post("/v1/products", headers=_auth(token), json={**_PRODUCT, "name": "100%_wool"})
        await client.post("/v1/products", headers=_auth(token), json={**_PRODUCT, "name": "100x wool"})
        await client.post("/v1/products", headers=_auth(token), json={**_PRODUCT, "name": "a_b"})
        await client.post("/v1/products", headers=_auth(token), json={**_PRODUCT, "name": "axb"})
        percent = await client.get("/v1/products?search=100%25", headers=_auth(token))
        underscore = await client.get("/v1/products?search=a%5Fb", headers=_auth(token))
    assert {p["name"] for p in percent.json()["items"]} == {"100%_wool"}
    assert {p["name"] for p in underscore.json()["items"]} == {"a_b"}


async def test_search_composes_with_filters_and_pagination(app_ctx, rsa_key):
    a = _make_token(rsa_key, roles=["merchant"])
    b = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        first = (
            await client.post("/v1/products", headers=_auth(a), json={**_PRODUCT, "name": "alpha one"})
        ).json()
        mid_a = first["merchant_id"]
        for name in ("alpha two", "beta three"):
            await client.post("/v1/products", headers=_auth(a), json={**_PRODUCT, "name": name})
        await client.post("/v1/products", headers=_auth(b), json={**_PRODUCT, "name": "alpha nine"})

        scoped = (await client.get(f"/v1/products?search=alpha&merchant_id={mid_a}", headers=_auth(a))).json()
        assert {p["name"] for p in scoped["items"]} == {"alpha one", "alpha two"}

        # Walk the searched set one row per page: exactly the three alpha rows,
        # no dups, no skips, cursor terminates.
        walked: list[str] = []
        cursor: str | None = None
        for _ in range(4):
            url = "/v1/products?search=alpha&limit=1" + (f"&cursor={cursor}" if cursor else "")
            page = (await client.get(url, headers=_auth(a))).json()
            walked.extend(p["name"] for p in page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
        miss = (await client.get("/v1/products?search=nonexistent", headers=_auth(a))).json()
    assert set(walked) == {"alpha one", "alpha two", "alpha nine"} and len(walked) == 3
    assert miss["items"] == [] and miss["next_cursor"] is None


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


async def test_empty_patch_is_422_and_emits_no_event(app_ctx, rsa_key, sessionmaker):
    """``{}`` changes nothing, so it must not publish a ``ProductUpdated`` event —
    consumers would act on a state transition that never happened."""
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        resp = await client.patch(f"/v1/products/{created['id']}", headers=_auth(token), json={})
    assert resp.status_code == 422, resp.text
    assert resp.headers["content-type"].startswith("application/problem+json")  # matches the published contract
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated"]


async def test_explicit_null_on_a_not_null_field_is_422(app_ctx, rsa_key, sessionmaker):
    """``{"name": null}``/``{"price": null}`` target NOT-NULL columns — reject at the
    boundary (422) rather than let the DB raise a NOT-NULL violation (500)."""
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        for body in ({"name": None}, {"price": None}):
            resp = await client.patch(f"/v1/products/{created['id']}", headers=_auth(token), json=body)
            assert resp.status_code == 422, resp.text
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated"]


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


async def test_delete_missing_or_already_deleted_is_404(app_ctx, rsa_key, sessionmaker):
    """A never-existed id and an already-soft-deleted one are the same 404 — the
    second delete must not re-emit ``ProductDeleted`` (consumers would tombstone twice)."""
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        missing = await client.delete(f"/v1/products/{uuid.uuid4()}", headers=_auth(token))
        assert missing.status_code == 404

        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        assert (await client.delete(f"/v1/products/{created['id']}", headers=_auth(token))).status_code == 204
        again = await client.delete(f"/v1/products/{created['id']}", headers=_auth(token))
    assert again.status_code == 404  # soft-deleted rows are invisible to the repository
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated", "ProductDeleted"]


# --- optimistic locking ---------------------------------------------------


async def test_concurrent_updates_lose_the_race_with_409_not_500(app_ctx, rsa_key, sessionmaker):
    """Two merchants patching the same product: the loser gets a retryable 409.

    ``version_id`` optimistic locking is what stops the second writer silently
    clobbering the first. Its ``StaleDataError`` must be translated at the adapter
    boundary — untranslated it falls through to the 500 handler, which tells the
    caller "server bug, don't retry" about a perfectly retryable conflict.
    Each racing task gets its **own** session, mirroring two concurrent requests.
    """
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
    product_id = uuid.UUID(created["id"])

    async def patch(name: str):
        # Both writers load the row at version 1 before either commits.
        async with sessionmaker() as session:
            repo = CatalogRepository(session)
            product = await repo.get_product(product_id)
            # The real adapter row must satisfy the port the use-cases code against
            # (`version_id` lives only on the ORM model — see ProductRecord).
            assert isinstance(product, ProductRecord)
            await barrier.wait()
            return await repo.update_product(
                product,
                {"name": name},
                product_updated_outbox(
                    product_id=product_id,
                    merchant_id=product.merchant_id,
                    name=name,
                    price=product.price,
                    category=product.category,
                    product_version=product.version_id + 1,
                ),
            )

    barrier = asyncio.Barrier(2)
    results = await asyncio.gather(patch("winner"), patch("loser"), return_exceptions=True)

    conflicts = [r for r in results if isinstance(r, ConcurrentUpdateError)]
    winners = [r for r in results if not isinstance(r, BaseException)]
    assert len(winners) == 1, results  # exactly one commit lands
    assert len(conflicts) == 1, results
    assert "retry" in conflicts[0].detail  # the caller is told this is retryable

    async with sessionmaker() as s:
        version = (
            await s.execute(text("SELECT version_id FROM catalog.products WHERE id = :id"), {"id": product_id})
        ).scalar_one()
    assert version == 2  # one bump, not two — the loser's write never committed
    # ...and the loser published nothing: the outbox row rolled back with it.
    assert [e["event_type"] for e in await _outbox(sessionmaker)] == ["ProductCreated", "ProductUpdated"]


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
    # The policy is pinned to the *declared* size (clamped by the server cap), so a
    # ticket for 100 KB can't be used to push 5 MiB.
    assert image_store.calls == [
        {"content_type": "image/jpeg", "max_bytes": _PRESIGN["content_length"], "ttl": body["expires_in"]}
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


async def test_product_events_carry_a_monotonic_aggregate_version(app_ctx, rsa_key, sessionmaker):
    """Every product event must carry the post-write ``version_id``, strictly rising.

    SNS is unordered and the relay publishes a batch concurrently, so a projector
    can see an older update after a newer one — or after the delete. ``event_id``
    dedup only kills exact redeliveries and ``schema_version`` versions the
    contract, so without this counter a stale update would restore an old price or
    resurrect a deleted product. The delete tombstone is on the same counter.
    """
    token = _make_token(rsa_key, roles=["merchant"])
    async with _client(app_ctx) as client:
        created = (await client.post("/v1/products", headers=_auth(token), json=_PRODUCT)).json()
        await client.patch(f"/v1/products/{created['id']}", headers=_auth(token), json={"price": "12.50"})
        await client.patch(f"/v1/products/{created['id']}", headers=_auth(token), json={"name": "renamed"})
        assert (await client.delete(f"/v1/products/{created['id']}", headers=_auth(token))).status_code == 204

    events = await _outbox(sessionmaker)
    assert [e["event_type"] for e in events] == [
        "ProductCreated",
        "ProductUpdated",
        "ProductUpdated",
        "ProductDeleted",
    ]
    versions = [e["payload"]["data"]["product_version"] for e in events]
    assert versions == [1, 2, 3, 4]  # matches the row's version_id after each write


async def test_image_flip_bumps_the_version_it_publishes(sessionmaker):
    """The image flips are raw SQL (no ORM unit-of-work), so they must bump
    ``version_id`` themselves — otherwise two flips publish the same version and a
    projector cannot order them against a concurrent merchant edit."""

    pid = await _seed_pending(sessionmaker, "tokB")  # seeded at version_id = 1
    seen_rows = []

    def outbox(row):
        seen_rows.append(dict(row))
        return ("ProductUpdated", json.dumps({"type": "ProductUpdated"}))

    async with sessionmaker() as s:
        assert (
            await CatalogRepository(s).mark_image_ready(pid, "tokB", "public/x.webp", outbox=outbox)
        ).applied is True
    async with sessionmaker() as s:
        version = (
            await s.execute(text("SELECT version_id FROM catalog.products WHERE id = :id"), {"id": pid})
        ).scalar_one()

    assert version == 2
    assert seen_rows[0]["product_version"] == 2  # RETURNING carries the post-increment value


async def test_mark_image_ready_is_token_guarded(sessionmaker):
    """A stale event (superseded upload token) updates zero rows; the current one wins."""

    pid = await _seed_pending(sessionmaker, "tokB")
    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        assert (await repo.mark_image_ready(pid, "tokA", "public/stale.webp")).applied is False  # stale → no-op
        assert (await repo.mark_image_ready(pid, "tokB", "public/current.webp")).applied is True  # current → applied
    async with sessionmaker() as s:
        row = (
            await s.execute(text("SELECT image_status, image_key FROM catalog.products WHERE id = :id"), {"id": pid})
        ).one()
    assert row.image_status == "ready"
    assert row.image_key == "public/current.webp"  # the stale event never clobbered it


async def test_mark_image_ready_returns_the_key_it_replaced(sessionmaker):
    """The flip reports the superseded ``image_key`` so the worker can reclaim its
    renditions — ``public/`` is outside the ``uploads/`` lifecycle rule, so nothing
    else would, and every re-upload would leak a main image plus two thumbnails."""

    pid = await _seed_pending(sessionmaker, "tokB")
    async with sessionmaker() as s:
        first = await CatalogRepository(s).mark_image_ready(pid, "tokB", "public/first.webp")
    assert first == (True, None)  # nothing replaced on the very first image

    async with sessionmaker() as s:  # merchant re-uploads: back to pending, new token
        await s.execute(
            text("UPDATE catalog.products SET image_status = 'pending', image_upload_token = 'tokC' WHERE id = :id"),
            {"id": pid},
        )
        await s.commit()
    async with sessionmaker() as s:
        second = await CatalogRepository(s).mark_image_ready(pid, "tokC", "public/second.webp")
    assert second == (True, "public/first.webp")

    async with sessionmaker() as s:  # ...and queued the replaced key for deletion, in that same txn
        queued = (
            await s.execute(
                text("SELECT object_key, attempts FROM catalog.image_reclaim WHERE product_id = :id"), {"id": pid}
            )
        ).all()
    assert [(r.object_key, r.attempts) for r in queued] == [("public/first.webp", 0)]


async def test_image_reclaim_is_leased_on_claim_and_dropped_when_finished(sessionmaker):
    """The cleanup queue behaves like a lease: a claim bumps ``attempts`` and pushes
    ``next_attempt_at`` out, so a crashed sweep retries instead of leaking the object,
    and a second worker polling meanwhile claims nothing (no double-delete storm)."""

    pid = await _seed_pending(sessionmaker, "tokR")
    async with sessionmaker() as s:
        await CatalogRepository(s).schedule_image_reclaim(pid, "public/old.webp")
        await CatalogRepository(s).schedule_image_reclaim(pid, "public/old.webp")  # idempotent

    async with sessionmaker() as s:
        claimed = [t for t in await CatalogRepository(s).claim_image_reclaims(batch_size=50) if t.product_id == pid]
    assert [(t.object_key, t.attempts) for t in claimed] == [("public/old.webp", 1)]

    async with sessionmaker() as s:  # leased → invisible to the next sweep
        again = await CatalogRepository(s).claim_image_reclaims(batch_size=50)
    assert [t for t in again if t.product_id == pid] == []

    async with sessionmaker() as s:
        await CatalogRepository(s).finish_image_reclaim([claimed[0].id])
    async with sessionmaker() as s:
        left = (
            await s.execute(text("SELECT count(*) FROM catalog.image_reclaim WHERE product_id = :id"), {"id": pid})
        ).scalar_one()
    assert left == 0


async def test_deferring_a_reclaim_records_why_and_retries_later(sessionmaker):
    pid = await _seed_pending(sessionmaker, "tokD")
    async with sessionmaker() as s:
        await CatalogRepository(s).schedule_image_reclaim(pid, "public/stuck.webp")
    async with sessionmaker() as s:
        task = next(
            t
            for t in await CatalogRepository(s).claim_image_reclaims(batch_size=50)
            if t.object_key == "public/stuck.webp"
        )
    async with sessionmaker() as s:
        await CatalogRepository(s).defer_image_reclaim(task.id, delay_seconds=60, error="RuntimeError: s3 down")
    async with sessionmaker() as s:
        row = (
            await s.execute(
                text("SELECT last_error, next_attempt_at > now() AS pending FROM catalog.image_reclaim WHERE id = :id"),
                {"id": task.id},
            )
        ).one()
    assert row.last_error == "RuntimeError: s3 down" and row.pending


async def _expire_presign(sessionmaker, pid: uuid.UUID, *, seconds_ago: int) -> None:
    async with sessionmaker() as s:
        await s.execute(
            text(
                "UPDATE catalog.products SET image_upload_expires_at = now() - make_interval(secs => :ago) "
                "WHERE id = :id"
            ),
            {"id": pid, "ago": seconds_ago},
        )
        await s.commit()


async def _image_state(sessionmaker, pid: uuid.UUID):
    async with sessionmaker() as s:
        return (
            await s.execute(
                text(
                    "SELECT image_status, image_key, image_upload_token, image_upload_expires_at, version_id "
                    "FROM catalog.products WHERE id = :id"
                ),
                {"id": pid},
            )
        ).one()


async def test_abandoned_upload_is_reaped_back_to_its_previous_image(sessionmaker):
    """A presign that is never used must not strip the product of the image it was
    already serving: presigning drops ``image_url`` immediately, and only an upload
    event ever moves the row out of ``pending``."""

    pid = await _seed_pending(sessionmaker, "tokE")
    async with sessionmaker() as s:  # it had a ready image before the re-upload attempt
        await s.execute(text("UPDATE catalog.products SET image_key = 'public/old.webp' WHERE id = :id"), {"id": pid})
        await s.commit()
    await _expire_presign(sessionmaker, pid, seconds_ago=1000)

    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        assert PendingUpload(pid, "tokE") in await repo.due_pending_uploads(grace_seconds=900, batch_size=100)
        assert await repo.expire_abandoned_upload(pid, "tokE")

    row = await _image_state(sessionmaker, pid)
    assert row.image_status == "ready" and row.image_key == "public/old.webp"
    assert row.image_upload_token is None and row.image_upload_expires_at is None
    assert row.version_id == 2, "the image state changed, so the aggregate version must move"


async def test_abandoned_first_upload_is_reaped_to_none(sessionmaker):
    pid = await _seed_pending(sessionmaker, "tokF")  # never had an image
    await _expire_presign(sessionmaker, pid, seconds_ago=1000)
    async with sessionmaker() as s:
        await CatalogRepository(s).expire_abandoned_upload(pid, "tokF")
    assert (await _image_state(sessionmaker, pid)).image_status == "none"


async def test_expiry_is_token_guarded_against_an_upload_that_landed_meanwhile(sessionmaker):
    """Candidates are read without a lock, so the restore is a compare-and-set: if the
    upload completed (or the merchant re-presigned) between the read and the probe,
    it must update nothing rather than undo newer state."""

    pid = await _seed_pending(sessionmaker, "tokF2")
    await _expire_presign(sessionmaker, pid, seconds_ago=1000)
    async with sessionmaker() as s:
        assert not await CatalogRepository(s).expire_abandoned_upload(pid, "staletok")
    assert (await _image_state(sessionmaker, pid)).image_status == "pending"


async def test_deferring_keeps_the_row_pending_and_moves_the_deadline_out(sessionmaker):
    """The raw object exists — only the event is late — so the token the eventual
    flip is guarded on must survive, and the row must stop being a candidate."""

    pid = await _seed_pending(sessionmaker, "tokF3")
    await _expire_presign(sessionmaker, pid, seconds_ago=1000)
    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        await repo.defer_upload_expiry(pid, "tokF3", delay_seconds=3600)
    async with sessionmaker() as s:
        due = await CatalogRepository(s).due_pending_uploads(grace_seconds=900, batch_size=50)
    assert pid not in [p.product_id for p in due]
    row = await _image_state(sessionmaker, pid)
    assert row.image_status == "pending" and row.image_upload_token == "tokF3"


async def test_an_upload_inside_the_grace_window_is_left_alone(sessionmaker):
    """The grace covers bytes that landed just before the presign died and are still
    queued or mid-processing — reaping those would clear the token their flip is
    guarded on and revert a perfectly good upload."""

    pid = await _seed_pending(sessionmaker, "tokG")
    await _expire_presign(sessionmaker, pid, seconds_ago=60)
    async with sessionmaker() as s:
        due = await CatalogRepository(s).due_pending_uploads(grace_seconds=900, batch_size=100)
    assert pid not in [p.product_id for p in due]
    row = await _image_state(sessionmaker, pid)
    assert row.image_status == "pending" and row.image_upload_token == "tokG"


async def test_reaping_emits_a_product_updated_row(sessionmaker):
    """``image_url`` reappears, so the read-cache must be invalidated like any other
    image transition."""

    pid = await _seed_pending(sessionmaker, "tokH")
    await _expire_presign(sessionmaker, pid, seconds_ago=1000)
    seen: list[dict] = []

    def outbox(row):
        seen.append(dict(row))
        return ("ProductUpdated", json.dumps({"product_id": str(row["product_id"])}))

    async with sessionmaker() as s:
        await CatalogRepository(s).expire_abandoned_upload(pid, "tokH", outbox=outbox)
    assert any(r["product_id"] == pid for r in seen)
    assert all(r["product_version"] == 2 for r in seen if r["product_id"] == pid)


async def test_a_ready_flip_clears_the_upload_deadline(sessionmaker):
    pid = await _seed_pending(sessionmaker, "tokI")
    await _expire_presign(sessionmaker, pid, seconds_ago=0)
    async with sessionmaker() as s:
        assert (await CatalogRepository(s).mark_image_ready(pid, "tokI", "public/new.webp")).applied
    row = await _image_state(sessionmaker, pid)
    assert row.image_upload_expires_at is None, "a completed upload must not stay reapable"


async def test_mark_image_ready_emits_outbox_only_when_applied(sessionmaker):
    """A landed image-ready flip writes its ProductUpdated outbox row in the same
    txn (so the read-cache is invalidated); a stale (superseded) flip writes none.

    The payload is built from the UPDATE's own ``RETURNING`` row, so it can't carry
    state read before the write."""

    pid = await _seed_pending(sessionmaker, "tokB")
    seen_rows = []

    def outbox(row):
        seen_rows.append(dict(row))
        return ("ProductUpdated", json.dumps({"type": "ProductUpdated", "name": row["name"]}))

    async with sessionmaker() as s:
        repo = CatalogRepository(s)
        assert (await repo.mark_image_ready(pid, "tokA", "public/stale.webp", outbox=outbox)).applied is False  # stale
        assert (
            await repo.mark_image_ready(pid, "tokB", "public/current.webp", outbox=outbox)
        ).applied is True  # applied
    async with sessionmaker() as s:
        row = (
            await s.execute(
                text(
                    "SELECT count(*) AS n, min(payload) AS payload FROM catalog.outbox "
                    "WHERE event_type = 'ProductUpdated'"
                )
            )
        ).one()
    assert row.n == 1  # exactly one outbox row — only the applied flip emitted
    assert len(seen_rows) == 1 and seen_rows[0]["product_id"] == pid  # factory fed the updated row
    assert json.loads(row.payload)["name"] == seen_rows[0]["name"]

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
