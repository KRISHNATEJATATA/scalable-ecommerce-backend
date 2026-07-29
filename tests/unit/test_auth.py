"""Auth tests (ticket 04): OIDC resource-server validation, RBAC, JIT, admin.

No Keycloak container: an RS256 keypair is generated in-process, test tokens are
signed with it, and JWKS signing-key resolution is replaced by a fake
``PyJWKClient`` serving the test public key on ``app.state.jwks_client``. DB-touching
paths (JIT provisioning, ``is_active`` mirror) run against Testcontainers-Postgres
(never SQLite). Admin Keycloak calls are replaced by an in-memory fake port.
"""

from __future__ import annotations

import asyncio
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
from src.identity.adapters.db.repository import IdentityRepository
from src.shared.config.setting import AppSettings, get_settings

REPO_ROOT = Path(__file__).resolve().parents[2]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"


# --- keypair + token helpers ----------------------------------------------


def _pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


class _FakeSigningKey:
    def __init__(self, public_key) -> None:
        self.key = public_key


class _FakeJWKClient:
    """Stand-in for ``PyJWKClient`` — resolves every token to the test public key."""

    def __init__(self, public_key) -> None:
        self._public_key = public_key

    def get_signing_key_from_jwt(self, _token: str) -> _FakeSigningKey:
        return _FakeSigningKey(self._public_key)


def _make_token(rsa_key, *, roles=(), email="user@test.io", sub=None, exp_delta=300, key=None, alg="RS256") -> str:
    now = int(time.time())
    claims = {
        "sub": sub or str(uuid.uuid4()),
        "email": email,
        "iss": ISSUER,
        "aud": AUDIENCE,
        "iat": now,
        "exp": now + exp_delta,
        "realm_access": {"roles": list(roles)},
    }
    signing = key if key is not None else _pem(rsa_key)
    if alg == "none":
        return jwt.encode(claims, key="", algorithm="none")
    return jwt.encode(claims, signing, algorithm=alg)


# --- fake admin port ------------------------------------------------------


class _FakeAdmin:
    def __init__(self) -> None:
        self.granted: list[tuple[str, str]] = []
        self.revoked: list[tuple[str, str]] = []
        self.enabled: dict[str, bool] = {}
        self.emails: dict[str, str] = {}
        self.created: list[tuple[str, str]] = []

    async def grant_realm_role(self, user_sub: str, role: str) -> None:
        self.granted.append((user_sub, role))

    async def revoke_realm_role(self, user_sub: str, role: str) -> None:
        self.revoked.append((user_sub, role))

    async def set_enabled(self, user_sub: str, enabled: bool) -> None:
        self.enabled[user_sub] = enabled

    async def get_user_email(self, user_sub: str) -> str | None:
        return self.emails.get(user_sub)

    async def create_user(self, email: str) -> str:
        sub = str(uuid.uuid4())
        self.created.append(email)
        self.emails[sub] = email
        return sub


# --- Postgres (identity module only) --------------------------------------


@pytest.fixture(scope="module")
def _migrated():
    with PostgresContainer("postgres:16-alpine") as pg:
        async_url = pg.get_connection_url(driver="asyncpg")
        old_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = async_url
        get_settings.cache_clear()
        try:
            subprocess.run(
                [sys.executable, "-m", "alembic", "-c", "src/identity/alembic.ini", "upgrade", "head"],
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
        await conn.execute(text("TRUNCATE identity.users CASCADE"))
    yield eng
    await eng.dispose()


@pytest.fixture
def sessionmaker(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def app_ctx(_migrated, rsa_key, sessionmaker):
    """Build the app with fake JWKS + admin + a real Postgres sessionmaker wired in."""
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
    app.state.identity_admin = _FakeAdmin()
    return app


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# --- token validation -----------------------------------------------------


async def test_valid_token_provisions_and_returns_me(app_ctx, rsa_key):
    sub = str(uuid.uuid4())
    token = _make_token(rsa_key, roles=["consumer"], email="alice@test.io", sub=sub)
    async with _client(app_ctx) as client:
        first = await client.get("/v1/me", headers=_auth(token))
        second = await client.get("/v1/me", headers=_auth(token))
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["oidc_sub"] == sub
    assert body["email"] == "alice@test.io"
    assert body["is_active"] is True
    # JIT is idempotent: same sub → same local row on reuse.
    assert second.json()["id"] == body["id"]


async def test_missing_token_is_401(app_ctx):
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/me")
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == "Bearer"


async def test_alg_none_is_401(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"], alg="none")
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/me", headers=_auth(token))
    assert resp.status_code == 401


async def test_tampered_signature_is_401(app_ctx):
    # Signed by a DIFFERENT key than the JWKS serves → signature verification fails.
    attacker = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _make_token(attacker, roles=["consumer"], key=_pem(attacker))
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/me", headers=_auth(token))
    assert resp.status_code == 401


async def test_expired_token_is_401(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"], exp_delta=-10)
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/me", headers=_auth(token))
    assert resp.status_code == 401


# --- RBAC / role gates ----------------------------------------------------


async def test_role_gate_rejects_non_admin_403(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.post(f"/v1/admin/users/{uuid.uuid4()}/roles/merchant", headers=_auth(token))
    assert resp.status_code == 403


async def test_admin_grants_merchant_role_204(app_ctx, rsa_key):
    target = str(uuid.uuid4())
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.post(f"/v1/admin/users/{target}/roles/merchant", headers=_auth(token))
    assert resp.status_code == 204
    assert (target, "merchant") in app_ctx.state.identity_admin.granted


async def test_require_role_admin_does_not_satisfy_other_role():
    """admin bypass is for ownership, NOT role membership — a merchant gate rejects admin."""
    from src.shared.auth.dependencies import require_role
    from src.shared.auth.principal import Principal
    from src.shared.errors.exceptions import AuthorizationError

    guard = require_role("merchant")
    admin = Principal(sub="x", email=None, roles=frozenset({"admin"}))
    with pytest.raises(AuthorizationError):
        await guard(admin)
    merchant = Principal(sub="y", email=None, roles=frozenset({"merchant"}))
    assert await guard(merchant) is merchant


# --- disable flips is_active + revocation ---------------------------------


async def test_disable_flips_is_active_and_blocks_me(app_ctx, rsa_key):
    sub = str(uuid.uuid4())
    user_token = _make_token(rsa_key, roles=["consumer"], sub=sub, email="bob@test.io")
    admin_token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        assert (await client.get("/v1/me", headers=_auth(user_token))).status_code == 200
        disabled = await client.post(f"/v1/admin/users/{sub}/disable", headers=_auth(admin_token))
        assert disabled.status_code == 204
        after = await client.get("/v1/me", headers=_auth(user_token))
    assert app_ctx.state.identity_admin.enabled[sub] is False
    # Local mirror flipped → disabled account is rejected with 403.
    assert after.status_code == 403


async def test_admin_creates_user_201(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.post(
            "/v1/admin/users",
            headers=_auth(token),
            json={"email": "new-user@test.io"},
        )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert "sub" in body
    assert "new-user@test.io" in app_ctx.state.identity_admin.created


async def test_disable_non_provisioned_user_still_blocks_future_login(app_ctx, rsa_key):
    """Disabling a user who never authenticated must not be a no-op (ticket 04)."""
    sub = str(uuid.uuid4())
    admin_token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        # Simulate Keycloak already knowing this (not-yet-provisioned) account.
        app_ctx.state.identity_admin.emails[sub] = "never-logged-in@test.io"
        disabled = await client.post(f"/v1/admin/users/{sub}/disable", headers=_auth(admin_token))
        assert disabled.status_code == 204
        # The account authenticates for the first time only after being disabled.
        late_token = _make_token(rsa_key, roles=["consumer"], sub=sub, email="never-logged-in@test.io")
        resp = await client.get("/v1/me", headers=_auth(late_token))
    assert resp.status_code == 403


# --- service role (machine-to-machine) -------------------------------------


async def test_service_role_can_call_internal_whoami(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["service"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/internal/whoami", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    assert resp.json()["role"] == "service"


async def test_non_service_role_rejected_from_internal_whoami(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/internal/whoami", headers=_auth(token))
    assert resp.status_code == 403


# --- JIT idempotency under concurrency ------------------------------------


async def test_jit_get_or_create_is_race_safe(sessionmaker):
    sub = str(uuid.uuid4())

    async def provision():
        # Each racing task gets its own session (mirrors real concurrent requests).
        async with sessionmaker() as session:
            return await IdentityRepository(session).get_or_create(sub, "race@test.io")

    a, b = await asyncio.gather(provision(), provision())
    assert a.id == b.id  # both observe the single row (one insert, one DO UPDATE)

    async with sessionmaker() as session:
        result = await session.execute(text("SELECT count(*) FROM identity.users WHERE oidc_sub = :s"), {"s": sub})
        count = result.scalar_one()
    assert count == 1
