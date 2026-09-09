"""Auth tests: OIDC resource-server validation, RBAC, JIT, admin.

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
from src.identity.application.outbox import user_created_outbox
from src.identity.domain.user import DirectoryUser
from src.shared.config.setting import AppSettings, get_settings
from src.shared.errors.exceptions import DependencyUnavailableError

REPO_ROOT = Path(__file__).resolve().parents[2]
ISSUER = "https://keycloak.test/realms/ecommerce"
AUDIENCE = "ecommerce-api"

# Canned directory subs for the admin listing tests (stable literals, not
# uuid4(), so a failing assertion shows a reproducible sub).
MERCHANT_SUB = "00000000-0000-0000-0000-000000000001"
DISABLED_SUB = "00000000-0000-0000-0000-000000000002"


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
        # Directory for the admin listing: 20 entries so the default page is a
        # FULL page (next_cursor set). One merchant; one disabled consumer with
        # no email (covers email=None + disabled = not enabled).
        self.directory: list[DirectoryUser] = [
            DirectoryUser(sub=MERCHANT_SUB, email="merchant@test.io", enabled=True),
            DirectoryUser(sub=DISABLED_SUB, email=None, enabled=False),
            *[DirectoryUser(sub=f"pad-{i:02d}", email=f"pad-{i:02d}@test.io", enabled=True) for i in range(18)],
        ]
        self.realm_roles: set[tuple[str, str]] = {(MERCHANT_SUB, "merchant")}
        self.listed: list[tuple[str | None, int, int]] = []

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

    async def list_users(self, search: str | None, first: int, max_results: int) -> list[DirectoryUser]:
        self.listed.append((search, first, max_results))
        return self.directory[first : first + max_results]

    async def has_realm_role(self, user_sub: str, role: str) -> bool:
        return (user_sub, role) in self.realm_roles


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


async def test_admin_sub_path_param_must_be_uuid(app_ctx, rsa_key):
    """regression: traversal / non-UUID ``{sub}`` never reaches the admin port.

    ``..%2F..%2Fevil`` is 404, not 422: the ASGI scope path is percent-decoded
    before routing, so the injected slash splits the segment and no route matches.
    Single-segment junk that *does* reach the route (``%2E%2E`` decodes to ``..``)
    is 422 on the UUID pattern.
    """
    token = _make_token(rsa_key, roles=["admin"])
    admin = app_ctx.state.identity_admin
    cases = [
        ("post", "/v1/admin/users/{sub}/roles/merchant", admin.granted),
        ("delete", "/v1/admin/users/{sub}/roles/merchant", admin.revoked),
        ("post", "/v1/admin/users/{sub}/disable", admin.enabled),
    ]
    async with _client(app_ctx) as client:
        for method, path, _ in cases:
            for sub, expected in (("%2E%2E", 422), ("..%2F..%2Fevil", 404), ("not-a-uuid", 422)):
                resp = await client.request(method, path.format(sub=sub), headers=_auth(token))
                assert resp.status_code == expected, (method, path, sub, resp.text)
    assert admin.granted == []
    assert admin.revoked == []
    assert admin.enabled == {}


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


async def test_admin_create_rejects_malformed_email_422(app_ctx, rsa_key):
    """A non-email body must be turned away at the schema (422), never reach
    Keycloak (whose 400 would otherwise have surfaced as a raw 500)."""
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.post(
            "/v1/admin/users",
            headers=_auth(token),
            json={"email": "not-an-email"},
        )
    assert resp.status_code == 422, resp.text
    assert app_ctx.state.identity_admin.created == []


async def test_admin_create_rejects_oversized_email_422(app_ctx, rsa_key):
    """300 chars blows past Keycloak's username/email column bound (255) —
    reject at the schema (422) instead of an opaque 500."""
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.post(
            "/v1/admin/users",
            headers=_auth(token),
            json={"email": "a" * 300 + "@x"},
        )
    assert resp.status_code == 422, resp.text
    assert app_ctx.state.identity_admin.created == []


async def test_disable_non_provisioned_user_still_blocks_future_login(app_ctx, rsa_key):
    """Disabling a user who never authenticated must not be a no-op."""
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


async def test_malformed_roles_claim_is_rejected(app_ctx, rsa_key):
    """`realm_access.roles` must be a list of strings — a loose parse is a role grant."""
    now = int(time.time())

    def _token(realm_access) -> str:
        return jwt.encode(
            {
                "sub": str(uuid.uuid4()),
                "email": "user@test.io",
                "iss": ISSUER,
                "aud": AUDIENCE,
                "iat": now,
                "exp": now + 300,
                "realm_access": realm_access,
            },
            _pem(rsa_key),
            algorithm="RS256",
        )

    hostile = [
        {"roles": {"admin": True}},  # dict → iterating keys would yield "admin"
        {"roles": "admin"},  # str → membership test on a string
        {"roles": [{"name": "admin"}]},  # list of non-strings
        "admin",  # realm_access itself not a mapping
        [],  # falsy non-mapping — must not be coerced to {}
        None,  # explicit null — malformed, not "no roles"
        {"roles": ""},  # falsy non-list
        {"roles": None},  # present but null
    ]
    async with _client(app_ctx) as client:
        for realm_access in hostile:
            resp = await client.get("/v1/internal/whoami", headers=_auth(_token(realm_access)))
            assert resp.status_code == 401, (realm_access, resp.text)


async def test_non_string_email_claim_is_rejected(app_ctx, rsa_key):
    now = int(time.time())
    token = jwt.encode(
        {
            "sub": str(uuid.uuid4()),
            "email": {"nested": "oops"},
            "iss": ISSUER,
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + 300,
            "realm_access": {"roles": ["consumer"]},
        },
        _pem(rsa_key),
        algorithm="RS256",
    )
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/me", headers=_auth(token))
    assert resp.status_code == 401


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
            return await IdentityRepository(session).get_or_create(sub, "race@test.io", user_created_outbox)

    a, b = await asyncio.gather(provision(), provision())
    assert a.id == b.id  # both observe the single row (one insert, one DO UPDATE)

    async with sessionmaker() as session:
        result = await session.execute(text("SELECT count(*) FROM identity.users WHERE oidc_sub = :s"), {"s": sub})
        count = result.scalar_one()
        events = await session.execute(
            text("SELECT count(*) FROM identity.outbox WHERE event_type = 'UserCreated' AND payload LIKE :p"),
            {"p": f'%"user_id":"{a.id}"%'},
        )
    assert count == 1
    # UserCreated is written in the same txn as the insert, and exactly once —
    # the loser of the ON CONFLICT race must not re-announce a creation.
    assert events.scalar_one() == 1


async def test_jit_recreated_keycloak_account_is_a_new_principal(sessionmaker):
    """Recycled email + new ``sub`` must get its own row, never inherit the old one."""
    email = f"recycled-{uuid.uuid4()}@test.io"
    async with sessionmaker() as session:
        first = await IdentityRepository(session).get_or_create(str(uuid.uuid4()), email, user_created_outbox)

    new_sub = str(uuid.uuid4())
    async with sessionmaker() as session:
        second = await IdentityRepository(session).get_or_create(new_sub, email, user_created_outbox)

    # A new Keycloak account is a new principal: separate row, so it cannot
    # inherit the previous holder's orders.user_id / products.merchant_id.
    assert second.id != first.id
    assert second.oidc_sub == new_sub


async def test_disable_blocks_concurrent_jit_from_seeing_an_active_user(sessionmaker):
    """JIT must wait out an in-flight disable, not be served as active meanwhile."""
    from src.identity.application.service import IdentityAdminService

    sub = str(uuid.uuid4())
    email = f"racing-{uuid.uuid4()}@test.io"

    class _SlowAdmin:
        async def set_enabled(self, _sub, _enabled): ...

        async def get_user_email(self, _sub):
            await asyncio.sleep(0.3)  # Keycloak round-trip, with the lock held
            return email

    async def disable():
        async with sessionmaker() as session:
            await IdentityAdminService(IdentityRepository(session), _SlowAdmin()).disable_user(sub)

    async def jit():
        await asyncio.sleep(0.05)  # arrives mid-disable
        async with sessionmaker() as session:
            return await IdentityRepository(session).get_or_create(sub, email, user_created_outbox)

    _, row = await asyncio.gather(disable(), jit())
    assert row.is_active is False  # blocked on the advisory lock, resumed to a disabled row


async def test_disable_fails_closed_when_keycloak_breaks(sessionmaker):
    """A Keycloak failure must never leave the mirror active for a disabled account."""
    from src.identity.application.service import IdentityAdminService

    # Read fails → nothing mutated anywhere, so the admin's retry is clean.
    class _BrokenRead:
        set_enabled_called = False

        async def get_user_email(self, _sub):
            raise RuntimeError("keycloak 500")

        async def set_enabled(self, _sub, _enabled):
            self.set_enabled_called = True

    sub = str(uuid.uuid4())
    admin = _BrokenRead()
    async with sessionmaker() as session:
        with pytest.raises(RuntimeError):
            await IdentityAdminService(IdentityRepository(session), admin).disable_user(sub)
    assert admin.set_enabled_called is False

    # Keycloak disable fails → the local mirror is already off, so tokens are dead here.
    class _BrokenWrite:
        async def get_user_email(self, _sub):
            return f"closed-{uuid.uuid4()}@test.io"

        async def set_enabled(self, _sub, _enabled):
            raise RuntimeError("keycloak 500")

    async with sessionmaker() as session:
        with pytest.raises(RuntimeError):
            await IdentityAdminService(IdentityRepository(session), _BrokenWrite()).disable_user(sub)
    async with sessionmaker() as session:
        row = await IdentityRepository(session).get_by_oidc_sub(sub)
    assert row is not None and row.is_active is False


async def test_disable_provisions_an_already_inactive_mirror(sessionmaker):
    """A disable for a never-seen user must never leave an active row behind."""
    sub = str(uuid.uuid4())
    async with sessionmaker() as session:
        row = await IdentityRepository(session).get_or_create(sub, f"off-{uuid.uuid4()}@test.io", is_active=False)
    assert row.is_active is False


# --- admin user directory (GET /v1/admin/users) ----------------------------


async def test_admin_listing_returns_first_page_and_cursors(app_ctx, rsa_key):
    """200: a full default page of 20 with exactly the four contract fields + next_cursor."""
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"items", "next_cursor"}
    assert len(body["items"]) == 20
    assert isinstance(body["next_cursor"], str) and body["next_cursor"]
    for item in body["items"]:
        assert set(item) == {"sub", "email", "merchant_role", "disabled"}
    by_sub = {item["sub"]: item for item in body["items"]}
    # disabled mirrors Keycloak enabled=false; email may be None.
    assert by_sub[DISABLED_SUB]["disabled"] is True
    assert by_sub[DISABLED_SUB]["email"] is None
    assert by_sub[MERCHANT_SUB]["disabled"] is False
    # merchant_role is resolved live from Keycloak role mappings, not token claims.
    assert by_sub[MERCHANT_SUB]["merchant_role"] is True
    assert by_sub[DISABLED_SUB]["merchant_role"] is False
    # Port received the default page window (offset 0, limit 20, no search).
    fake = app_ctx.state.identity_admin
    assert fake.listed == [(None, 0, 20)]
    # The returned cursor must translate to first=20 at the port on the next page.
    async with _client(app_ctx) as client:
        second = await client.get("/v1/admin/users", headers=_auth(token), params={"cursor": body["next_cursor"]})
    assert second.status_code == 200, second.text
    assert fake.listed[-1] == (None, 20, 20)
    # The directory holds exactly 20 users: page two is empty and terminal.
    assert second.json() == {"items": [], "next_cursor": None}


async def test_admin_listing_forwards_search(app_ctx, rsa_key):
    """``?search=`` is forwarded untouched to the Keycloak port."""
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token), params={"search": "alice"})
    assert resp.status_code == 200, resp.text
    assert app_ctx.state.identity_admin.listed == [("alice", 0, 20)]


async def test_admin_listing_rejects_consumer_403(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["consumer"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token))
    assert resp.status_code == 403
    body = resp.json()
    assert body["status"] == 403 and "type" in body
    # The gate fires before the handler: the directory was never touched.
    assert app_ctx.state.identity_admin.listed == []


async def test_admin_listing_requires_token_401(app_ctx):
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users")
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == "Bearer"
    assert app_ctx.state.identity_admin.listed == []


async def test_admin_listing_rejects_malformed_cursor_400(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token), params={"cursor": "garbage!!"})
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["status"] == 400 and "type" in body and "trace_id" in body


async def test_admin_listing_rejects_unknown_query_param_400(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token), params={"bogus": "1"})
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["status"] == 400
    assert "bogus" in body["detail"]
    # The guard raises before the service call.
    assert app_ctx.state.identity_admin.listed == []


async def test_admin_listing_rejects_limit_over_max_422(app_ctx, rsa_key):
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token), params={"limit": "101"})
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert isinstance(body["details"], list) and body["details"]


async def test_admin_listing_maps_keycloak_outage_to_503(app_ctx, rsa_key):
    """HTTP-layer proof: DependencyUnavailableError from the port surfaces as a 503 Problem Detail."""

    class _Outage(_FakeAdmin):
        async def list_users(self, search: str | None, first: int, max_results: int) -> list[DirectoryUser]:
            raise DependencyUnavailableError("Keycloak is unavailable")

    app_ctx.state.identity_admin = _Outage()
    token = _make_token(rsa_key, roles=["admin"])
    async with _client(app_ctx) as client:
        resp = await client.get("/v1/admin/users", headers=_auth(token))
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["status"] == 503 and "type" in body and "trace_id" in body


async def test_admin_listing_is_in_openapi(app_ctx):
    # The GET operation specifically (the POST create route shares this path).
    assert "get" in app_ctx.openapi()["paths"]["/v1/admin/users"]
