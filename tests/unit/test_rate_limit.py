"""App-level Valkey token-bucket rate limiting.

Two layers of proof:

- The atomic Lua bucket against a **real Valkey** (the ``real_valkey`` Testcontainer),
  with ``now_us`` injected so refill/recovery are deterministic (no wall-clock sleeps):
  capacity is the burst ceiling, refill restores tokens, ``retry_after`` names when the
  next token lands, and distinct keys are independent.
- The FastAPI dependencies through an ``httpx`` ASGI round-trip: exceeding a limit is a
  **429 RFC 9457 Problem** with a ``Retry-After`` header; buckets are keyed by the
  authenticated ``sub`` (one caller can't drain another's budget); the unauthenticated
  fallback keys by client IP; and every degrade path (flag off, no client, Valkey fault)
  **fails open** rather than refusing a valid request.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import Depends, FastAPI

from src.shared.auth.dependencies import get_current_user
from src.shared.auth.principal import Principal
from src.shared.config.setting import AppSettings
from src.shared.errors.exception_handlers import register_exception_handlers
from src.shared.middleware.security import RequestIDMiddleware
from src.shared.ratelimit import (
    BUCKET_CHECKOUT,
    BUCKET_UPLOAD,
    RateLimitConfig,
    ValkeyTokenBucket,
    ip_rate_limited,
    rate_limited,
)

# A fixed base timestamp (µs epoch) so refill math is exact; the bucket only ever sees
# the ``now_us`` we pass, never the wall clock.
_T0 = 1_700_000_000_000_000
_SECOND = 1_000_000


# --- token bucket vs real Valkey -------------------------------------------------


async def test_capacity_is_the_burst_ceiling(real_valkey):
    bucket = ValkeyTokenBucket(real_valkey)
    cfg = RateLimitConfig(capacity=3, refill=1, period_seconds=60)
    outcomes = [await bucket.allow(key="ratelimit:write:sub-a", config=cfg, now_us=_T0) for _ in range(4)]
    assert [d.allowed for d in outcomes] == [True, True, True, False]
    # remaining counts down to the empty bucket that refuses the fourth call
    assert [d.remaining for d in outcomes[:3]] == [2, 1, 0]


async def test_refill_restores_tokens_over_time(real_valkey):
    bucket = ValkeyTokenBucket(real_valkey)
    cfg = RateLimitConfig(capacity=1, refill=1, period_seconds=1)  # one token per second
    key = "ratelimit:checkout:sub-a"
    assert (await bucket.allow(key=key, config=cfg, now_us=_T0)).allowed
    assert not (await bucket.allow(key=key, config=cfg, now_us=_T0)).allowed  # burst spent
    # exactly one period later, one token has accrued and is spendable again
    assert (await bucket.allow(key=key, config=cfg, now_us=_T0 + _SECOND)).allowed


async def test_retry_after_reports_time_to_next_token(real_valkey):
    bucket = ValkeyTokenBucket(real_valkey)
    cfg = RateLimitConfig(capacity=1, refill=1, period_seconds=10)
    key = "ratelimit:upload:sub-a"
    assert (await bucket.allow(key=key, config=cfg, now_us=_T0)).allowed
    denied = await bucket.allow(key=key, config=cfg, now_us=_T0)  # empty, must wait a full period
    assert not denied.allowed
    assert denied.retry_after_seconds == 10


async def test_distinct_keys_are_independent_buckets(real_valkey):
    bucket = ValkeyTokenBucket(real_valkey)
    cfg = RateLimitConfig(capacity=1, refill=1, period_seconds=60)
    assert (await bucket.allow(key="ratelimit:write:sub-a", config=cfg, now_us=_T0)).allowed
    # a different subject's bucket is untouched by sub-a's spend
    assert (await bucket.allow(key="ratelimit:write:sub-b", config=cfg, now_us=_T0)).allowed


# --- dependencies via an HTTP round-trip ----------------------------------------


def _settings(**rl) -> AppSettings:
    return AppSettings(_env_file=None, database_url="postgresql+asyncpg://u:p@localhost:5432/d", **rl)


def _build_app(valkey, *, with_user_route: bool = True, **rl) -> FastAPI:
    """A bare app with the RFC 9457 handlers + one rate-limited route per keying style.

    Kept intentionally small (no DB, no lifespan): the bucket only needs
    ``app.state.settings`` and ``app.state.valkey``, exactly as the real app wires them.
    """
    app = FastAPI()
    app.state.settings = _settings(**rl)
    app.state.valkey = valkey
    app.add_middleware(RequestIDMiddleware)  # so the Problem body carries a real trace_id, as in production
    register_exception_handlers(app)
    if with_user_route:
        app.add_api_route(
            "/limited", lambda: {"ok": True}, methods=["POST"], dependencies=[Depends(rate_limited(BUCKET_CHECKOUT))]
        )
    app.add_api_route(
        "/public", lambda: {"ok": True}, methods=["POST"], dependencies=[Depends(ip_rate_limited(BUCKET_UPLOAD))]
    )
    return app


def _as_principal(sub: str):
    return lambda: Principal(sub=sub, email=None, roles=frozenset())


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_exceeding_a_user_bucket_returns_429_problem_with_retry_after(real_valkey):
    app = _build_app(real_valkey, rate_limit_checkout_capacity=2, rate_limit_checkout_refill=2)
    app.dependency_overrides[get_current_user] = _as_principal("consumer-1")
    async with _client(app) as client:
        assert (await client.post("/limited")).status_code == 200
        assert (await client.post("/limited")).status_code == 200
        over = await client.post("/limited")
    assert over.status_code == 429
    assert over.headers["content-type"].startswith("application/problem+json")
    assert int(over.headers["Retry-After"]) >= 1
    body = over.json()
    assert body["status"] == 429
    assert body["title"] == "Too Many Requests"
    assert body["trace_id"]  # rides the same RFC 9457 shape as every other error


async def test_buckets_are_keyed_by_authenticated_subject(real_valkey):
    app = _build_app(real_valkey, rate_limit_checkout_capacity=1, rate_limit_checkout_refill=1)
    async with _client(app) as client:
        app.dependency_overrides[get_current_user] = _as_principal("consumer-1")
        assert (await client.post("/limited")).status_code == 200
        assert (await client.post("/limited")).status_code == 429  # consumer-1 spent its one token
        # a different caller has their own untouched budget — one caller can't lock out another
        app.dependency_overrides[get_current_user] = _as_principal("consumer-2")
        assert (await client.post("/limited")).status_code == 200


async def test_unauthenticated_fallback_keys_by_client_ip(real_valkey):
    app = _build_app(real_valkey, with_user_route=False, rate_limit_upload_capacity=1, rate_limit_upload_refill=1)
    async with _client(app) as client:
        assert (await client.post("/public")).status_code == 200  # no auth dependency on this route
        over = await client.post("/public")  # same client IP exhausts the shared bucket
    assert over.status_code == 429


async def test_disabled_flag_short_circuits_without_consuming_tokens(real_valkey):
    app = _build_app(
        real_valkey,
        rate_limit_enabled=False,
        rate_limit_checkout_capacity=1,
        rate_limit_checkout_refill=1,
    )
    app.dependency_overrides[get_current_user] = _as_principal("consumer-1")
    async with _client(app) as client:
        statuses = [(await client.post("/limited")).status_code for _ in range(5)]
    assert statuses == [200] * 5


async def test_no_valkey_client_fails_open(real_valkey):
    app = _build_app(real_valkey, rate_limit_checkout_capacity=1)
    app.state.valkey = None  # bare app with Valkey unwired
    app.dependency_overrides[get_current_user] = _as_principal("consumer-1")
    async with _client(app) as client:
        statuses = [(await client.post("/limited")).status_code for _ in range(3)]
    assert statuses == [200] * 3


async def test_valkey_fault_fails_open_not_500(real_valkey):
    class _BoomValkey:
        async def eval(self, *_args, **_kwargs):
            raise OSError("valkey is down")

    app = _build_app(_BoomValkey(), rate_limit_checkout_capacity=1)  # type: ignore[arg-type]
    app.dependency_overrides[get_current_user] = _as_principal("consumer-1")
    async with _client(app) as client:
        statuses = [(await client.post("/limited")).status_code for _ in range(3)]
    assert statuses == [200] * 3  # abuse control never becomes the reason a valid write fails


# --- the real routers carry the deps (wiring regression guard) -------------------


def _route_deps(router) -> dict[str, tuple]:
    """Map ``METHOD path`` → the route-level ``dependencies=[...]`` tuple for a real router.

    Only actual APIRoute entries are mapped: a router's ``routes`` list also holds
    the mounted sub-router objects themselves, which don't carry per-route deps.
    """
    return {
        f"{sorted(route.methods)[0]} {route.path}": tuple(route.dependencies)
        for route in router.routes
        if getattr(route, "methods", None) and getattr(route, "dependencies", None) is not None
    }


def _rate_limit_deps(deps) -> list:
    """The rate-limit Depends among a route's dependencies (the factories' ``_guard`` closures).

    ``require_role`` also closes over a ``_guard``, so match on the factory's
    qualname (``rate_limited.<locals>._guard`` / ``ip_rate_limited.<locals>._guard``),
    not the bare function name. FastAPI exposes the wrapped callable as
    ``dependency`` (``call`` in some versions) — read both.
    """
    found = []
    for d in deps:
        call = getattr(d, "dependency", None) or getattr(d, "call", None)
        qualname = getattr(call, "__qualname__", "")
        if qualname.startswith(("rate_limited.", "ip_rate_limited.")) and qualname.endswith("._guard"):
            found.append(d)
    return found


def test_real_routers_carry_their_rate_limit_dependencies():
    """The gated production routes must actually list the rate-limit dependency.

    The HTTP tests above run a synthetic app, so deleting ``dependencies=[...]``
    from a route decorator would still pass them — this introspects the real
    catalog/orders routers so the wiring itself is pinned (path strings match the
    OpenAPI contract's five 429-documented endpoints).
    """
    from src.catalog.api import routes as catalog_routes
    from src.orders.api import routes as orders_routes

    catalog = _route_deps(catalog_routes.router)
    checkout = _route_deps(orders_routes.checkout_router)

    gated = (
        ("POST /products", catalog),
        ("PATCH /products/{product_id}", catalog),
        ("DELETE /products/{product_id}", catalog),
        ("POST /products/{product_id}/image:presign", catalog),
        ("POST /checkout", checkout),
    )
    for key, deps in gated:
        assert key in deps, f"route {key} not found on the real router"
        found = _rate_limit_deps(deps[key])
        assert len(found) == 1, f"{key} must carry exactly one rate-limit dependency (has {len(found)})"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
