"""FastAPI dependencies that enforce a :class:`~src.shared.ratelimit.token_bucket.ValkeyTokenBucket`.

Two keying strategies, chosen by whether the route is authenticated:

- :func:`rate_limited` — for authenticated routes (item create/update/delete,
  checkout, image presign). It depends on the *already-verified* principal, so the
  bucket key is the caller's OIDC ``sub``. FastAPI caches ``get_current_user`` per
  request, so a route that also injects the principal validates the token once, not
  twice — the rate check rides the existing auth, it never re-decodes the JWT.

- :func:`ip_rate_limited` — the documented fallback for **unauthenticated** routes,
  where there is no ``sub`` to key on. The key is the client IP as rewritten by
  ``ProxyHeadersMiddleware`` from ``X-Forwarded-For`` **only when the immediate peer
  is in ``trusted_proxies``** (see the setting's comment): past the ALB a client cannot
  forge it. The trade-off is inherent to IP keying — callers behind one NAT/corporate
  egress share a bucket — which is why authenticated endpoints key on ``sub`` instead.

Both **fail open**: no Valkey client wired, the flag off, or a Valkey fault means the
request proceeds. A rate limiter is best-effort abuse control; a cache outage must
never take checkout or every merchant write down with it. The prod backstop is the ALB
AWS WAF rate rules (Terraform tickets), which sit outside the app entirely.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from fastapi import Request

from src.shared.auth.dependencies import PrincipalDep
from src.shared.errors.exceptions import RateLimitExceededError
from src.shared.ratelimit.token_bucket import RateLimitConfig, ValkeyTokenBucket, config_for

log = logging.getLogger(__name__)

#: A dependency callable (FastAPI injects the request/params) that enforces a bucket
#: by raising, or returns normally to let the request through.
RateLimitDep = Callable[..., Awaitable[None]]


def _client_ip(request: Request) -> str | None:
    """The client IP after proxy-header rewriting (unforgeable past a trusted peer).

    ``ProxyHeadersMiddleware`` replaces ``request.client.host`` with the leftmost
    ``X-Forwarded-For`` value only when the socket peer is in ``trusted_proxies``;
    otherwise the real peer address stands. ``None`` when the ASGI server supplies
    no client address (exotic setups) — callers fail open rather than group every
    such request into one shared bucket a single caller could drain for all.
    """
    return request.client.host if request.client is not None else None


async def _enforce(request: Request, bucket: str, identity: str) -> None:
    """Spend one token from ``bucket:identity`` or raise :class:`RateLimitExceededError`.

    Every degrade path returns without raising — the limiter protects the service and
    must never become the reason a legitimate request fails. Note the deliberate
    ordering this preserves: as a route-level ``dependencies=[...]`` entry it runs
    *before* body validation, so an invalid-body retry loop still spends a token
    (spend per attempt) — do not "fix" it into the handler signature.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None or not settings.rate_limit_enabled:  # mis-wired app: fail open too
        return
    valkey = getattr(request.app.state, "valkey", None)
    if valkey is None:  # bare app / no Valkey wired: fail open, never block traffic
        return
    config: RateLimitConfig = config_for(settings, bucket)
    try:
        decision = await ValkeyTokenBucket(valkey).allow(key=f"ratelimit:{bucket}:{identity}", config=config)
    except Exception:  # boundary: a Valkey fault is not a reason to refuse a valid request
        log.warning("rate-limit check failed for bucket %r; failing open", bucket, exc_info=True)
        return
    if not decision.allowed:
        raise RateLimitExceededError(retry_after_seconds=decision.retry_after_seconds)


def rate_limited(bucket: str) -> RateLimitDep:
    """Per-authenticated-user token bucket. Use as a route ``dependencies=[...]`` entry.

    Call the factory once at module scope and reuse the resulting ``Depends`` object
    across routes (mirrors ``require_role``) so the identity is stable per bucket.
    Depends on the shared ``PrincipalDep`` so it runs after auth and reuses the
    request-cached token validation rather than decoding the JWT a second time.
    """

    async def _guard(request: Request, principal: PrincipalDep) -> None:
        await _enforce(request, bucket, identity=f"sub:{principal.sub}")

    return _guard


def ip_rate_limited(bucket: str) -> RateLimitDep:
    """Per-client-IP token bucket — the fallback key for unauthenticated routes."""

    async def _guard(request: Request) -> None:
        ip = _client_ip(request)
        if ip is None:  # no client address from the ASGI server: fail open
            return
        await _enforce(request, bucket, identity=f"ip:{ip}")

    return _guard
