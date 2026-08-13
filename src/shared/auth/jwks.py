"""JWKS signing-key resolution for Keycloak-issued RS256 tokens.

One process-wide :class:`~jwt.PyJWKClient` is built at startup (stored on
``app.state.jwks_client``) so PyJWT's built-in ``kid`` cache is reused across
requests. Signing-key lookup is blocking ``urllib`` I/O, so it is offloaded to a
thread. A network failure (our problem — the token may be valid) is surfaced as
:class:`DependencyUnavailableError` (503); an unknown ``kid`` is a client problem
(401), raised by the caller.

**Unknown-``kid`` amplification guard.** ``PyJWKClient.get_signing_key`` refetches
the JWKS whenever a ``kid`` misses the cache, and that ``kid`` comes from the
*unverified* token header — so unauthenticated requests carrying random ``kid``s
would each burn a threadpool thread on an outbound Keycloak call, exhausting the
pool and amplifying load onto Keycloak. Refreshes are therefore **coalesced** (one
in flight per client) and **rate-limited** to one per ``min_refresh_interval``;
inside that window an uncached ``kid`` is rejected without touching the network.
Legitimate key rotation still resolves, just up to one interval later.
"""

from __future__ import annotations

import asyncio
import time

from fastapi.concurrency import run_in_threadpool
from jwt import PyJWK, PyJWKClient, get_unverified_header
from jwt.exceptions import (
    InvalidKeyError,
    PyJWKClientConnectionError,
    PyJWKClientError,
    PyJWKError,
    PyJWKSetError,
)

from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import DependencyUnavailableError

_GUARD_ATTR = "_ecommerce_refresh_guard"


class _RefreshGuard:
    """Per-client coalescing lock + last refresh *attempt* and its outcome.

    Throttling keys off the attempt (a failed fetch must still not be retried per
    request — that is the amplification guard), but the outcome is kept so requests
    inside the window inherit the provider failure (503) instead of being told the
    key is unknown (401).
    """

    __slots__ = ("lock", "last_attempt", "last_error")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.last_attempt = 0.0
        self.last_error: DependencyUnavailableError | None = None


def build_jwks_client(settings: AppSettings) -> PyJWKClient:
    """Build the shared JWKS client (its own ``kid`` cache) from settings.

    ``timeout`` is short and explicit: PyJWT defaults to 30s, long enough that a
    blackholed Keycloak would pin a threadpool thread for half a minute per request.
    """
    if not settings.keycloak_jwks_url:
        raise RuntimeError("KEYCLOAK_JWKS_URL is not configured")
    return PyJWKClient(
        settings.keycloak_jwks_url,
        # No per-kid LRU: that cache never expires, so a key removed from the JWKS
        # (rotated out, or revoked after a compromise) would stay trusted for the
        # process' lifetime. The JWKS-set cache expires, so it alone bounds trust.
        cache_keys=False,
        timeout=settings.jwks_timeout_seconds,
    )


def _cached_kids(client: PyJWKClient) -> set[str | None] | None:
    """``kid``s in the client's cached JWK set, or ``None`` if unknowable.

    ``None`` (no JWK-set cache — a client built with ``cache_jwk_set=False``, or a
    test double) means "can't prove this lookup hits the network", so the caller
    skips the guard rather than rejecting valid tokens.
    """
    cache = getattr(client, "jwk_set_cache", None)
    if cache is None:
        return None
    data = cache.get()
    if not isinstance(data, dict):
        return set()  # nothing cached yet → any lookup will fetch
    keys = data.get("keys")
    if not isinstance(keys, list):
        return set()  # cached body is unusable (e.g. ``"keys": null``) → treat as uncached
    return {key.get("kid") for key in keys if isinstance(key, dict)}


def _would_fetch(client: PyJWKClient, kid: str | None) -> bool:
    """True only if we can prove resolving ``kid`` requires an outbound fetch."""
    cached = _cached_kids(client)
    return cached is not None and kid not in cached


async def _lookup(client: PyJWKClient, token: str) -> PyJWK:
    """Fetch/resolve the key, mapping *provider* failures to 503 and only a genuinely
    unknown ``kid`` to the caller's 401 path.

    PyJWT reports "the JWKS body wasn't a JSON object", "the key set has no usable
    keys" and "no key matches this kid" through the same ``PyJWKClientError``, and a
    garbage body escapes as a raw ``JSONDecodeError``. Only the last of those is the
    client's fault. They are told apart by what the client cached: a successful fetch
    populates the cache, so a **non-empty** cached key set means we got a usable JWKS
    that simply lacks this ``kid`` (401); anything else means the provider gave us
    nothing usable (503).
    """
    try:
        return await run_in_threadpool(client.get_signing_key_from_jwt, token)
    except PyJWKClientConnectionError as exc:
        raise DependencyUnavailableError("identity provider (JWKS) is unreachable") from exc
    except PyJWKClientError as exc:
        if _cached_kids(client):
            raise  # usable JWKS, unknown kid → caller maps to 401
        raise DependencyUnavailableError("identity provider (JWKS) returned an unusable key set") from exc
    except (PyJWKSetError, PyJWKError, InvalidKeyError, ValueError, AttributeError, TypeError) as exc:
        # Malformed key set / undecodable JWKS body — provider-side, never the token's.
        # ``AttributeError``/``TypeError`` cover entries PyJWT parses without guarding,
        # e.g. ``{"keys": [null]}`` or a non-object key.
        raise DependencyUnavailableError("identity provider (JWKS) returned a malformed key set") from exc


async def resolve_signing_key(client: PyJWKClient, token: str, *, min_refresh_interval: float = 10.0) -> PyJWK:
    """Resolve the signing key for ``token`` off the event loop.

    JWKS endpoint unreachable → 503 (our failure); an unknown ``kid`` / malformed
    token raises ``PyJWKClientError``/``InvalidTokenError`` for the caller to map to 401.
    """
    kid = get_unverified_header(token).get("kid")  # malformed → InvalidTokenError → 401
    if not _would_fetch(client, kid):
        return await _lookup(client, token)

    guard = getattr(client, _GUARD_ATTR, None)
    if guard is None:  # no await between the check and the set → no race
        guard = _RefreshGuard()
        setattr(client, _GUARD_ATTR, guard)

    async with guard.lock:  # coalesce: one refresh in flight, not one per request
        if not _would_fetch(client, kid):  # a concurrent refresh may have brought it in
            return await _lookup(client, token)
        now = time.monotonic()
        if now - guard.last_attempt < min_refresh_interval:
            if guard.last_error is not None:  # the throttled attempt failed on their side
                raise DependencyUnavailableError("identity provider (JWKS) is unavailable") from guard.last_error
            raise PyJWKClientError(f'Unable to find a signing key that matches: "{kid}"')
        guard.last_attempt = now
        guard.last_error = None  # this attempt supersedes the previous outcome
        try:
            return await _lookup(client, token)
        except DependencyUnavailableError as exc:
            # Only a provider failure is remembered. Any other error means Keycloak
            # answered — the kid is genuinely unknown (401), and a stale outage flag
            # would keep throttled requests on 503 long after recovery.
            guard.last_error = exc
            raise
