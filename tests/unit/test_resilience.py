"""JWKS resolution guards: unknown-``kid`` refresh amplification + worker retry loop."""

from __future__ import annotations

import asyncio
import json
import logging

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.exceptions import PyJWKClientError

from src.shared.auth.jwks import resolve_signing_key
from src.shared.bus.polling import poll_forever

log = logging.getLogger(__name__)


class _CountingJWKClient:
    """PyJWKClient stand-in that reports its cached JWK set and counts fetches."""

    def __init__(self, cached_kids: list[str]) -> None:
        self.fetches = 0
        self._cached = {"keys": [{"kid": k} for k in cached_kids]}
        client = self

        class _Cache:
            def get(self):
                return client._cached

        self.jwk_set_cache = _Cache()

    def get_signing_key_from_jwt(self, token: str):
        self.fetches += 1
        return object()


def _token(kid: str) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return jwt.encode({"sub": "x"}, key, algorithm="RS256", headers={"kid": kid})


async def test_unknown_kid_flood_triggers_one_refresh():
    """Random unverified kids must not become one outbound Keycloak call each."""
    client = _CountingJWKClient(cached_kids=["known"])

    async def attempt(i: int) -> None:
        with pytest.raises(PyJWKClientError):
            await resolve_signing_key(client, _token(f"bogus-{i}"), min_refresh_interval=60)

    # The very first unknown kid is allowed to refresh (it may be a real rotation);
    # everything inside the interval is rejected without touching the network.
    await resolve_signing_key(client, _token("bogus-0"), min_refresh_interval=60)
    await asyncio.gather(*(attempt(i) for i in range(1, 20)))

    assert client.fetches == 1  # 20 attackers, one refresh — no thread/JWKS amplification


async def test_known_kid_is_never_rate_limited():
    """A valid, cached kid resolves from cache regardless of the refresh window."""
    client = _CountingJWKClient(cached_kids=["known"])
    await resolve_signing_key(client, _token("bogus"), min_refresh_interval=60)  # burns the window

    for _ in range(5):
        await resolve_signing_key(client, _token("known"), min_refresh_interval=60)

    assert client.fetches == 6  # all served; the cached-kid path is never blocked


async def test_broken_jwks_is_503_not_401():
    """Provider-side breakage must be 503; only an unknown kid against a usable set is 401."""
    from src.shared.errors.exceptions import DependencyUnavailableError

    class _BrokenClient(_CountingJWKClient):
        """Cache holds an unusable key set (empty JWKS / non-JSON-object body)."""

        def __init__(self, cached) -> None:
            super().__init__(cached_kids=[])
            self._cached = cached

        def get_signing_key_from_jwt(self, token: str):
            raise PyJWKClientError("The JWKS endpoint did not contain any signing keys")

    for cached in ({"keys": []}, ["not", "an", "object"], None):
        client = _BrokenClient(cached)
        with pytest.raises(DependencyUnavailableError):
            await resolve_signing_key(client, _token("anything"))

    class _GarbageBody(_CountingJWKClient):
        def get_signing_key_from_jwt(self, token: str):
            raise json.JSONDecodeError("Expecting value", "<html>502</html>", 0)

    with pytest.raises(DependencyUnavailableError):
        await resolve_signing_key(_GarbageBody(cached_kids=["known"]), _token("known"))


async def test_malformed_jwks_entry_is_503_not_500():
    """``{"keys": [null]}`` blows up inside PyJWT — provider's fault, so 503."""
    from src.shared.errors.exceptions import DependencyUnavailableError

    class _NullEntry(_CountingJWKClient):
        def get_signing_key_from_jwt(self, token: str):
            raise AttributeError("'NoneType' object has no attribute 'get'")

    with pytest.raises(DependencyUnavailableError):
        await resolve_signing_key(_NullEntry(cached_kids=["known"]), _token("bogus"))


async def test_throttled_requests_inherit_the_provider_failure():
    """After a failed refresh, throttled requests must stay 503 — never downgrade to 401."""
    from jwt.exceptions import PyJWKClientConnectionError

    from src.shared.errors.exceptions import DependencyUnavailableError

    class _Flaky(_CountingJWKClient):
        down = True

        def get_signing_key_from_jwt(self, token: str):
            self.fetches += 1
            if self.down:
                raise PyJWKClientConnectionError("connection refused")
            return object()

    client = _Flaky(cached_kids=["known"])
    for _ in range(3):
        with pytest.raises(DependencyUnavailableError):
            await resolve_signing_key(client, _token("rotated"), min_refresh_interval=60)
    assert client.fetches == 1  # throttled, but still 503 rather than a bogus 401

    client.down = False
    guard = client._ecommerce_refresh_guard
    guard.last_attempt = 0.0  # simulate the window elapsing
    await resolve_signing_key(client, _token("rotated"), min_refresh_interval=60)
    assert guard.last_error is None  # success clears the failure state


async def test_recovered_provider_with_unknown_kid_does_not_stay_503():
    """Once Keycloak answers again, an unknown kid is 401 — the outage flag must clear."""
    from jwt.exceptions import PyJWKClientConnectionError

    from src.shared.errors.exceptions import DependencyUnavailableError

    class _Flaky(_CountingJWKClient):
        down = True

        def get_signing_key_from_jwt(self, token: str):
            self.fetches += 1
            if self.down:
                raise PyJWKClientConnectionError("connection refused")
            raise PyJWKClientError('Unable to find a signing key that matches: "bogus"')

    client = _Flaky(cached_kids=["known"])
    with pytest.raises(DependencyUnavailableError):
        await resolve_signing_key(client, _token("bogus"), min_refresh_interval=60)

    client.down = False
    client._ecommerce_refresh_guard.last_attempt = 0.0  # window elapsed
    for _ in range(2):  # the refresh itself, then a throttled follow-up
        with pytest.raises(PyJWKClientError):  # → 401, not a resurrected 503
            await resolve_signing_key(client, _token("bogus"), min_refresh_interval=60)


async def test_unknown_kid_against_usable_jwks_stays_401():
    """A healthy JWKS that simply lacks the kid is the client's problem, not ours."""

    class _NoSuchKid(_CountingJWKClient):
        def get_signing_key_from_jwt(self, token: str):
            raise PyJWKClientError('Unable to find a signing key that matches: "bogus"')

    with pytest.raises(PyJWKClientError):  # → AuthenticationError (401) at the dependency
        await resolve_signing_key(_NoSuchKid(cached_kids=["known"]), _token("bogus"))


async def test_keycloak_lookup_failure_is_not_treated_as_missing_user():
    """Only 404 means "gone" — a 403/5xx must propagate, not silently skip the mirror."""
    from keycloak.exceptions import KeycloakGetError

    from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin

    class _FailingKc:
        def __init__(self, code: int) -> None:
            self._code = code

        async def a_get_user(self, _sub):
            if self._code == 404:
                raise KeycloakGetError(error_message="not found", response_code=404)
            raise KeycloakGetError(error_message="boom", response_code=self._code)

    admin = KeycloakIdentityAdmin.__new__(KeycloakIdentityAdmin)

    async def _client_for(code):
        kc = _FailingKc(code)
        admin._client = lambda: _wrap(kc)
        return kc

    async def _wrap(kc):
        return kc

    await _client_for(404)
    assert await admin.get_user_email("sub") is None  # genuinely gone

    for code in (401, 403, 500, 503):
        await _client_for(code)
        with pytest.raises(KeycloakGetError):
            await admin.get_user_email("sub")


async def test_poll_forever_survives_transient_errors():
    """A transient SQS error must back off and retry, not kill the worker."""
    stop = asyncio.Event()
    calls = 0

    async def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("SQS unavailable")
        stop.set()
        return 1

    await asyncio.wait_for(poll_forever(flaky, stop, log, initial_backoff=0.01), timeout=5)
    assert calls == 3  # recovered instead of propagating out of run()


async def test_poll_forever_stops_promptly_while_backing_off():
    """SIGTERM during a backoff sleep must not wait out the full delay."""
    stop = asyncio.Event()

    async def always_fails() -> int:
        raise ConnectionError("SQS unavailable")

    task = asyncio.create_task(poll_forever(always_fails, stop, log, initial_backoff=30))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
