"""JWKS resolution guards, worker retry loop, and the resilience primitives.

Covers: the unknown-``kid`` refresh-amplification guard, the bounded-retry +
circuit-breaker shell on the payment gateway and Keycloak admin (transient-only,
fail-fast while open, observable via ``circuit_state``), and the shared
``poll_forever`` loop's survival of transient errors.
"""

from __future__ import annotations

import asyncio
import json
import logging

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.exceptions import PyJWKClientError
from keycloak.exceptions import KeycloakConnectionError, KeycloakGetError, KeycloakPostError

from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin
from src.payments.adapters.resilient_gateway import ResilientPaymentGateway
from src.payments.adapters.stub_gateway import StubPaymentGateway
from src.payments.ports.gateway import GatewayOutcome
from src.shared.auth.jwks import resolve_signing_key
from src.shared.bus.polling import poll_forever
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import DependencyUnavailableError, KeycloakConflictError, KeycloakEntityNotFoundError
from src.shared.resilience import CircuitBreaker, CircuitOpenError, retry_transient

log = logging.getLogger(__name__)

_SETTINGS = dict(
    environment="local",
    keycloak_issuer="https://keycloak.test/realms/ecommerce",
    keycloak_admin_client_id="ecommerce-admin",
    keycloak_admin_client_secret="secret",
    resilience_max_attempts=2,
    resilience_retry_base_delay_seconds=0.01,
    resilience_retry_max_delay_seconds=0.02,
    resilience_breaker_failure_threshold=2,
    resilience_breaker_reset_seconds=0.05,
)


# --- JWKS unknown-kid guards (unchanged contracts) -----------------------------


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
    """Only 404 means "gone" — any other fault must surface (now as the 503 type,
    after bounded retries and breaker accounting), never as a silent skip."""

    class _FailingKc:
        def __init__(self, code: int | None) -> None:
            self._code = code

        async def a_get_user(self, _sub):
            if self._code == 404:
                raise KeycloakGetError(error_message="not found", response_code=404)
            if self._code is None:
                raise KeycloakConnectionError("connection refused")
            raise KeycloakGetError(error_message="boom", response_code=self._code)

    admin = KeycloakIdentityAdmin(AppSettings(**_SETTINGS))

    async def _client_for(code):
        kc = _FailingKc(code)

        async def _wrap():
            return kc

        admin._client = _wrap  # type: ignore[method-assign]
        return kc

    await _client_for(404)
    assert await admin.get_user_email("sub") is None  # genuinely gone

    for code in (401, 403):  # definitive-but-failing answers: still raw, still not "missing"
        await _client_for(code)
        with pytest.raises(KeycloakGetError):
            await admin.get_user_email("sub")

    for code in (None, 500, 503):  # connection faults and 5xx: bounded retries → 503 type
        await _client_for(code)
        with pytest.raises(DependencyUnavailableError):
            await admin.get_user_email("sub")


# --- poll_forever (worker loop) -------------------------------------------------


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


# --- CircuitBreaker -----------------------------------------------------------


def test_breaker_opens_after_threshold_and_fails_fast():
    breaker = CircuitBreaker("test", failure_threshold=2, reset_timeout_seconds=60)
    assert breaker.allow()
    breaker.record_failure()
    assert breaker.allow()  # still closed
    breaker.record_failure()
    assert breaker.state == "open"
    assert not breaker.allow()  # fail fast — no probe during the window


async def test_breaker_half_opens_after_reset_and_closes_on_success():
    breaker = CircuitBreaker("test", failure_threshold=1, reset_timeout_seconds=0.01)
    breaker.record_failure()
    assert breaker.state == "open"
    await asyncio.sleep(0.02)
    assert breaker.allow()  # window elapsed → half-open admits one probe
    assert breaker.state == "half_open"
    assert not breaker.allow()  # a second concurrent caller fails fast
    breaker.record_success()
    assert breaker.state == "closed"
    assert breaker.allow()


async def test_breaker_reopens_when_probe_fails():
    breaker = CircuitBreaker("test", failure_threshold=1, reset_timeout_seconds=0.01)
    breaker.record_failure()
    await asyncio.sleep(0.02)
    assert breaker.allow()
    breaker.record_failure()  # the probe failed
    assert breaker.state == "open"
    assert not breaker.allow()  # fresh window, fail fast again


def test_breaker_ignores_straggler_success_while_open():
    breaker = CircuitBreaker("test", failure_threshold=1, reset_timeout_seconds=60)
    breaker.record_failure()
    assert breaker.state == "open"
    breaker.record_success()  # a late probe result after re-open must not un-arm
    assert breaker.state == "open"
    assert not breaker.allow()


async def test_breaker_cancelled_probe_does_not_wedge_half_open():
    """A cancelled call (saga step timeout, force-exit) records no outcome — it must
    release the half-open probe slot, or the breaker would 503 forever until restart."""
    breaker = CircuitBreaker("test", failure_threshold=1, reset_timeout_seconds=0.01)
    breaker.record_failure()
    await asyncio.sleep(0.02)
    assert breaker.allow()  # probe admitted
    breaker.record_abandoned()  # ...then the call was cancelled mid-flight
    assert breaker.allow()  # slot released — a fresh probe can be admitted

    # And the full wrapper path: a cancelled gateway call must not wedge either.
    from src.payments.adapters.resilient_gateway import ResilientPaymentGateway

    class _HangingGateway:
        async def charge(self, **kwargs):  # noqa: ANN002, ANN202
            await asyncio.sleep(30)

        async def lookup(self, idempotency_key: str):  # noqa: ANN202
            await asyncio.sleep(30)

    gateway = ResilientPaymentGateway(
        _HangingGateway(), max_attempts=1, failure_threshold=1, reset_timeout_seconds=0.01
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(gateway.charge(amount=1, idempotency_key="k", payment_method_token="t"), timeout=0.05)
    await asyncio.sleep(0.02)  # reset window elapses while the probe was abandoned
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(gateway.charge(amount=1, idempotency_key="k2", payment_method_token="t"), timeout=0.05)


def test_breaker_success_resets_the_failure_streak():
    breaker = CircuitBreaker("test", failure_threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()  # proves the dependency recovered
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == "closed"  # streak was reset — 2 < 3


def test_circuit_open_error_is_a_dependency_unavailable_error():
    # The fail-fast error must ride the existing 503 Problem handler.
    assert issubclass(CircuitOpenError, DependencyUnavailableError)


# --- retry_transient ----------------------------------------------------------


async def test_retry_transient_retries_then_succeeds():
    calls = 0

    async def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("transient")
        return 42

    result = await retry_transient(flaky, attempts=3, base_delay_seconds=0.01, max_delay_seconds=0.02)
    assert result == 42 and calls == 3


async def test_retry_transient_gives_up_after_bounded_attempts():
    calls = 0

    async def always_down() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        await retry_transient(always_down, attempts=3, base_delay_seconds=0.01)
    assert calls == 3  # bounded — documented max, not infinite


async def test_retry_transient_never_retries_definitive_answers():
    calls = 0

    async def not_found() -> None:
        nonlocal calls
        calls += 1
        raise KeycloakGetError(error_message="nope", response_code=404)

    with pytest.raises(KeycloakGetError):
        await retry_transient(not_found, attempts=3, base_delay_seconds=0.01)
    assert calls == 1  # 4xx is an answer, not an outage


async def test_retry_transient_never_retries_circuit_open():
    """The fail-fast error must not burn retry attempts against an open breaker."""
    calls = 0

    async def gated() -> None:
        nonlocal calls
        calls += 1
        raise CircuitOpenError("open")

    with pytest.raises(CircuitOpenError):
        await retry_transient(gated, attempts=3, base_delay_seconds=0.01)
    assert calls == 1


async def test_retry_transient_treats_5xx_as_transient():
    attempts: list[int] = []

    async def flaky_5xx() -> int:
        attempts.append(1)
        if len(attempts) == 1:
            raise KeycloakGetError(error_message="boom", response_code=503)
        return 7

    assert await retry_transient(flaky_5xx, attempts=2, base_delay_seconds=0.01) == 7


# --- ResilientPaymentGateway --------------------------------------------------


class _DownGateway:
    """Gateway that always raises a transient fault (and counts calls)."""

    def __init__(self) -> None:
        self.calls = 0

    async def charge(self, **kwargs):  # noqa: ANN002, ANN202
        self.calls += 1
        raise ConnectionError("gateway down")

    async def lookup(self, idempotency_key: str):  # noqa: ANN202
        self.calls += 1
        raise ConnectionError("gateway down")


async def test_gateway_transient_fault_opens_breaker_and_fails_fast():
    down = _DownGateway()
    gateway = ResilientPaymentGateway(down, max_attempts=2, base_delay_seconds=0.01, failure_threshold=2)
    with pytest.raises(DependencyUnavailableError):
        await gateway.charge(amount=10, idempotency_key="k1", payment_method_token="tok")
    assert down.calls == 2  # one logical call = bounded tries
    with pytest.raises(DependencyUnavailableError):
        await gateway.charge(amount=10, idempotency_key="k2", payment_method_token="tok")
    assert down.calls == 4  # second logical call still tried (streak 2 → open)
    # Third logical call: breaker open → fails fast without touching the gateway.
    with pytest.raises(DependencyUnavailableError):
        await gateway.charge(amount=10, idempotency_key="k3", payment_method_token="tok")
    assert down.calls == 4


async def test_gateway_open_breaker_does_not_call_the_inner_gateway():
    down = _DownGateway()
    gateway = ResilientPaymentGateway(down, max_attempts=1, failure_threshold=1)
    with pytest.raises(DependencyUnavailableError):
        await gateway.lookup("k1")
    assert down.calls == 1
    with pytest.raises(DependencyUnavailableError):
        await gateway.lookup("k2")
    assert down.calls == 1  # breaker open — the gateway was not contacted again


async def test_gateway_decline_is_not_a_fault():
    """A business decline is a definitive answer: breaker stays closed."""
    stub = StubPaymentGateway(fail_token_substring="decline")
    gateway = ResilientPaymentGateway(stub, max_attempts=1, failure_threshold=1)
    for i in range(5):  # far past the threshold
        result = await gateway.charge(amount=10, idempotency_key=f"k{i}", payment_method_token="decline-me")
        assert result.outcome == GatewayOutcome.FAILED
    assert gateway._breaker.state == "closed"


async def test_gateway_retry_exhaustion_raises_unavailable():
    down = _DownGateway()
    gateway = ResilientPaymentGateway(down, max_attempts=3, base_delay_seconds=0.01)
    with pytest.raises(DependencyUnavailableError):
        await gateway.lookup("k1")
    assert down.calls == 3


# --- Keycloak admin resilience ------------------------------------------------


class _FlakyKeycloak:
    """Stands in for ``KeycloakAdmin``: raises the injected error until ``up``."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.up = False
        self.calls = 0

    async def a_get_realm_role(self, role_name: str):  # noqa: ANN202
        self.calls += 1
        if not self.up:
            raise self._error
        return {"name": role_name}

    async def a_assign_realm_roles(self, user_id: str, roles: list):  # noqa: ANN002, ANN202
        self.calls += 1

    async def a_create_user(self, payload: dict):  # noqa: ANN002, ANN202
        self.calls += 1
        raise self._error


def _admin_with(monkeypatch, error: Exception) -> tuple[KeycloakIdentityAdmin, _FlakyKeycloak]:
    admin = KeycloakIdentityAdmin(AppSettings(**_SETTINGS))
    kc = _FlakyKeycloak(error)

    async def fake_client():
        return kc

    monkeypatch.setattr(admin, "_client", fake_client)
    return admin, kc


async def test_keycloak_transient_retries_then_succeeds(monkeypatch):
    admin, kc = _admin_with(monkeypatch, KeycloakConnectionError("connection refused"))
    kc.up = True  # the injected error would fire; the flaky wrapper below replaces it
    original = kc.a_get_realm_role
    state = {"n": 0}

    async def flaky(role_name):
        state["n"] += 1
        if state["n"] == 1:
            raise KeycloakConnectionError("refused")  # first attempt transient-fails
        return await original(role_name)

    kc.a_get_realm_role = flaky  # type: ignore[method-assign]
    await admin.grant_realm_role("sub", "merchant")  # retried once, then fine
    assert state["n"] == 2


async def test_keycloak_sustained_outage_maps_to_dependency_unavailable(monkeypatch):
    admin, kc = _admin_with(monkeypatch, KeycloakConnectionError("refused"))
    with pytest.raises(DependencyUnavailableError):
        await admin.grant_realm_role("sub", "merchant")
    assert kc.calls == _SETTINGS["resilience_max_attempts"]  # bounded, then 503


async def test_keycloak_5xx_is_transient_404_is_not(monkeypatch):
    admin, kc = _admin_with(monkeypatch, KeycloakGetError(error_message="boom", response_code=500))
    with pytest.raises(DependencyUnavailableError):
        await admin.grant_realm_role("sub", "merchant")
    assert kc.calls == _SETTINGS["resilience_max_attempts"]

    admin2, kc2 = _admin_with(monkeypatch, KeycloakGetError(error_message="gone", response_code=404))
    with pytest.raises(KeycloakEntityNotFoundError):  # untranslated — a definitive answer
        await admin2.grant_realm_role("sub", "merchant")
    assert kc2.calls == 1  # not retried


async def test_keycloak_breaker_opens_and_fails_fast(monkeypatch):
    admin, kc = _admin_with(monkeypatch, KeycloakConnectionError("refused"))
    for _ in range(_SETTINGS["resilience_breaker_failure_threshold"]):
        with pytest.raises(DependencyUnavailableError):
            await admin.grant_realm_role("sub", "merchant")
    expected = _SETTINGS["resilience_max_attempts"] * _SETTINGS["resilience_breaker_failure_threshold"]
    assert kc.calls == expected
    with pytest.raises(DependencyUnavailableError):  # breaker open → no network call
        await admin.grant_realm_role("sub", "merchant")
    assert kc.calls == expected


async def test_keycloak_409_conflict_not_retried_not_breaker_counted(monkeypatch):
    admin, kc = _admin_with(monkeypatch, KeycloakPostError(error_message="exists", response_code=409))
    with pytest.raises(KeycloakConflictError):
        await admin.create_user("taken@example.com")
    assert kc.calls == 1  # a definitive answer: one call, no retry
    assert admin._breaker.state == "closed"


async def test_keycloak_breaker_metrics_exposed(monkeypatch):
    """The breaker gauge must be present with the dependency label after use."""
    from prometheus_client import generate_latest

    admin, _kc = _admin_with(monkeypatch, KeycloakConnectionError("refused"))
    with pytest.raises(DependencyUnavailableError):
        await admin.grant_realm_role("sub", "merchant")
    body = generate_latest().decode()
    assert "circuit_state" in body
    assert 'dependency="keycloak_admin"' in body
