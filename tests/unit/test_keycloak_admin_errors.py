"""Unit tests for the Keycloak Admin API adapter's error taxonomy.

A caller-fixable Keycloak outcome (unknown sub/role → 409/404 family) must reach
the admin as 404/409 Problem Details instead of the 500 boundary; genuine faults
(401 token, 5xx outage) stay exactly as raised.
"""

from __future__ import annotations

import pytest
from keycloak.exceptions import KeycloakError, KeycloakGetError, KeycloakPostError, KeycloakPutError

from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin
from src.identity.domain.user import DirectoryUser
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import (
    DependencyUnavailableError,
    KeycloakConflictError,
    KeycloakEntityNotFoundError,
    KeycloakInvalidRequestError,
)

_SETTINGS = dict(
    environment="local",
    keycloak_issuer="https://keycloak.test/realms/ecommerce",
    keycloak_admin_client_id="ecommerce-admin",
    keycloak_admin_client_secret="secret",
)


class _FakeKeycloak:
    """Stands in for ``KeycloakAdmin``; every call raises the injected error."""

    def __init__(self, error: KeycloakError) -> None:
        self._error = error

    async def a_get_realm_role(self, role_name: str):  # noqa: ANN202
        raise self._error

    async def a_assign_realm_roles(self, user_id: str, roles: list):  # noqa: ANN202
        raise AssertionError("assign must be unreachable when the role lookup already failed")

    async def a_update_user(self, user_id: str, payload: dict):  # noqa: ANN202
        raise self._error

    async def a_create_user(self, payload: dict):  # noqa: ANN202
        raise self._error


def _admin_with(monkeypatch, error: KeycloakError) -> KeycloakIdentityAdmin:
    admin = KeycloakIdentityAdmin(AppSettings(**_SETTINGS))
    error_holder = error

    async def fake_client():  # noqa: ANN202
        return _FakeKeycloak(error_holder)

    monkeypatch.setattr(admin, "_client", fake_client)
    return admin


async def test_unknown_role_or_sub_maps_to_not_found(monkeypatch) -> None:
    admin = _admin_with(monkeypatch, KeycloakGetError(error_message="not found", response_code=404))
    with pytest.raises(KeycloakEntityNotFoundError):
        await admin.grant_realm_role("sub-1", "merchant")


async def test_unknown_user_on_disable_maps_to_not_found(monkeypatch) -> None:
    admin = _admin_with(monkeypatch, KeycloakPutError(error_message="not found", response_code=404))
    with pytest.raises(KeycloakEntityNotFoundError):
        await admin.set_enabled("sub-gone", enabled=False)


async def test_duplicate_email_on_create_maps_to_conflict(monkeypatch) -> None:
    admin = _admin_with(monkeypatch, KeycloakPostError(error_message="exists", response_code=409))
    with pytest.raises(KeycloakConflictError):
        await admin.create_user("taken@example.com")


async def test_keycloak_400_on_create_maps_to_invalid_request(monkeypatch) -> None:
    """A Keycloak 400 (e.g. error-invalid-email) is caller-fixable input, not a
    server fault: it must surface as the purpose-named 4xx, never the raw
    KeycloakPostError that would fall through to the 500 boundary"""
    admin = _admin_with(monkeypatch, KeycloakPostError(error_message="error-invalid-email", response_code=400))
    with pytest.raises(KeycloakInvalidRequestError) as caught:
        await admin.create_user("not-an-email")
    # Generic detail: _translate is shared by every Admin-API call, not only
    # account creation — an email-specific wording would misname other 400s.
    assert caught.value.detail == "Keycloak rejected this request: HTTP 400 — check the submitted fields"


async def test_outage_is_retried_then_maps_to_dependency_unavailable(monkeypatch) -> None:
    """A provider outage (5xx) is transient: retried, breaker-counted, and after the
    bounded attempts surfaces as 503 DependencyUnavailable — no raw 500 boundary."""
    outage = KeycloakGetError(error_message="boom", response_code=500)
    admin = _admin_with(monkeypatch, outage)
    with pytest.raises(DependencyUnavailableError) as caught:
        await admin.grant_realm_role("sub-1", "merchant")
    assert not isinstance(caught.value, KeycloakError)  # translated, not re-raised raw


async def test_auth_faults_are_reraised_untranslated(monkeypatch) -> None:
    """401/403 are definitive answers (Keycloak is up): no retry, raw re-raise."""
    auth = KeycloakGetError(error_message="expired token", response_code=401)
    admin = _admin_with(monkeypatch, auth)
    with pytest.raises(KeycloakGetError) as caught:
        await admin.grant_realm_role("sub-1", "merchant")
    assert caught.value is auth


class _RecordingKeycloak:
    """Stands in for ``KeycloakAdmin`` for the directory reads; returns canned reps, records calls."""

    def __init__(self, users: list, roles: list, error: KeycloakError | None = None) -> None:
        self.users = users
        self.roles = roles
        self.error = error
        self.get_users_query: dict | None = None
        self.roles_of_user_id: str | None = None

    async def a_get_users(self, query: dict | None = None):  # noqa: ANN202
        if self.error is not None:
            raise self.error
        self.get_users_query = query
        return self.users

    async def a_get_realm_roles_of_user(self, user_id: str):  # noqa: ANN202
        self.roles_of_user_id = user_id
        if self.error is not None:
            raise self.error
        return self.roles


def _admin_with_fake(monkeypatch, fake: _RecordingKeycloak) -> KeycloakIdentityAdmin:
    admin = KeycloakIdentityAdmin(AppSettings(**_SETTINGS))

    async def fake_client() -> _RecordingKeycloak:
        return fake

    monkeypatch.setattr(admin, "_client", fake_client)
    return admin


async def test_list_users_maps_reps_and_wraps_search_for_infix(monkeypatch) -> None:
    fake = _RecordingKeycloak(
        users=[
            {"id": "sub-1", "email": "a@example.com", "enabled": True},
            {"id": "sub-2", "enabled": False},
        ],
        roles=[],
    )
    admin = _admin_with_fake(monkeypatch, fake)
    users = await admin.list_users("alice", 20, 50)
    assert users == [
        DirectoryUser(sub="sub-1", email="a@example.com", enabled=True),
        DirectoryUser(sub="sub-2", email=None, enabled=False),
    ]
    # Keycloak's bare `search` is prefix-only; the wrap is what buys substring.
    assert fake.get_users_query == {"first": 20, "max": 50, "search": "*alice*"}


async def test_list_users_without_search_omits_search_key(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[])
    admin = _admin_with_fake(monkeypatch, fake)
    assert await admin.list_users(None, 0, 100) == []
    assert fake.get_users_query == {"first": 0, "max": 100}
    # An empty `search` must not be wrapped into Keycloak's match-everything "*".
    assert await admin.list_users("", 0, 100) == []
    assert fake.get_users_query == {"first": 0, "max": 100}


async def test_list_users_outage_maps_to_dependency_unavailable(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[], error=KeycloakGetError(error_message="boom", response_code=500))
    admin = _admin_with_fake(monkeypatch, fake)
    with pytest.raises(DependencyUnavailableError):
        await admin.list_users(None, 0, 100)


async def test_has_realm_role_true_and_false(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[{"name": "consumer"}, {"name": "merchant"}])
    admin = _admin_with_fake(monkeypatch, fake)
    assert await admin.has_realm_role("sub-1", "merchant") is True
    assert await admin.has_realm_role("sub-1", "admin") is False
    assert fake.roles_of_user_id == "sub-1"


async def test_has_realm_role_404_means_roleless(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[], error=KeycloakGetError(error_message="gone", response_code=404))
    admin = _admin_with_fake(monkeypatch, fake)
    assert await admin.has_realm_role("sub-gone", "merchant") is False


async def test_has_realm_role_outage_maps_to_dependency_unavailable(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[], error=KeycloakGetError(error_message="boom", response_code=500))
    admin = _admin_with_fake(monkeypatch, fake)
    with pytest.raises(DependencyUnavailableError):
        await admin.has_realm_role("sub-1", "merchant")


async def test_has_realm_role_non_get_error_maps_to_dependency_unavailable(monkeypatch) -> None:
    fake = _RecordingKeycloak(users=[], roles=[], error=KeycloakPostError(error_message="boom", response_code=500))
    admin = _admin_with_fake(monkeypatch, fake)
    with pytest.raises(DependencyUnavailableError):
        await admin.has_realm_role("sub-1", "merchant")
