"""Unit tests for the Keycloak Admin API adapter's error taxonomy.

A caller-fixable Keycloak outcome (unknown sub/role → 409/404 family) must reach
the admin as 404/409 Problem Details instead of the 500 boundary; genuine faults
(401 token, 5xx outage) stay exactly as raised.
"""

from __future__ import annotations

import pytest
from keycloak.exceptions import KeycloakError, KeycloakGetError, KeycloakPostError, KeycloakPutError

from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import KeycloakConflictError, KeycloakEntityNotFoundError

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


async def test_genuine_faults_are_reraised_untranslated(monkeypatch) -> None:
    outage = KeycloakGetError(error_message="boom", response_code=500)
    admin = _admin_with(monkeypatch, outage)
    with pytest.raises(KeycloakGetError) as caught:
        await admin.grant_realm_role("sub-1", "merchant")
    assert caught.value is outage
