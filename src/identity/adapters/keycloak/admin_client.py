"""Keycloak Admin API adapter (service-account client).

Implements :class:`src.identity.ports.admin.IdentityAdminPort` via
``python-keycloak``. The library exposes native async methods (``a_*``), so no
``run_in_threadpool`` wrapping is needed. The connection is built lazily on first
use and reused (it refreshes its own service-account token).

In Keycloak the OIDC ``sub`` claim *is* the internal user id, so ``user_sub`` is
passed straight through as the admin ``user_id``.
"""

from __future__ import annotations

from keycloak.exceptions import KeycloakGetError

from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from src.shared.config.setting import AppSettings


class KeycloakIdentityAdmin:
    """Grant/revoke realm roles and enable/disable users via Keycloak's Admin API."""

    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._admin: KeycloakAdmin | None = None

    async def _client(self) -> KeycloakAdmin:
        if self._admin is None:
            settings = self._settings
            if not (
                settings.keycloak_issuer and settings.keycloak_admin_client_id and settings.keycloak_admin_client_secret
            ):
                raise RuntimeError("Keycloak admin client is not configured")
            # issuer is ``<server_url>/realms/<realm>`` → strip back to the server root.
            server_url = settings.keycloak_issuer.rsplit("/realms/", 1)[0] + "/"
            connection = KeycloakOpenIDConnection(
                server_url=server_url,
                realm_name=settings.keycloak_realm,
                client_id=settings.keycloak_admin_client_id,
                client_secret_key=settings.keycloak_admin_client_secret,
            )
            self._admin = KeycloakAdmin(connection=connection)
        return self._admin

    async def grant_realm_role(self, user_sub: str, role: str) -> None:
        kc = await self._client()
        role_rep = await kc.a_get_realm_role(role)
        await kc.a_assign_realm_roles(user_sub, [role_rep])

    async def revoke_realm_role(self, user_sub: str, role: str) -> None:
        kc = await self._client()
        role_rep = await kc.a_get_realm_role(role)
        await kc.a_delete_realm_roles_of_user(user_sub, [role_rep])

    async def set_enabled(self, user_sub: str, enabled: bool) -> None:
        kc = await self._client()
        await kc.a_update_user(user_sub, {"enabled": enabled})

    async def get_user_email(self, user_sub: str) -> str | None:
        """Look up a Keycloak user's email by ``sub`` (``None`` if the user is gone)."""
        kc = await self._client()
        try:
            user = await kc.a_get_user(user_sub)
        except KeycloakGetError:
            return None
        return user.get("email")

    async def create_user(self, email: str, temporary_password: str) -> str:
        """Create a new Keycloak account (``consumer`` default role, per Keycloak realm
        config) and return its ``sub``. The caller sets a temporary password; Keycloak
        owns the credential/refresh story from here on.
        """
        kc = await self._client()
        return await kc.a_create_user(
            {
                "email": email,
                "username": email,
                "enabled": True,
                "emailVerified": False,
                "credentials": [{"type": "password", "value": temporary_password, "temporary": True}],
            }
        )
