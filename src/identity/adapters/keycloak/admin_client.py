"""Keycloak Admin API adapter (service-account client).

Implements :class:`src.identity.ports.admin.IdentityAdminPort` via
``python-keycloak``. The library exposes native async methods (``a_*``), so no
``run_in_threadpool`` wrapping is needed. The connection is built lazily on first
use and reused (it refreshes its own service-account token).

In Keycloak the OIDC ``sub`` claim *is* the internal user id, so ``user_sub`` is
passed straight through as the admin ``user_id``.
"""

from __future__ import annotations

import asyncio

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

    async def create_user(self, email: str) -> str:
        """Create a new Keycloak account (``consumer`` default role, per Keycloak realm
        config), email it a set-up link, and return its ``sub``.

        No credential is passed through this API — the spec forbids the app ever
        seeing passwords. The account is created with ``UPDATE_PASSWORD`` (and
        ``VERIFY_EMAIL``, since the address is unverified) required actions, and we
        immediately send Keycloak's **execute-actions email** so the user actually
        receives the link to set their password. Without that email a
        credentialless account can never authenticate. Keycloak (the credential
        authority) owns the password/refresh story from there on.

        Create + email are made effectively atomic: if the email send fails **or
        the call is cancelled**, the just-created (credentialless, unauthenticatable)
        account is deleted, so a retry starts clean instead of colliding with an
        orphaned half-provisioned user on the ``UNIQUE(email)`` constraint. The
        compensating delete is shielded so a cancellation can't abort the cleanup
        itself.
        """
        kc = await self._client()
        actions = ["UPDATE_PASSWORD", "VERIFY_EMAIL"]
        user_sub = await kc.a_create_user(
            {
                "email": email,
                "username": email,
                "enabled": True,
                "emailVerified": False,
                "requiredActions": actions,
            }
        )
        try:
            await kc.a_send_update_account(user_id=user_sub, payload=actions)
        except BaseException:
            # BaseException (not Exception) so a cancellation during the email send
            # also triggers compensation; shield the delete so the cancellation
            # can't abort the rollback and re-orphan the account.
            await asyncio.shield(kc.a_delete_user(user_sub))
            raise
        return user_sub
