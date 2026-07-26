"""Port (Protocol) for Keycloak identity administration.

Keycloak is the identity/role authority; the app manages users/roles through its
Admin API. Implemented by ``adapters/keycloak/admin_client.KeycloakIdentityAdmin``.
Keyed by the OIDC ``sub`` — in Keycloak the token ``sub`` *is* the internal user id.
``create_user`` provisions a brand-new Keycloak account (returns its ``sub``);
``get_user_email`` backs disabling a not-yet-JIT-provisioned user (the local mirror
row must exist before ``is_active`` can be flipped).
"""

from __future__ import annotations

from typing import Protocol


class IdentityAdminPort(Protocol):
    async def grant_realm_role(self, user_sub: str, role: str) -> None: ...

    async def revoke_realm_role(self, user_sub: str, role: str) -> None: ...

    async def set_enabled(self, user_sub: str, enabled: bool) -> None: ...

    async def get_user_email(self, user_sub: str) -> str | None: ...

    async def create_user(self, email: str, temporary_password: str) -> str: ...
