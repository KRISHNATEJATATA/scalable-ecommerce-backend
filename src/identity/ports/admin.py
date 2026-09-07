"""Port (Protocol) for Keycloak identity administration.

Keycloak is the identity/role authority; the app manages users/roles through its
Admin API. Implemented by ``adapters/keycloak/admin_client.KeycloakIdentityAdmin``.
Keyed by the OIDC ``sub`` — in Keycloak the token ``sub`` *is* the internal user id.
``create_user`` provisions a brand-new Keycloak account (returns its ``sub``);
``get_user_email`` backs disabling a not-yet-JIT-provisioned user (the local mirror
row must exist before ``is_active`` can be flipped). ``list_users``/``has_realm_role``
serve the admin directory listing — Keycloak is the complete directory (the local
mirror is JIT-only and incomplete), and role flags are read live from realm mappings.
"""

from __future__ import annotations

from typing import Protocol

from src.identity.domain.user import DirectoryUser


class IdentityAdminPort(Protocol):
    async def grant_realm_role(self, user_sub: str, role: str) -> None: ...

    async def revoke_realm_role(self, user_sub: str, role: str) -> None: ...

    async def set_enabled(self, user_sub: str, enabled: bool) -> None: ...

    async def get_user_email(self, user_sub: str) -> str | None: ...

    async def create_user(self, email: str) -> str: ...

    async def list_users(self, search: str | None, first: int, max_results: int) -> list[DirectoryUser]: ...

    async def has_realm_role(self, user_sub: str, role: str) -> bool: ...
