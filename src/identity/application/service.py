"""Identity use-cases.

``IdentityService`` covers reads + JIT provisioning over the local user mirror
(maps repo rows through the domain to the ``UserResponse`` schema so the ORM row
never crosses the service boundary). ``IdentityAdminService`` wraps the Keycloak
Admin API (role grants + enable/disable), also mirroring ``is_active`` locally on
disable. No soft-delete filter: disabled users still resolve so FKs anchor and
JIT lookups find an existing row.
"""

from __future__ import annotations

import uuid

from src.identity.api.schemas import UserResponse
from src.identity.application.mappers import to_domain
from src.identity.ports.admin import IdentityAdminPort
from src.identity.ports.repository import IdentityRepositoryPort


class IdentityService:
    """Read-side use-cases + JIT provisioning over the local user mirror."""

    def __init__(self, repo: IdentityRepositoryPort) -> None:
        self._repo = repo

    async def get_by_oidc_sub(self, oidc_sub: str) -> UserResponse | None:
        """Resolve a user by their OIDC ``sub``, or ``None`` if absent."""
        row = await self._repo.get_by_oidc_sub(oidc_sub)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))

    async def get_by_id(self, user_id: uuid.UUID) -> UserResponse | None:
        """Resolve a user by primary key, or ``None`` if absent."""
        row = await self._repo.get_by_id(user_id)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))

    async def get_or_create_by_sub(self, oidc_sub: str, email: str) -> UserResponse:
        """JIT-provision (or fetch) the local mirror for a verified caller."""
        row = await self._repo.get_or_create(oidc_sub, email)
        return UserResponse.model_validate(to_domain(row))


class IdentityAdminService:
    """Admin use-cases: manage Keycloak roles/enablement (admin-gated at the route)."""

    def __init__(self, repo: IdentityRepositoryPort, admin: IdentityAdminPort) -> None:
        self._repo = repo
        self._admin = admin

    async def grant_merchant(self, oidc_sub: str) -> None:
        """Grant the ``merchant`` realm role in Keycloak (the role authority)."""
        await self._admin.grant_realm_role(oidc_sub, "merchant")

    async def revoke_merchant(self, oidc_sub: str) -> None:
        """Revoke the ``merchant`` realm role in Keycloak."""
        await self._admin.revoke_realm_role(oidc_sub, "merchant")

    async def disable_user(self, oidc_sub: str) -> None:
        """Disable in Keycloak and mirror ``is_active=false`` locally.

        If the caller was never JIT-provisioned (no local row yet), a plain
        ``UPDATE`` matches nothing and the disable silently fails to stick —
        a subsequent authenticated request would JIT-provision a fresh, active
        row. Look the account up in Keycloak and provision it (disabled) so
        the mirror can't drift from the Keycloak-side disable.
        """
        await self._admin.set_enabled(oidc_sub, False)
        row = await self._repo.set_active(oidc_sub, False)
        if row is None:
            email = await self._admin.get_user_email(oidc_sub)
            if email is not None:
                await self._repo.get_or_create(oidc_sub, email)
                await self._repo.set_active(oidc_sub, False)

    async def create_user(self, email: str, temporary_password: str) -> str:
        """Create a new Keycloak account and return its ``sub`` (admin only)."""
        return await self._admin.create_user(email, temporary_password)
