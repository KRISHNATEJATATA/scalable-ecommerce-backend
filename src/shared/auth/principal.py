"""The authenticated caller — a claims-only view of a verified Keycloak token.

Built purely from validated JWT claims (no DB row): ``sub`` anchors the local
user mirror, ``roles`` are Keycloak realm roles (``realm_access.roles``) driving
RBAC. Frozen so a request handler can't mutate the caller's identity.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Principal:
    """The verified caller: OIDC ``sub``, email claim, and realm roles."""

    sub: str
    email: str | None
    roles: frozenset[str]

    def has_role(self, *roles: str) -> bool:
        """True if the caller holds *any* of ``roles`` (any-of gate)."""
        return any(role in self.roles for role in roles)

    @property
    def is_admin(self) -> bool:
        """Admin bypasses *ownership* checks (never a role-membership gate)."""
        return "admin" in self.roles
