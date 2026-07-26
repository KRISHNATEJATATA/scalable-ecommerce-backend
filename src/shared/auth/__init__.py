"""Shared OIDC resource-server auth: the single auth dependency across modules.

Validate-only against Keycloak (RS256, JWKS). ``get_current_user`` returns a
claims-only :class:`Principal` (no DB hit); ``require_role`` gates on realm roles.
The DB-backed ``get_current_db_user`` (JIT provisioning) lives in
``src/shared/container.py`` to keep this package free of a cross-module import.
"""

from src.shared.auth.dependencies import PrincipalDep, get_current_user, require_role
from src.shared.auth.principal import Principal

__all__ = ["Principal", "PrincipalDep", "get_current_user", "require_role"]
