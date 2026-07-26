"""Auth dependencies: token validation and role gates.

``get_current_user`` validates the Bearer token and returns a claims-only
:class:`Principal` — **no DB hit**, so RBAC via ``require_role`` stays DB-free.
The verify algorithm is pinned to ``settings.jwt_algorithm`` (RS256), which
structurally rejects ``alg:none`` and HS/RS confusion — never trust the token header.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated

import jwt
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError, PyJWKClientError

from src.shared.auth.jwks import resolve_signing_key
from src.shared.auth.principal import Principal
from src.shared.errors.exceptions import AuthenticationError, AuthorizationError

# auto_error=False → we own the 401 Problem Detail (HTTPBearer defaults to 403).
_bearer = HTTPBearer(auto_error=False, description="Keycloak-issued RS256 access token")


async def get_current_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> Principal:
    """Validate the Bearer token and return the claims-only caller (no DB hit)."""
    if credentials is None:
        raise AuthenticationError("missing bearer token")

    token = credentials.credentials
    settings = request.app.state.settings
    jwks_client = request.app.state.jwks_client

    # An unconfigured issuer would make PyJWT skip issuer validation entirely
    # (it only checks the claim's *presence*, not its value) — fail closed.
    if not settings.keycloak_issuer:
        raise RuntimeError("KEYCLOAK_ISSUER is not configured")

    # JWKS unreachable → DependencyUnavailableError (503) propagates; an unknown
    # kid / malformed token is a client error → 401.
    try:
        signing_key = await resolve_signing_key(jwks_client, token)
    except (PyJWKClientError, InvalidTokenError) as exc:
        raise AuthenticationError("could not resolve token signing key") from exc

    try:
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=[settings.jwt_algorithm],
            audience=settings.keycloak_audience,
            issuer=settings.keycloak_issuer,
            options={"require": ["exp", "iss", "aud", "sub"]},
        )
    except InvalidTokenError as exc:
        raise AuthenticationError("invalid or expired token") from exc

    # A structurally-valid-but-malformed claim set (e.g. non-string ``sub``, or
    # ``realm_access`` not a mapping) is still a client problem → 401, not 500.
    try:
        sub = claims["sub"]
        if not isinstance(sub, str) or not sub:
            raise AuthenticationError("token 'sub' claim is missing or invalid")
        realm_access = claims.get("realm_access") or {}
        roles = frozenset(realm_access.get("roles") or [])
    except (AttributeError, TypeError, KeyError) as exc:
        raise AuthenticationError("token has malformed claims") from exc

    return Principal(sub=sub, email=claims.get("email"), roles=roles)


PrincipalDep = Annotated[Principal, Depends(get_current_user)]


def require_role(*roles: str) -> Callable[[Principal], Awaitable[Principal]]:
    """Dependency factory gating on realm roles (any-of); missing → 403.

    Role gates are explicit — ``admin`` does not auto-satisfy them (admin bypass
    applies to *ownership*, not role membership).
    """

    async def _guard(principal: PrincipalDep) -> Principal:
        if not principal.has_role(*roles):
            raise AuthorizationError(f"requires one of roles: {', '.join(roles)}")
        return principal

    return _guard
