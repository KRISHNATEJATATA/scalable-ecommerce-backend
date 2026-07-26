"""JWKS signing-key resolution for Keycloak-issued RS256 tokens.

One process-wide :class:`~jwt.PyJWKClient` is built at startup (stored on
``app.state.jwks_client``) so PyJWT's built-in ``kid`` cache is reused across
requests. Signing-key lookup is blocking ``urllib`` I/O, so it is offloaded to a
thread. A network failure (our problem — the token may be valid) is surfaced as
:class:`DependencyUnavailableError` (503); an unknown ``kid`` is a client problem
(401), raised by the caller.
"""

from __future__ import annotations

from fastapi.concurrency import run_in_threadpool
from jwt import PyJWK, PyJWKClient
from jwt.exceptions import PyJWKClientConnectionError

from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import DependencyUnavailableError


def build_jwks_client(settings: AppSettings) -> PyJWKClient:
    """Build the shared JWKS client (its own ``kid`` cache) from settings."""
    if not settings.keycloak_jwks_url:
        raise RuntimeError("KEYCLOAK_JWKS_URL is not configured")
    return PyJWKClient(settings.keycloak_jwks_url)


async def resolve_signing_key(client: PyJWKClient, token: str) -> PyJWK:
    """Resolve the signing key for ``token`` off the event loop.

    JWKS endpoint unreachable → 503 (our failure); an unknown ``kid`` / malformed
    token raises ``PyJWKClientError``/``InvalidTokenError`` for the caller to map to 401.
    """
    try:
        return await run_in_threadpool(client.get_signing_key_from_jwt, token)
    except PyJWKClientConnectionError as exc:
        raise DependencyUnavailableError("identity provider (JWKS) is unreachable") from exc
