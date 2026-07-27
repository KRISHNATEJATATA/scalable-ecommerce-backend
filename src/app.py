"""FastAPI application factory.

``create_app()`` builds and wires the app: logging, middleware (proxy headers,
CORS, request-id, security headers), RFC 9457 exception handlers, routers, and a
lifespan that owns the Postgres engine + Valkey client. Interactive docs are
served in dev only and hidden in prod.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from src.catalog.api import routes as catalog_routes
from src.identity.adapters.keycloak.admin_client import KeycloakIdentityAdmin
from src.identity.api import routes as identity_routes
from src.shared.api import health, metrics
from src.shared.auth.jwks import build_jwks_client
from src.shared.clients import postgres_client, valkey_client
from src.shared.config.logging import setup_logging
from src.shared.config.setting import AppSettings, get_settings
from src.shared.errors.exception_handlers import register_exception_handlers
from src.shared.middleware.security import RequestIDMiddleware, SecurityHeadersMiddleware


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Own the shared connection pools + auth clients for the app's lifetime."""
    settings: AppSettings = app.state.settings
    app.state.db_engine = postgres_client.create_engine(settings)
    app.state.db_sessionmaker = postgres_client.create_sessionmaker(app.state.db_engine)
    app.state.valkey = valkey_client.create_client(settings)
    # Process-wide JWKS client (reuses PyJWT's kid cache) + Keycloak admin adapter
    # (constructed lazily-connecting: no network at startup).
    app.state.jwks_client = build_jwks_client(settings)
    app.state.identity_admin = KeycloakIdentityAdmin(settings)
    try:
        yield
    finally:
        await app.state.db_engine.dispose()
        await app.state.valkey.aclose()


def create_app(settings: AppSettings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    is_prod = settings.environment == "prod"
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        lifespan=_lifespan,
        # Hide interactive docs in prod; free everywhere else.
        docs_url=None if is_prod else "/docs",
        redoc_url=None if is_prod else "/redoc",
        openapi_url=None if is_prod else "/openapi.json",
    )
    app.state.settings = settings

    # Trust ALB-forwarded scheme/host so redirects and client IPs are correct.
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
    # Bearer-token auth uses no cookies → allow_credentials stays false.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RequestIDMiddleware)

    register_exception_handlers(app)

    app.include_router(health.router, prefix=settings.api_v1_prefix)
    app.include_router(identity_routes.router, prefix=settings.api_v1_prefix)
    app.include_router(catalog_routes.router, prefix=settings.api_v1_prefix)
    app.include_router(metrics.router)

    return app
