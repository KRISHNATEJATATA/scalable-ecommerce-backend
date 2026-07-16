"""Application settings.

Single, typed, fail-fast configuration surface built on ``pydantic-settings``.
Every setting is a field on :class:`AppSettings`; **no code reads ``os.environ``
directly** (see ``.github/copilot-instructions.md``). Adding an env var means
adding a field here *and* an entry in ``.env.example``.

Resolved once on first access (``settings`` / ``get_settings()``) so a missing
required var fails the process at startup rather than at mere import time — the
latter would break test collection and any env without config.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Typed application configuration loaded from the environment / ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- App ---
    app_name: str = "scalable-ecommerce-backend"
    environment: Literal["local", "dev", "staging", "prod"] = "local"
    debug: bool = False
    log_level: str = "INFO"

    # --- Database (required: fail-fast on missing config) ---
    database_url: PostgresDsn = Field(
        ...,
        description="Async SQLAlchemy DSN, e.g. postgresql+asyncpg://user:pass@host:5432/db",
    )
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_pre_ping: bool = True

    # --- Valkey (ephemeral state: rate-limit counters, idempotency keys) ---
    valkey_url: str = "redis://localhost:6379/0"

    # --- Auth: OIDC via Keycloak (app is a pure resource server, Phase 5) ---
    # The app only VALIDATES Keycloak-issued RS256 tokens (JWKS). Keycloak owns
    # login/refresh/passwords. Algorithm is hardcoded to RS256 (alg:none guard).
    keycloak_issuer: str | None = None
    keycloak_realm: str = "ecommerce"
    keycloak_audience: str = "ecommerce-api"
    keycloak_jwks_url: str | None = None
    jwt_algorithm: Literal["RS256"] = "RS256"
    # Admin service-account for user/role management via Keycloak's Admin API.
    keycloak_admin_client_id: str | None = None
    keycloak_admin_client_secret: str | None = None

    # --- HTTP / CORS (bearer-token auth: no cookies → allow_credentials false) ---
    api_v1_prefix: str = "/v1"
    cors_allow_origins: list[str] = Field(default_factory=list)

    # --- Feature flags (plain env booleans; not a flag service) ---
    enable_reviews: bool = False

    # --- S3 / uploads (Phase 7) ---
    s3_bucket: str | None = None
    s3_region: str = "us-east-1"
    s3_endpoint_url: str | None = None  # MinIO locally; None → real AWS S3

    # --- SQS async worker (Phase 8) ---
    sqs_queue_url: str | None = None
    sqs_endpoint_url: str | None = None  # ElasticMQ locally


@lru_cache
def get_settings() -> AppSettings:
    """Return the cached settings singleton (DI-friendly)."""
    return AppSettings()


def __getattr__(name: str) -> AppSettings:
    # lazy singleton via PEP 562 so importing this module doesn't
    # require config; fail-fast still fires on first `settings` access.
    if name == "settings":
        return get_settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
