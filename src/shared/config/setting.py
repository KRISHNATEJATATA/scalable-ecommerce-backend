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

from pydantic import Field, PostgresDsn, model_validator
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

    # --- Product read-cache ---
    # Single-product reads are cached under ``product:{id}`` and invalidated by the
    # catalog-cache consumer on ProductUpdated/ProductDeleted. Jittered TTL + a
    # SET NX fill-lock guard a hot key against a cache-stampede.
    product_cache_enabled: bool = True
    product_cache_ttl_seconds: int = Field(default=300, gt=0)  # base entry TTL
    product_cache_ttl_jitter_seconds: int = Field(default=60, ge=0)  # random 0..N added (anti-stampede)
    product_cache_lock_ttl_seconds: int = Field(default=5, gt=0)  # fill-lock TTL (self-heals a crashed filler)
    product_cache_negative_ttl_seconds: int = Field(
        default=10, gt=0
    )  # 404 tombstone TTL (short: a later create shows up fast)
    # SQS queue the catalog-cache invalidation consumer drains. LocalStack locally.
    catalog_cache_queue_url: str | None = None

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

    # --- S3 / uploads (Phase 7-8) ---
    s3_bucket: str | None = None
    s3_region: str = "us-east-1"
    s3_endpoint_url: str | None = None  # LocalStack S3 locally; None → real AWS S3
    # Public CDN base (CloudFront in prod; LocalStack path locally) for serving
    # product images UNSIGNED. None → fall back to f"{s3_endpoint_url}/{s3_bucket}".
    s3_public_base_url: str | None = None

    # --- Secure image uploads + worker ---
    image_max_upload_bytes: int = Field(default=5 * 1024 * 1024, gt=0)  # presign policy ceiling (5 MiB)
    image_max_dimension: int = Field(default=2048, gt=0)  # worker re-encode clamp (px, longest side)
    image_max_source_pixels: int = Field(default=40_000_000, gt=0)  # ~40MP decompression-bomb guard (pre-decode)
    image_upload_ttl_seconds: int = Field(default=300, gt=0)  # presigned-POST validity (~5 min)
    # SQS queue the ImageWorker drains (S3 ObjectCreated → SQS). LocalStack locally.
    image_queue_url: str | None = None

    # --- SQS async worker (Phase 8) ---
    sqs_queue_url: str | None = None
    sqs_endpoint_url: str | None = None  # ElasticMQ locally

    # --- Event bus: transactional outbox → SNS/SQS ---
    # Relay publishes outbox rows to per-event-type SNS topics; consumers read
    # per-subscription SQS queues with DLQs. LocalStack locally; None → real AWS
    # (ECS task role supplies credentials, no keys in code).
    bus_endpoint_url: str | None = None
    bus_region: str = "us-east-1"
    bus_topic_prefix: str = "ecommerce-"  # SNS topic name = f"{prefix}{EventType}"
    relay_batch_size: int = Field(default=100, gt=0)
    relay_poll_interval_seconds: float = Field(default=1.0, gt=0)
    consumer_max_messages: int = Field(default=10, ge=1, le=10)  # SQS receive batch (max 10)
    consumer_wait_time_seconds: int = Field(default=10, ge=0, le=20)  # SQS long-poll seconds
    consumer_dedup_ttl_seconds: int = Field(default=86400, gt=0)  # completion-marker TTL (~24h)
    # Short processing-lease TTL: a claim expires this fast, so a worker that
    # crashes mid-handle releases the event for redrive instead of blocking it for
    # the full dedup TTL. Keep it <= the SQS queue's visibility timeout (i.e. set
    # the queue visibility >= this) so a crashed worker's lease has expired by the
    # time SQS redelivers — otherwise the redelivery keeps finding a held lease,
    # bounces, and prematurely hits maxReceiveCount → DLQ. bus_bootstrap sets the
    # local queue visibility from this value.
    consumer_lease_ttl_seconds: int = Field(default=60, gt=0)

    @model_validator(mode="after")
    def _require_public_image_base_in_cloud(self) -> "AppSettings":
        """Fail-fast: a real-AWS deploy serving images MUST set the public CDN base.

        When ``s3_bucket`` is set but ``s3_endpoint_url`` is ``None`` (real AWS S3,
        i.e. staging/prod), ``s3_public_base_url`` is the only way to build an
        unsigned image URL — without it product responses would emit a broken
        ``None/<bucket>/<key>`` link. Reject the config at startup instead.
        """
        if self.s3_bucket and self.s3_endpoint_url is None and not self.s3_public_base_url:
            raise ValueError("s3_public_base_url is required when s3_bucket is set without s3_endpoint_url (real AWS)")
        return self


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
