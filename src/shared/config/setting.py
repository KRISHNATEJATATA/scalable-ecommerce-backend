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

from pydantic import Field, PostgresDsn, field_validator, model_validator
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
    # 5xx Problem bodies are always sanitized (a generic message; internals only
    # in the logs). ``true`` may keep raw exception text in the 5xx ``detail``
    # for debugging — refused outright outside local/dev, so a misconfigured
    # deploy can never leak SQL/driver errors to callers.
    verbose_error_details: bool = False
    # --- Database (required: fail-fast on missing config) ---
    database_url: PostgresDsn = Field(
        ...,
        description="Async SQLAlchemy DSN, e.g. postgresql+asyncpg://user:pass@host:5432/db",
    )
    db_pool_size: int = 5
    db_max_overflow: int = 10
    # Workers are single-task loops holding one session at a time, so they get a
    # far smaller pool than the API — every process's pool counts against the same
    # RDS max_connections (budget formula in docs/DEPLOYMENT.md).
    db_worker_pool_size: int = 2
    db_worker_max_overflow: int = 0
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
    # Ceiling on how long a cache miss waits for another caller's fill before it
    # serves itself from the DB. Without it a wedged DB read renews the fill lock
    # forever while every waiter spins on Valkey — a brownout would convert into a
    # request pileup instead of duplicate (bounded) DB reads.
    product_cache_max_fill_wait_seconds: float = Field(default=2.0, gt=0)
    # SQS queue the catalog-cache invalidation consumer drains. LocalStack locally.
    catalog_cache_queue_url: str | None = None

    # --- Auth: OIDC via Keycloak (app is a pure resource server, Phase 5) ---
    # The app only VALIDATES Keycloak-issued RS256 tokens (JWKS). Keycloak owns
    # login/refresh/passwords. Algorithm is hardcoded to RS256 (alg:none guard).
    keycloak_issuer: str | None = None
    keycloak_realm: str = "ecommerce"
    keycloak_audience: str = "ecommerce-api"
    keycloak_jwks_url: str | None = None
    # JWKS fetch timeout (PyJWT defaults to 30s — long enough that a blackholed
    # Keycloak pins a threadpool thread per request).
    jwks_timeout_seconds: float = Field(default=3.0, gt=0)
    # Minimum gap between JWKS refreshes triggered by an *unknown* kid. The kid is
    # read from the unverified token header, so without this an attacker sending
    # random kids forces one outbound Keycloak call per request. Key rotation still
    # resolves, up to one interval later.
    jwks_min_refresh_interval_seconds: float = Field(default=10.0, ge=0)
    jwt_algorithm: Literal["RS256"] = "RS256"
    # Admin service-account for user/role management via Keycloak's Admin API.
    # server_url defaults to the issuer root; override when the issuer the tokens
    # carry (e.g. http://localhost:8080) is not reachable from inside the app.
    keycloak_server_url: str | None = None
    keycloak_admin_client_id: str | None = None
    keycloak_admin_client_secret: str | None = None

    # --- HTTP / CORS (bearer-token auth: no cookies → allow_credentials false) ---
    api_v1_prefix: str = "/v1"
    # Per-dependency deadline for /v1/ready. Must stay well under the ALB's own
    # health-check timeout: a blackholed dependency accepts the connection and
    # never answers, so an unbounded probe just pins a worker until the ALB gives up.
    readiness_probe_timeout_seconds: float = Field(default=2.0, gt=0)
    cors_allow_origins: list[str] = Field(default_factory=list)
    # Peers whose X-Forwarded-* headers we trust. The ALB *appends* to
    # X-Forwarded-For rather than replacing it, so trusting every peer ("*") lets
    # any client forge their own client IP — which would make IP-keyed rate
    # limiting trivially bypassable. Pin this to the ALB / VPC subnet CIDR in
    # every deployed environment; the default is loopback + private ranges.
    trusted_proxies: list[str] = Field(default_factory=lambda: ["127.0.0.1", "10.0.0.0/8", "172.16.0.0/12"])

    # --- Feature flags (plain env booleans; not a flag service) ---
    enable_reviews: bool = False

    # --- Payments (stub gateway behind PaymentGatewayPort) ---
    # Tokens containing this substring decline — the stub's only failure knob,
    # enough to drive both event paths end to end.
    payment_stub_fail_token_substring: str = "decline"
    # HMAC-SHA256 secret verifying gateway webhook bodies
    # (``X-Payment-Signature: sha256=<hex>``). Unset → webhooks are refused
    # (fail closed), never processed unsigned.
    payment_webhook_secret: str | None = None
    # How long a charge may sit ``pending`` before the reconciliation poller asks
    # the gateway what happened (covers normal webhook latency; longer than any
    # plausible delivery delay without racing one).
    payment_reconciliation_grace_seconds: int = Field(default=30, gt=0)
    payment_reconciliation_poll_interval_seconds: float = Field(default=60.0, gt=0)
    payment_reconciliation_batch_size: int = Field(default=50, gt=0)
    # Upper bound of the reconciliation window: a charge still ``pending`` past
    # this age is *abandoned* (guarded flip to ``failed``) instead of asked about
    # again, so rows the gateway never saw stop consuming a batch slot on every
    # pass. Must exceed ``payment_reconciliation_grace_seconds`` (enforced below).
    payment_reconciliation_max_age_seconds: int = Field(default=7 * 24 * 3600, gt=0)

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
    # Visibility timeout for that queue. Image ingest is the heaviest per-message
    # work in the repo (download + sniff + three WebP encodes of a ~40MP source),
    # so SQS's 30s default would redeliver a slow-but-succeeding message mid-flight
    # and burn redrive attempts until it DLQs. Keep it well above the worst-case
    # single-image processing time; s3_bootstrap applies it to the local queue.
    image_visibility_timeout_seconds: int = Field(default=300, gt=0)
    # Lifecycle expiry for raw `uploads/` objects. Nothing references them once the
    # worker has produced the public renditions, and a rejected (possibly malicious)
    # upload must not be retained forever — S3 reclaims them instead of the app.
    image_upload_retention_days: int = Field(default=7, gt=0)
    # Grace added to a presign's expiry before the image worker reaps an abandoned
    # upload (product `pending` but no bytes ever arrived) back to its previous
    # image state. Must stay above the queue's visibility timeout: an upload that
    # landed just before expiry may still be queued or mid-processing, and reaping
    # it would clear the token its flip is guarded on.
    image_upload_reaper_grace_seconds: int = Field(default=900, gt=0)

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
    # ARN namespace the per-event-type topics live under, e.g.
    # "arn:aws:sns:us-east-1:123456789012:" — the full ARN is this + the topic name,
    # so the name still has exactly one source (`bus_topic_prefix`). Set it wherever
    # Terraform owns topic creation (staging/prod): with it the relay resolves ARNs
    # by string and needs only `sns:Publish`, without it it must call
    # `sns:CreateTopic` on every cold start. Leave empty on LocalStack, which has no
    # pre-created topics. `run_relay` refuses to start on real AWS without it.
    bus_topic_arn_prefix: str | None = None
    relay_batch_size: int = Field(default=100, gt=0)
    # Bounded concurrent SNS publishes per claimed batch: serial awaits held the
    # outbox row locks + a pooled connection for batch × RTT.
    relay_publish_concurrency: int = Field(default=10, gt=0)
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

    # --- Cart (Valkey-only pre-checkout basket) ---
    # The cart is client-input-shaped state: both caps keep one caller from
    # amplifying Valkey memory. Abandoned carts expire on a rolling TTL (every
    # read/mutation refreshes it); eviction empties a cart — documented and
    # acceptable for a cart, never for an order.
    cart_max_items: int = Field(default=50, gt=0)  # max distinct lines per cart
    cart_max_qty_per_line: int = Field(default=10, gt=0)  # max units per line
    cart_ttl_seconds: int = Field(default=30 * 24 * 3600, gt=0)  # rolling expiry (~30d of inactivity)
    # SQS queue the cart product-event consumer drains. LocalStack locally.
    cart_queue_url: str | None = None

    # --- Inventory reservations + reaper ---
    # A reservation holds stock (bumps `reserved`) until the checkout saga commits
    # or compensates. The TTL is the backstop for a saga that never does either:
    # the reaper releases anything still held past `expires_at`. Keep the TTL
    # comfortably longer than the saga's own step timeouts, or a slow-but-alive
    # checkout gets its stock reaped out from under it.
    reservation_ttl_seconds: int = Field(default=900, gt=0)  # 15 min hold
    reservation_reaper_poll_interval_seconds: float = Field(default=10.0, gt=0)
    reservation_reaper_batch_size: int = Field(default=100, gt=0)

    # --- Checkout saga (orders) ---
    # Per-step timeout for the orchestrated saga (reserve → charge → commit).
    # Must stay well under RESERVATION_TTL_SECONDS (enforced below): a step that
    # runs longer than the hold lets the reaper reclaim stock from a live
    # checkout. The recovery poller settles orders still `pending` past this age.
    checkout_saga_step_timeout_seconds: int = Field(default=60, gt=0)
    checkout_saga_recovery_batch_size: int = Field(default=50, gt=0)
    # Valkey fast-path TTL for `Idempotency-Key → (body_hash, status, response)`.
    # Eviction only loses the fast path: the DB UNIQUE backstop still prevents a
    # duplicate order, degrading to re-reading the stored order (or 409).
    checkout_idempotency_ttl_seconds: int = Field(default=86400, gt=0)  # ~24h

    # --- Worker metrics export ---
    # Workers don't serve `/metrics` (that's the API process), so their counters
    # are invisible unless exported. Long-running workers get a scrape port;
    # short-lived `--once` runs push to a Pushgateway instead, since a scrape
    # target that exits between scrapes is never sampled. Both opt-in: unset =
    # off, so tests and local runs bind no port and make no network call.
    worker_metrics_port: int | None = Field(default=None, gt=0, le=65535)
    metrics_pushgateway_url: str | None = None

    @field_validator("bus_topic_arn_prefix")
    @classmethod
    def _validate_topic_arn_prefix(cls, value: str | None) -> str | None:
        """Fail-fast on a prefix that would build a malformed topic ARN.

        The publisher appends the topic name verbatim, so a prefix missing its
        trailing ``:`` (or pointing at the wrong service) would produce ARNs that
        fail per-publish at runtime rather than at startup. A trailing ``:`` is
        appended when absent; anything that isn't an SNS ARN namespace is rejected.
        """
        if value is None or not value.strip():
            return None
        prefix = value.strip()
        if not prefix.endswith(":"):
            prefix += ":"
        # arn:<partition>:sns:<region>:<account>:  -> 6 fields, last one empty
        parts = prefix.split(":")
        if len(parts) != 6 or parts[0] != "arn" or parts[2] != "sns" or not parts[3] or not parts[4]:
            raise ValueError(
                "bus_topic_arn_prefix must be an SNS ARN namespace like "
                "'arn:aws:sns:<region>:<account-id>:' (topic name is appended)"
            )
        return prefix

    @property
    def image_public_base_url(self) -> str | None:
        """Unsigned public base for product images (CDN, or the local endpoint/bucket path).

        The endpoint fallback needs BOTH the endpoint and the bucket — an endpoint
        without a bucket would otherwise build a malformed ``<endpoint>/None/<key>``.
        """
        if self.s3_public_base_url:
            return self.s3_public_base_url
        if self.s3_endpoint_url and self.s3_bucket:
            return f"{self.s3_endpoint_url}/{self.s3_bucket}"
        return None

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

    @model_validator(mode="after")
    def _require_reaper_grace_above_visibility_timeout(self) -> "AppSettings":
        """Fail-fast: the abandoned-upload grace must outlast one processing attempt.

        The reaper clears ``image_upload_token``, which is the guard the worker's
        flip is conditioned on. If the grace were <= the queue's visibility timeout,
        a message still being processed (or about to be redelivered) could have its
        product reaped mid-flight, turning a perfectly good upload into a stale flip
        whose renditions are then reclaimed — the merchant's image silently vanishes.
        The relationship is what makes the reaper safe, so it is enforced, not
        documented and hoped for.
        """
        if self.image_upload_reaper_grace_seconds <= self.image_visibility_timeout_seconds:
            raise ValueError(
                "image_upload_reaper_grace_seconds must exceed image_visibility_timeout_seconds "
                f"({self.image_upload_reaper_grace_seconds} <= {self.image_visibility_timeout_seconds}): "
                "an in-flight upload would be reaped mid-processing"
            )
        return self

    @model_validator(mode="after")
    def _require_saga_step_timeout_below_reservation_ttl(self) -> "AppSettings":
        """Fail-fast: a saga step must finish before its stock hold can expire.

        The reaper releases any hold past `expires_at` with no knowledge of the
        saga. If a step routinely ran longer than the TTL, a slow-but-alive
        checkout would get its stock reclaimed mid-flight and compensate a sale
        that should have succeeded. The relationship is what makes the hold
        safe, so it is enforced, not documented and hoped for.
        """
        if self.checkout_saga_step_timeout_seconds >= self.reservation_ttl_seconds:
            raise ValueError(
                "checkout_saga_step_timeout_seconds must stay below reservation_ttl_seconds "
                f"({self.checkout_saga_step_timeout_seconds} >= {self.reservation_ttl_seconds}): "
                "a live checkout would lose its stock to the reaper mid-saga"
            )
        return self

    @model_validator(mode="after")
    def _refuse_verbose_errors_outside_dev(self) -> "AppSettings":
        """Fail-fast: verbose 5xx details are a dev-only debugging aid.

        Any environment a real caller can reach (staging included) must not hand
        raw exception text (SQL statements, driver errors) to callers — reject
        the config at startup instead of leaking at the first 500.
        """
        if self.environment not in ("local", "dev") and self.verbose_error_details:
            raise ValueError(
                "verbose_error_details is only allowed with environment local/dev: "
                f"raw 5xx detail would leak internals ({self.environment})"
            )
        return self

    @model_validator(mode="after")
    def _require_reconciliation_max_age_above_grace(self) -> "AppSettings":
        """Fail-fast: the reconciliation sweep window must be non-empty.

        Rows older than ``payment_reconciliation_max_age_seconds`` are abandoned
        (guarded flip to ``failed``) instead of asked about again. If max_age were
        <= grace, the lookup window ``[grace, max_age]`` would be empty and every
        charge past grace would be abandoned on the first pass — including ones a
        slow-but-alive provider was still confirming. The relationship is what
        makes abandonment safe, so it is enforced, not documented and hoped for.
        """
        if self.payment_reconciliation_max_age_seconds <= self.payment_reconciliation_grace_seconds:
            raise ValueError(
                "payment_reconciliation_max_age_seconds must exceed payment_reconciliation_grace_seconds "
                f"({self.payment_reconciliation_max_age_seconds} <= {self.payment_reconciliation_grace_seconds}): "
                "charges past grace would be abandoned instead of reconciled"
            )
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
