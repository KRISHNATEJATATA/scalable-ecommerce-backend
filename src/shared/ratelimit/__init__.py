"""App-level Valkey token-bucket rate limiting.

Public surface (import from here, not the submodules):

- :func:`rate_limited` / :func:`ip_rate_limited` — FastAPI dependencies returning a
  reusable ``Depends`` marker for a named bucket, keyed by the caller's ``sub`` or
  (fallback, unauthenticated routes) the client IP.
- Bucket-name constants (:data:`BUCKET_CHECKOUT` / :data:`BUCKET_WRITE` /
  :data:`BUCKET_UPLOAD`) — the fixed vocabulary :func:`config_for` understands.
- :class:`ValkeyTokenBucket` (+ :class:`RateLimitConfig` / :class:`BucketDecision`) —
  the atomic Valkey primitive, exercised directly by tests.
"""

from src.shared.ratelimit.dependencies import ip_rate_limited, rate_limited
from src.shared.ratelimit.token_bucket import (
    BUCKET_CHECKOUT,
    BUCKET_UPLOAD,
    BUCKET_WRITE,
    BucketDecision,
    RateLimitConfig,
    ValkeyTokenBucket,
    config_for,
)

__all__ = [
    "BUCKET_CHECKOUT",
    "BUCKET_UPLOAD",
    "BUCKET_WRITE",
    "BucketDecision",
    "RateLimitConfig",
    "ValkeyTokenBucket",
    "config_for",
    "ip_rate_limited",
    "rate_limited",
]
