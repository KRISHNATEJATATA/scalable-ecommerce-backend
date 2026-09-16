"""App-level Valkey token bucket — the shared rate-limit primitive.

One bucket per *subject* (an authenticated ``sub`` or, for unauthenticated routes, a
client IP) per *bucket name* (checkout / write / upload). The bucket is a classic
token bucket: ``capacity`` is the burst ceiling, and ``refill`` tokens are restored
every ``period_seconds`` (sustained rate ≈ ``refill / period``). A request consumes
one token; with none left it is refused (the dependency raises ``RateLimitExceededError``
→ 429), and ``retry_after`` says how long until one accrues.

The whole read-refill-consume-write is **one atomic Lua script** so concurrent
requests to the same key serialize in Valkey (no lost updates, no over-admission),
mirroring how :mod:`src.catalog.adapters.cache` owns its compare-and-* primitives.

Time is passed in from the client (``now_us`` microsecond epoch) rather than read
with Redis ``TIME``: a server-side clock inside the script would make the result
non-deterministic and untestable, and the injected clock keeps refill/recovery
exactly reproducible in tests. Two ECS replicas then measure slightly different
``now`` for the same key; the error is bounded by their clock skew and skews the
effective rate by at most that much. Rate limiting is best-effort admission control
(the ALB WAF rules are the prod backstop), so per-key clock skew across a handful of
replicas is acceptable — not worth a shared clock.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from src.shared.config.setting import AppSettings

#: Bucket names. Fixed vocabulary — :func:`config_for` maps each to its settings trio,
#: so an unknown name is a wiring bug (``ValueError``), never a silent unlimited path.
BUCKET_CHECKOUT = "checkout"
BUCKET_WRITE = "write"
BUCKET_UPLOAD = "upload"

# Token bucket, fully atomic. Fields are a hash so we store the float token count and
# the last-refill timestamp together. Fresh buckets start full. A negative elapsed
# (a replica's clock ran backward) refills nothing rather than shaving tokens.
# Returns {allowed (0/1), remaining (floor of tokens after the decision), retry_after_us}.
_TOKEN_BUCKET_LUA = """
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local rate = refill / period
local fields = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(fields[1])
local ts = tonumber(fields[2])
if tokens == nil or ts == nil then
    tokens = capacity
    ts = now
else
    local elapsed = (now - ts) / 1000000.0
    if elapsed > 0 then
        tokens = math.min(capacity, tokens + elapsed * rate)
        ts = now
    end
end
local allowed = 0
local retry_after_us = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_after_us = math.ceil((1 - tokens) / rate * 1000000.0)
end
-- Reclaim an idle bucket once a full recharge (capacity/rate) plus one period would
-- have passed; the next request then simply re-seeds it to full.
local ttl = math.ceil(capacity / rate) + period
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
redis.call('EXPIRE', KEYS[1], ttl)
return {allowed, math.floor(tokens), retry_after_us}
"""


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """The three numbers that shape one bucket."""

    capacity: int
    refill: int
    period_seconds: int


@dataclass(frozen=True, slots=True)
class BucketDecision:
    """The outcome of one :meth:`ValkeyTokenBucket.allow`."""

    allowed: bool
    remaining: int
    retry_after_seconds: int


def config_for(settings: AppSettings, bucket: str) -> RateLimitConfig:
    """Resolve a bucket name to its typed settings trio (config rule: never inline)."""
    if bucket == BUCKET_CHECKOUT:
        return RateLimitConfig(
            settings.rate_limit_checkout_capacity,
            settings.rate_limit_checkout_refill,
            settings.rate_limit_checkout_refill_seconds,
        )
    if bucket == BUCKET_WRITE:
        return RateLimitConfig(
            settings.rate_limit_write_capacity,
            settings.rate_limit_write_refill,
            settings.rate_limit_write_refill_seconds,
        )
    if bucket == BUCKET_UPLOAD:
        return RateLimitConfig(
            settings.rate_limit_upload_capacity,
            settings.rate_limit_upload_refill,
            settings.rate_limit_upload_refill_seconds,
        )
    raise ValueError(f"unknown rate-limit bucket: {bucket!r}")


class ValkeyTokenBucket:
    """A token bucket whose state lives entirely in Valkey (no process-local counters,
    so every API replica shares one view of a subject's budget)."""

    def __init__(self, valkey: Any) -> None:
        # Typed ``Any`` like every other Valkey adapter (ValkeyProductCache,
        # ValkeyIdempotencyStore): the redis-py-compatible stubs declare the sync
        # client's ``eval -> str``, which doesn't describe the async client's replies.
        self._valkey = valkey

    async def allow(self, *, key: str, config: RateLimitConfig, now_us: int | None = None) -> BucketDecision:
        """Spend one token from ``key``'s bucket, refilling first; report the outcome.

        ``now_us`` is injectable so tests drive refill/recovery deterministically
        (no sleeping on wall-clock); production defaults to the current epoch.
        """
        timestamp = time.time_ns() // 1000 if now_us is None else now_us
        allowed, remaining, retry_after_us = await self._valkey.eval(
            _TOKEN_BUCKET_LUA,
            1,
            key,
            # ARGV cross into Lua as strings; ``tonumber`` parses them there.
            str(config.capacity),
            str(config.refill),
            str(config.period_seconds),
            str(timestamp),
        )
        return BucketDecision(
            allowed=bool(allowed),
            remaining=int(remaining),
            retry_after_seconds=max(1, math.ceil(int(retry_after_us) / 1_000_000)),
        )
