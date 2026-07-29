"""Valkey product read-cache — the concrete :class:`ProductCachePort`.

Cache-aside over Valkey for the single-product read (``GET /products/{id}``):

- ``product:{id}`` holds the serialized :class:`ProductResponse` under a
  **jittered TTL** (``ttl + rand(0..jitter)``) so a burst of keys populated
  together don't all expire in the same instant and stampede the DB.
- ``product:lock:{id}`` is a ``SET NX`` fill-lock carrying a **unique per-caller
  token**. It is the *single authority* for the whole fill:

  * the winner **renews** it (compare-and-extend) while it reads the DB, so a fill
    slower than the base lock TTL never lets the lock lapse and a second filler
    start — the anti-stampede guard holds for arbitrarily slow reads;
  * the winner writes the cache with **store-if-owner** (compare-and-set): the
    value lands only while the winner still owns the lock;
  * **invalidation deletes the lock as well as the value**, so a fill that began
    before an update finds its lock gone and its store-if-owner is dropped — the
    stale value it read can never be written back. This replaces a separate
    (expiring, therefore racy) generation counter with the lock we already hold.
  * it self-expires, so a crashed holder never wedges the key — a waiter promotes
    once the (un-renewed) lock lapses.

This adapter owns key naming, the two TTLs, and the atomic Lua primitives; the
(de)serialization and the single-filler orchestration live in
``application/service`` so the layer boundary (service → port ← adapter) holds.
"""

from __future__ import annotations

import secrets
import uuid
from typing import Any

from src.catalog.ports.cache import MISS

# Compare-and-delete: drop the lock only if it still holds our token. Prevents
# releasing a lock a different caller re-acquired after ours expired.
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

# Compare-and-extend: refresh the lock TTL only while we still own it. Called
# periodically during a slow DB fill so the lock never lapses under an active
# filler (no duplicate fill / stampede); returns 0 once we've lost ownership.
_RENEW_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""

# Store-if-owner: cache the value only while the fill lock still holds our token.
# An invalidation deletes the lock, so a fill that raced it stores nothing.
_STORE_IF_OWNER_LUA = """
if redis.call('get', KEYS[2]) == ARGV[2] then
    return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[3])
end
return false
"""

# Invalidate: drop the cached value AND the fill lock atomically. Deleting the
# lock signals any in-flight filler (its store-if-owner will now no-op) so a fill
# that began before this update can't write its stale read back afterwards.
_INVALIDATE_LUA = """
redis.call('del', KEYS[2])
return redis.call('del', KEYS[1])
"""

# Evict-if-value: drop the cached value only if it STILL equals the exact payload
# we read as corrupt. A concurrent filler may have replaced the poison entry with
# a fresh valid value between our read and this call — compare-and-delete leaves
# that newer fill intact instead of deleting it.
_EVICT_VALUE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class ValkeyProductCache:
    """Implements :class:`src.catalog.ports.cache.ProductCachePort` over Valkey."""

    def __init__(
        self,
        valkey: Any,
        *,
        ttl_seconds: int,
        ttl_jitter_seconds: int,
        lock_ttl_seconds: int,
        negative_ttl_seconds: int = 10,
    ) -> None:
        self._valkey = valkey
        self._ttl = ttl_seconds
        self._jitter = ttl_jitter_seconds
        self._lock_ttl = lock_ttl_seconds
        self._negative_ttl = negative_ttl_seconds

    @staticmethod
    def _key(product_id: uuid.UUID) -> str:
        return f"product:{product_id}"

    @staticmethod
    def _lock_key(product_id: uuid.UUID) -> str:
        return f"product:lock:{product_id}"

    @staticmethod
    def _as_str(value: Any) -> str | None:
        """Normalise a Valkey reply (bytes by default) to ``str``.

        Decodes with ``errors="replace"`` rather than raising on invalid UTF-8: a
        corrupt entry becomes a non-JSON string that fails the caller's
        deserialization and routes to eviction, instead of a ``UnicodeDecodeError``
        that would bypass eviction and re-hit the DB every read until the TTL lapses.
        """
        if isinstance(value, bytes | bytearray):
            return value.decode(errors="replace")
        return value

    async def get(self, product_id: uuid.UUID) -> str | None:
        """Return the cached serialized product, or ``None`` on a miss."""
        return self._as_str(await self._valkey.get(self._key(product_id)))

    async def store_if_owner(self, product_id: uuid.UUID, payload: str, token: str) -> bool:
        """Cache the product under a jittered TTL, only while ``token`` still owns the fill lock.

        Jittered TTL spreads expiries so co-populated hot keys don't expire in
        lockstep and cause a synchronised mass-miss. The store no-ops (returns
        ``False``) if an invalidation deleted the lock since we acquired it — that
        is what stops a fill from writing a value it read *before* an update.
        """
        ttl = self._ttl + secrets.randbelow(self._jitter + 1)
        result = await self._valkey.eval(
            _STORE_IF_OWNER_LUA, 2, self._key(product_id), self._lock_key(product_id), payload, token, ttl
        )
        return bool(result)

    async def store_miss_if_owner(self, product_id: uuid.UUID, token: str) -> bool:
        """Negative-cache a 404 (the :data:`MISS` tombstone) under a short TTL, store-if-owner.

        Same lock-token guard as :meth:`store_if_owner` (an invalidation that raced
        the fill deleted the lock → this no-ops). The short negative TTL means a
        product created after the 404 becomes visible again quickly, since a
        ``ProductCreated`` is not consumed by the invalidation worker.
        """
        result = await self._valkey.eval(
            _STORE_IF_OWNER_LUA, 2, self._key(product_id), self._lock_key(product_id), MISS, token, self._negative_ttl
        )
        return bool(result)

    async def invalidate(self, product_id: uuid.UUID) -> None:
        """Evict the product and drop any in-flight fill lock (idempotent; absent keys are a no-op)."""
        await self._valkey.eval(_INVALIDATE_LUA, 2, self._key(product_id), self._lock_key(product_id))

    async def evict_value(self, product_id: uuid.UUID, expected: str) -> None:
        """Drop the cached value only if it still equals ``expected`` (compare-and-delete).

        Used to evict a corrupt/undeserializable entry. Two guards matter:

        * it does **not** touch the fill lock (unlike :meth:`invalidate`), so
          concurrent readers keep their single-filler election and don't stampede;
        * it deletes **only** the exact poison payload we read — a concurrent filler
          that already replaced it with a fresh valid value is left intact.

        ponytail: compares the UTF-8 payload we decoded. We only ever write valid
        UTF-8 JSON, so this is byte-exact for our own entries; an externally-injected
        invalid-UTF-8 blob won't match and simply lingers until its TTL (bounded, no
        data loss) rather than risking deletion of a newer valid fill.
        """
        await self._valkey.eval(_EVICT_VALUE_LUA, 1, self._key(product_id), expected)

    async def acquire_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        """Claim the fill lock with a unique ``token`` (``SET NX EX``); ``True`` if won."""
        acquired = await self._valkey.set(self._lock_key(product_id), token, nx=True, ex=self._lock_ttl)
        return bool(acquired)

    async def renew_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        """Extend the fill lock's TTL only if ``token`` still owns it (compare-and-extend).

        Called periodically while the owner reads the DB so a slow fill keeps the
        lock alive; ``False`` once ownership is lost (expired, or an invalidation
        deleted it) — the caller's store-if-owner will then correctly no-op.
        """
        result = await self._valkey.eval(_RENEW_LOCK_LUA, 1, self._lock_key(product_id), token, self._lock_ttl)
        return bool(result)

    async def release_fill_lock(self, product_id: uuid.UUID, token: str) -> None:
        """Release the fill lock only if ``token`` still owns it (compare-and-delete)."""
        await self._valkey.eval(_RELEASE_LOCK_LUA, 1, self._lock_key(product_id), token)

    async def fill_lock_held(self, product_id: uuid.UUID) -> bool:
        """Return whether a fill lock currently exists for the product."""
        return bool(await self._valkey.exists(self._lock_key(product_id)))
