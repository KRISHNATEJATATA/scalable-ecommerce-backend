"""Port (Protocol) for the catalog product read-cache.

Implemented by ``adapters/cache.ValkeyProductCache`` and wired in
``src/shared/container.py``. Typed as a structural contract so the cache-aside
use-case in ``application/service`` and the cache-invalidation consumer depend on
the abstraction, not on Valkey — and tests inject an in-memory fake.

The orchestration (double-checked fill under a single-owner lock that is renewed
during a slow DB read, then a store gated on still owning that lock) lives in the
application service; the adapter owns key naming, the jittered TTL, the lock TTL,
and the atomic (compare-and-delete / -extend / -set) primitives. ``store_if_owner``
persists an opaque serialized ``ProductResponse``; the service does the
(de)serialization.
"""

from __future__ import annotations

import uuid
from typing import Protocol

# Negative-cache tombstone: a distinct non-JSON marker stored for a confirmed 404
# so a burst of misses on an absent id doesn't stampede the DB one-caller-at-a-time.
# Not a valid serialized ``ProductResponse``, so it can never collide with a real hit.
MISS = "\x00miss"


class ProductCachePort(Protocol):
    async def get(self, product_id: uuid.UUID) -> str | None:
        """Return the cached serialized product (or the :data:`MISS` tombstone), or ``None`` on a miss."""
        ...

    async def store_if_owner(self, product_id: uuid.UUID, payload: str, token: str) -> bool:
        """Cache the serialized product under a jittered TTL, only while ``token`` owns the fill lock.

        Returns ``False`` (a no-op) if the fill lock is no longer held by
        ``token`` — i.e. an invalidation deleted it since the fill began. This is
        what closes the cache-aside read/invalidate race without a separate
        (expiring, hence racy) generation counter.
        """
        ...

    async def store_miss_if_owner(self, product_id: uuid.UUID, token: str) -> bool:
        """Cache a :data:`MISS` tombstone (short negative TTL) while ``token`` owns the fill lock.

        Negative-caches a confirmed 404 so concurrent/repeat callers serve ``None``
        from the cache instead of each re-querying the DB. The TTL is deliberately
        short (a later create isn't invalidated — ``ProductCreated`` isn't consumed
        by ``catalog-cache``), so a not-yet-existing id isn't hidden for long.
        """
        ...

    async def invalidate(self, product_id: uuid.UUID) -> None:
        """Evict the product and drop any in-flight fill lock (idempotent; absent keys are a no-op)."""
        ...

    async def evict_value(self, product_id: uuid.UUID, expected: str) -> None:
        """Drop the cached value only if it still equals ``expected`` (compare-and-delete).

        For evicting a corrupt entry without releasing the single-filler lock (which
        :meth:`invalidate` deletes) — otherwise concurrent readers would stampede.
        The compare-and-delete leaves a newer valid value, written by a concurrent
        filler between our read and this call, untouched.
        """
        ...

    async def acquire_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        """Atomically claim the right to fill this key (``SET NX`` on ``token``); ``True`` if won."""
        ...

    async def renew_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        """Extend the fill lock only if ``token`` still owns it (compare-and-extend).

        Called periodically while the owner reads the DB so a fill slower than the
        base lock TTL never lets the lock lapse and a second filler start. ``False``
        once ownership is lost (expired, or an invalidation deleted the lock).
        """
        ...

    async def release_fill_lock(self, product_id: uuid.UUID, token: str) -> None:
        """Release the fill lock **only if** ``token`` still owns it (compare-and-delete)."""
        ...

    async def fill_lock_held(self, product_id: uuid.UUID) -> bool:
        """Return whether a fill lock currently exists for the product."""
        ...
