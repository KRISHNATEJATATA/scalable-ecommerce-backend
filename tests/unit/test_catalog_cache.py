"""Product cache-aside tests — the service orchestration we own.

Covers the acceptance criteria over an in-memory ``FakeProductCache`` that mirrors
the port's atomic contract (token-owned lock, store-if-owner): a read populates
the cache and the next read skips the DB; concurrent misses on one hot key drive a
**single** DB fill (single-owner lock + double-checked fill, not a stampede) — even
when the fill is slower than the lock TTL (the holder renews the lock); and an
invalidation that races a fill deletes the lock so the fill's stale read is dropped
rather than cached. The adapter's Valkey Lua (compare-and-delete/-extend/-set) is
exercised against a real Valkey in the manual smoke, not here.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.catalog.adapters.cache_worker import make_invalidation_handler
from src.catalog.application.service import CatalogService
from src.catalog.domain.image_status import ImageStatus
from src.catalog.ports.cache import MISS


class FakeProductCache:
    """In-memory ProductCachePort: token-owned lock + store-if-owner.

    Invalidation deletes the value *and* the lock (mirroring the adapter's atomic
    Lua), which is what makes a store racing an invalidation a no-op — there is no
    separate generation counter.
    """

    def __init__(self) -> None:
        self.data: dict[uuid.UUID, str] = {}
        self.locks: dict[uuid.UUID, str] = {}

    async def get(self, product_id: uuid.UUID) -> str | None:
        return self.data.get(product_id)

    async def store_if_owner(self, product_id: uuid.UUID, payload: str, token: str) -> bool:
        if self.locks.get(product_id) == token:  # store only while we still own the lock
            self.data[product_id] = payload
            return True
        return False

    async def store_miss_if_owner(self, product_id: uuid.UUID, token: str) -> bool:
        if self.locks.get(product_id) == token:  # negative-cache the 404 under the lock
            self.data[product_id] = MISS
            return True
        return False

    async def invalidate(self, product_id: uuid.UUID) -> None:
        self.data.pop(product_id, None)
        self.locks.pop(product_id, None)  # drop any in-flight fill lock → its store no-ops

    async def evict_value(self, product_id: uuid.UUID, expected: str) -> None:
        if self.data.get(product_id) == expected:  # compare-and-delete: leave a newer valid fill
            self.data.pop(product_id, None)  # value only — the fill lock is left intact

    async def acquire_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        if product_id in self.locks:
            return False
        self.locks[product_id] = token
        return True

    async def renew_fill_lock(self, product_id: uuid.UUID, token: str) -> bool:
        return self.locks.get(product_id) == token  # in-memory lock never lapses; just report ownership

    async def release_fill_lock(self, product_id: uuid.UUID, token: str) -> None:
        if self.locks.get(product_id) == token:  # compare-and-delete
            self.locks.pop(product_id, None)

    async def fill_lock_held(self, product_id: uuid.UUID) -> bool:
        return product_id in self.locks


@dataclass
class _Row:
    """Duck-typed catalog row (what ``to_domain`` reads)."""

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    image_status: str
    created_at: datetime
    updated_at: datetime


class CountingRepo:
    """Repository fake that counts DB reads and can yield the loop mid-read."""

    def __init__(self, row: _Row | None, *, delay: float = 0.0) -> None:
        self._row = row
        self._delay = delay
        self.get_calls = 0

    async def get_product(self, product_id: uuid.UUID):
        self.get_calls += 1
        await asyncio.sleep(self._delay)  # yield so a racing task can interleave
        return self._row


def _row(product_id: uuid.UUID) -> _Row:
    now = datetime.now(UTC)
    return _Row(
        id=product_id,
        merchant_id=uuid.uuid4(),
        name="Widget",
        description="a thing",
        category="misc",
        price=Decimal("9.99"),
        image_key=None,
        image_status=ImageStatus.NONE.value,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_read_populates_cache_then_next_read_skips_db() -> None:
    pid = uuid.uuid4()
    cache = FakeProductCache()
    repo = CountingRepo(_row(pid))
    service = CatalogService(repo, cache=cache)

    first = await service.get_product(pid)
    assert first is not None and first.id == pid
    assert repo.get_calls == 1
    assert pid in cache.data  # populated

    second = await service.get_product(pid)
    assert second is not None and second.id == pid
    assert repo.get_calls == 1  # served from cache — no second DB hit


@pytest.mark.asyncio
async def test_concurrent_misses_hot_key_single_db_fill() -> None:
    pid = uuid.uuid4()
    cache = FakeProductCache()
    # A slow DB read widens the race window so every waiter is in-flight during the fill.
    repo = CountingRepo(_row(pid), delay=0.05)
    service = CatalogService(repo, cache=cache)

    results = await asyncio.gather(*(service.get_product(pid) for _ in range(20)))

    assert all(r is not None and r.id == pid for r in results)
    assert repo.get_calls == 1  # single-owner lock + double-check → exactly one DB fill


@pytest.mark.asyncio
async def test_waiters_do_not_fall_back_to_db_while_fill_active() -> None:
    """Even a fill slower than the old fixed 1s cap must stay a single DB fill:
    waiters keep waiting while the lock is held, never racing the DB themselves."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    repo = CountingRepo(_row(pid), delay=1.3)  # longer than the retired 1s waiter cap
    service = CatalogService(repo, cache=cache)

    results = await asyncio.gather(*(service.get_product(pid) for _ in range(10)))

    assert all(r is not None for r in results)
    assert repo.get_calls == 1  # no waiter fell back to the DB


@pytest.mark.asyncio
async def test_no_cache_falls_through_to_db() -> None:
    pid = uuid.uuid4()
    repo = CountingRepo(_row(pid))
    service = CatalogService(repo)  # cache is None

    assert (await service.get_product(pid)) is not None
    assert (await service.get_product(pid)) is not None
    assert repo.get_calls == 2  # every read hits the DB when caching is disabled


@pytest.mark.asyncio
async def test_valkey_fault_degrades_to_db_read() -> None:
    """A Valkey failure must not surface as a 5xx — the read falls back to the DB
    (the graceful-degradation contract in docs/RUNBOOK.md)."""

    class BrokenCache(FakeProductCache):
        async def get(self, product_id: uuid.UUID) -> str | None:
            raise ConnectionError("valkey down")

    pid = uuid.uuid4()
    repo = CountingRepo(_row(pid))
    service = CatalogService(repo, cache=BrokenCache())

    result = await service.get_product(pid)
    assert result is not None and result.id == pid  # served from DB despite the cache fault
    assert repo.get_calls == 1


@pytest.mark.asyncio
async def test_missing_product_is_negative_cached() -> None:
    """A 404 is negative-cached (MISS tombstone) so a second read serves None from
    the cache instead of re-querying the DB — no per-request stampede on absent ids."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    repo = CountingRepo(None)
    service = CatalogService(repo, cache=cache)

    assert (await service.get_product(pid)) is None
    assert cache.data.get(pid) == MISS  # 404 tombstone stored

    assert (await service.get_product(pid)) is None
    assert repo.get_calls == 1  # second read served the negative cache — no second DB hit


@pytest.mark.asyncio
async def test_concurrent_misses_absent_id_single_db_fill() -> None:
    """A burst of concurrent reads for an absent id must hit the DB once, then serve
    the negative cache — not query the DB once per waiter."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    repo = CountingRepo(None, delay=0.05)
    service = CatalogService(repo, cache=cache)

    results = await asyncio.gather(*(service.get_product(pid) for _ in range(20)))

    assert all(r is None for r in results)
    assert repo.get_calls == 1  # single fill; waiters served the MISS tombstone


@pytest.mark.asyncio
async def test_corrupt_cache_entry_is_evicted_and_refilled() -> None:
    """A cache entry that no longer deserializes is evicted and refilled from the DB,
    rather than silently degrading to a DB read on every future request."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    cache.data[pid] = "{not valid product json"  # poison entry
    repo = CountingRepo(_row(pid))
    service = CatalogService(repo, cache=cache)

    result = await service.get_product(pid)
    assert result is not None and result.id == pid  # served correctly from the DB
    assert repo.get_calls == 1
    assert cache.data.get(pid) not in (None, "{not valid product json")  # poison replaced with a valid entry

    # And the refilled entry now serves without another DB hit.
    again = await service.get_product(pid)
    assert again is not None and again.id == pid
    assert repo.get_calls == 1


@pytest.mark.asyncio
async def test_concurrent_corrupt_entry_single_db_fill() -> None:
    """A corrupt entry seen by many concurrent readers must still drive a SINGLE DB
    fill: eviction drops only the value, not the fill lock, so readers don't stampede."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    cache.data[pid] = "{not valid product json"
    repo = CountingRepo(_row(pid), delay=0.05)  # slow read widens the race window
    service = CatalogService(repo, cache=cache)

    results = await asyncio.gather(*(service.get_product(pid) for _ in range(20)))

    assert all(r is not None and r.id == pid for r in results)
    assert repo.get_calls == 1  # single filler — corrupt eviction kept the lock intact


@pytest.mark.asyncio
async def test_evict_value_spares_a_newer_fill() -> None:
    """Compare-and-delete: eviction of a corrupt entry must NOT delete a fresh valid
    value a concurrent filler stored between our read and our evict."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    corrupt = "{not valid product json"
    # A filler replaced the poison entry with a fresh valid payload after we read it.
    fresh = '{"id": "valid"}'
    cache.data[pid] = fresh

    await cache.evict_value(pid, corrupt)  # we still pass the OLD payload we saw

    assert cache.data[pid] == fresh  # newer valid fill survives


@pytest.mark.asyncio
async def test_invalidation_during_fill_is_not_overwritten() -> None:
    """The lock-token guard closes the read/invalidate race: an invalidation that
    lands mid-fill deletes the fill lock, so the filler's store-if-owner no-ops and
    the stale value it read is never cached."""
    pid = uuid.uuid4()
    cache = FakeProductCache()
    repo = CountingRepo(_row(pid), delay=0.05)
    service = CatalogService(repo, cache=cache)

    async def read():
        return await service.get_product(pid)

    async def invalidate_mid_fill():
        await asyncio.sleep(0.02)  # while the filler holds the lock, before it stores
        await make_invalidation_handler(cache)({"type": "ProductUpdated", "data": {"product_id": str(pid)}})

    result, _ = await asyncio.gather(read(), invalidate_mid_fill())
    assert result is not None  # the caller still gets its answer
    assert pid not in cache.data  # but the stale value was NOT left in the cache


@pytest.mark.asyncio
async def test_invalidation_handler_evicts_key() -> None:
    pid = uuid.uuid4()
    cache = FakeProductCache()
    cache.data[pid] = '{"stale": true}'

    handler = make_invalidation_handler(cache)
    await handler({"type": "ProductUpdated", "data": {"product_id": str(pid)}})
    assert pid not in cache.data

    # Idempotent: a second delete (already-absent key) is a no-op, not an error.
    await handler({"type": "ProductDeleted", "data": {"product_id": str(pid)}})
    assert pid not in cache.data
