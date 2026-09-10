"""The Valkey cache adapter against a **real** Valkey (Testcontainers).

``tests/unit/test_catalog_cache.py`` covers the service orchestration with an
in-memory fake; this module is the other half of the contract: the adapter's Lua
primitives (owner-checked release/renew, store-if-owner, atomic invalidate,
compare-and-delete eviction) and reply-shape handling executed by the actual
engine the worker runs — a wrong KEYS/ARGV arity or a reply-shape surprise ships
silently without these.
"""

from __future__ import annotations

import uuid

import pytest
from valkey.asyncio import Valkey

from src.catalog.adapters.cache import ValkeyProductCache
from src.catalog.ports.cache import MISS

# ``real_valkey`` (ready-gated, per-test-flushed client on the session-scoped
# container) lives in tests/unit/conftest.py — its ping-until-ready gate matters
# most here, where the container may still be starting up under CI load.


@pytest.fixture
def cache(real_valkey) -> ValkeyProductCache:
    # jitter=0 makes the entry TTL deterministic where a test asserts it.
    return ValkeyProductCache(real_valkey, ttl_seconds=60, ttl_jitter_seconds=0, lock_ttl_seconds=5)


async def test_get_miss_returns_none_and_a_owned_store_roundtrips(cache):
    pid = uuid.uuid4()
    assert await cache.get(pid) is None  # miss

    await cache.acquire_fill_lock(pid, "tok-1")
    assert await cache.store_if_owner(pid, '{"id": "p"}', "tok-1") is True
    assert await cache.get(pid) == '{"id": "p"}'  # reply shape: bytes → str, byte-exact


async def test_store_if_owner_requires_the_live_lock_token(cache, real_valkey):
    pid = uuid.uuid4()
    assert await cache.acquire_fill_lock(pid, "tok-1") is True
    assert await cache.store_if_owner(pid, "payload-1", "tok-1") is True
    assert await cache.get(pid) == "payload-1"

    # A stale/foreign token stores nothing (the invalidation-race guarantee).
    await cache.invalidate(pid)
    assert await cache.acquire_fill_lock(pid, "tok-2") is True
    assert await cache.store_if_owner(pid, "stale-payload", "tok-1") is False
    assert await cache.get(pid) is None


async def test_acquire_is_single_flight_and_release_is_owner_checked(cache, real_valkey):
    pid = uuid.uuid4()
    assert await cache.acquire_fill_lock(pid, "tok-1") is True
    assert await cache.acquire_fill_lock(pid, "tok-2") is False  # second caller loses

    await cache.release_fill_lock(pid, "wrong-token")
    assert await cache.fill_lock_held(pid) is True  # wrong token must not release

    await cache.release_fill_lock(pid, "tok-1")
    assert await cache.fill_lock_held(pid) is False
    assert await cache.acquire_fill_lock(pid, "tok-2") is True  # freed for the next filler


async def test_renew_extends_only_for_the_owner(cache, real_valkey):
    pid = uuid.uuid4()
    await cache.acquire_fill_lock(pid, "tok-1")
    assert await cache.renew_fill_lock(pid, "tok-1") is True
    assert await cache.renew_fill_lock(pid, "not-the-owner") is False

    ttl = await real_valkey.ttl(f"product:lock:{pid}")
    assert 0 < ttl <= 5  # renewed back to (at most) the lock TTL, not extended past it

    await real_valkey.delete(f"product:lock:{pid}")
    assert await cache.renew_fill_lock(pid, "tok-1") is False  # expired lock can't be revived


async def test_invalidate_drops_value_and_lock_atomically(cache):
    pid = uuid.uuid4()
    await cache.acquire_fill_lock(pid, "tok-1")
    await cache.store_if_owner(pid, "payload", "tok-1")
    await cache.invalidate(pid)
    assert await cache.get(pid) is None
    assert await cache.fill_lock_held(pid) is False
    await cache.invalidate(pid)  # idempotent on absent keys


async def test_negative_cache_stores_the_miss_tombstone(cache):
    pid = uuid.uuid4()
    await cache.acquire_fill_lock(pid, "tok-1")
    assert await cache.store_miss_if_owner(pid, "tok-1") is True
    assert await cache.get(pid) == MISS


async def test_evict_value_deletes_only_the_exact_poison_payload(cache, real_valkey):
    pid = uuid.uuid4()
    key = f"product:{pid}"
    await real_valkey.set(key, "poison")
    await cache.evict_value(pid, "poison")
    assert await real_valkey.get(key) is None

    await real_valkey.set(key, "fresh-valid-fill")
    await cache.evict_value(pid, "poison")  # stale expectation must not delete the newer fill
    assert await real_valkey.get(key) == b"fresh-valid-fill"


async def test_entry_ttl_is_the_base_plus_at_most_the_jitter(_valkey_server):
    host, port = _valkey_server

    async def _ttl_for(jitter: int) -> int:
        client = Valkey(host=host, port=port)
        try:
            await client.flushdb()
            cache = ValkeyProductCache(client, ttl_seconds=50, ttl_jitter_seconds=jitter, lock_ttl_seconds=5)
            pid = uuid.uuid4()
            await cache.acquire_fill_lock(pid, "t")
            await cache.store_if_owner(pid, "p", "t")
            return await client.ttl(f"product:{pid}")
        finally:
            await client.aclose()

    base = await _ttl_for(0)
    assert base == 50  # jitter=0 → exactly the base TTL
    jitters = [await _ttl_for(30) for _ in range(8)]
    assert all(50 <= t <= 80 for t in jitters), jitters
    assert len(set(jitters)) > 1  # the jitter actually varies (anti-lockstep expiry)


async def test_invalid_utf8_payload_decodes_to_a_replace_string_not_an_error(cache, real_valkey):
    pid = uuid.uuid4()
    await real_valkey.set(f"product:{pid}", b"\xff\xfe-not-json")
    value = await cache.get(pid)  # must not raise UnicodeDecodeError
    assert isinstance(value, str) and "\ufffd" in value
