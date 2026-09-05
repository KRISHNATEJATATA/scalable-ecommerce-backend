"""Cart Valkey adapter tests — the Lua scripts against a real Valkey.

The route/consumer suite (``test_cart.py``) runs against an in-memory fake;
these tests pin the real atomic semantics the fake mirrors: clamp-on-increment
*with version carryover*, cart-full, line-absent, the consumer version gates
(including the ``cjson.null`` comparison), tombstone prune + no-resurrect,
index self-healing, and index-TTL refresh on read. Requires Docker (like the
Postgres suites); no environment-dependent skip.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from testcontainers.core.container import DockerContainer
from valkey.asyncio import Valkey

from src.cart.adapters.valkey.repository import ValkeyCartRepository, _cart_key, _index_key
from src.shared.errors.exceptions import InvalidCartOperationError

TTL = 100


def _await_ready(url: str) -> None:
    """Block until the container answers PING (slow-start race, not a skip)."""

    async def _go() -> None:
        probe = Valkey.from_url(url)
        try:
            for _ in range(50):
                try:
                    if await probe.ping():
                        return
                except Exception:
                    await asyncio.sleep(0.2)
            raise RuntimeError(f"valkey testcontainer never answered PING at {url}")
        finally:
            await probe.aclose()

    asyncio.run(_go())


@pytest.fixture(scope="module")
def _valkey_url():
    with DockerContainer("valkey/valkey:8").with_exposed_ports(6379) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"
        _await_ready(url)
        yield url


@pytest.fixture
async def repo(_valkey_url):
    client = Valkey.from_url(_valkey_url)
    await client.flushall()
    yield ValkeyCartRepository(client, ttl_seconds=TTL)
    await client.flushall()
    await client.aclose()


@pytest.fixture
async def client(_valkey_url):
    client = Valkey.from_url(_valkey_url)
    yield client
    await client.aclose()


async def _add(repo, user, pid, qty=1, **over):
    kw = dict(name="w", unit_price="9.99", image_url=None, quantity=qty, max_items=5, max_per_line=3)
    kw |= over
    return await repo.add_item(user, product_id=pid, **kw)


async def test_increment_preserves_version_and_clamps(repo):
    """Regression: the increment path once rebuilt the line versionless,
    letting a stale event re-apply over fresher data."""
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    assert await repo.refresh_product(pid, name="w2", unit_price="12.50", product_version=2) == 1
    cart = await _add(repo, user, pid, qty=2)
    (line,) = cart.items
    assert line.quantity == 3  # clamped to the cap
    assert line.product_version == 2  # carried over, not wiped
    assert line.name == "w" and line.unit_price == "9.99"  # re-snapped from the add
    assert await repo.refresh_product(pid, name="stale", unit_price="0.01", product_version=1) == 0


async def test_cart_full(repo):
    user = uuid.uuid4()
    await _add(repo, user, uuid.uuid4(), max_items=1)
    with pytest.raises(InvalidCartOperationError):
        await _add(repo, user, uuid.uuid4(), max_items=1)


async def test_set_remove_clear(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    assert await repo.set_quantity(user, product_id=pid, quantity=1, max_per_line=3) is None
    await _add(repo, user, pid)
    cart = await repo.set_quantity(user, product_id=pid, quantity=2, max_per_line=3)
    assert cart is not None and cart.items[0].quantity == 2
    await repo.set_quantity(user, product_id=pid, quantity=0, max_per_line=3)
    assert await repo.get_cart(user) is None
    assert not await client.exists(_cart_key(user))  # key dropped once only $meta would remain
    await _add(repo, user, pid)
    await repo.clear_cart(user)
    assert await repo.get_cart(user) is None
    assert await client.smembers(_index_key(pid)) == []


async def test_consumer_gates_tombstone_and_self_heal(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    assert await repo.refresh_product(pid, name="w2", unit_price="12.50", product_version=None) == 1
    assert await repo.refresh_product(pid, name="w3", unit_price="13.00", product_version=2) == 1
    assert await repo.refresh_product(pid, name="stale", unit_price="0.01", product_version=2) == 0
    assert await repo.refresh_product(pid, name="legacy", unit_price="0.02", product_version=None) == 0
    assert await repo.prune_product(pid) == 1
    assert await repo.refresh_product(pid, name="ghost", unit_price="99.00", product_version=9) == 0
    assert await repo.prune_product(pid) == 0
    assert await client.smembers(_index_key(pid)) == []
    # A stale index member (cart expired away) self-heals on the next event.
    assert await repo.refresh_product(pid, name="ghost", unit_price="99.00", product_version=9) == 0
    assert await client.smembers(_index_key(pid)) == []


async def test_get_refreshes_index_ttl(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    await client.expire(_index_key(pid), 5)  # simulate a read-heavy cart near index expiry
    assert await client.ttl(_index_key(pid)) <= 5
    assert await repo.get_cart(user) is not None
    assert await client.ttl(_index_key(pid)) > 5  # read refreshed it back to ~TTL
