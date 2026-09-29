"""Cart Valkey adapter tests — the Lua scripts against a real Valkey.

The route/consumer suite (``test_cart.py``) runs against an in-memory fake;
these tests pin the real atomic semantics the fake mirrors: clamp-on-increment
*with version carryover*, cart-full, line-absent, projection version gates,
tombstone prune + no-resurrect, and bounded lazy reconciliation. Requires
Docker (like the Postgres suites); no environment-dependent skip.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from testcontainers.core.container import DockerContainer
from valkey.asyncio import Valkey

from src.cart.adapters.valkey.repository import ValkeyCartRepository, _cart_key, _product_key
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
    await repo.get_cart(user)
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


async def test_consume_subtracts_purchases_and_leaves_other_lines_alone(repo, client):
    """Checkout's consume: the purchased line goes,
    lines added after checkout's snapshot survive — deleting the hash instead
    would be the data-loss bug."""
    user, a, b = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, a)
    await _add(repo, user, b, qty=2)
    await repo.consume_lines(user, lines=[(a, 1)])
    cart = await repo.get_cart(user)
    assert cart is not None
    assert [(line.product_id, line.quantity) for line in cart.items] == [(str(b), 2)]


async def test_consume_partial_quantity_keeps_the_line(repo):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid, qty=2)
    await repo.consume_lines(user, lines=[(pid, 1)])
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].quantity == 1


async def test_consume_emptied_cart_drops_key(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    await repo.consume_lines(user, lines=[(pid, 1)])
    assert await repo.get_cart(user) is None
    assert not await client.exists(_cart_key(user))  # emptied by consume, like set-to-zero


async def test_consume_absent_lines_are_a_noop(repo):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await repo.consume_lines(user, lines=[(pid, 1)])  # no cart at all
    await _add(repo, user, pid)
    await repo.consume_lines(user, lines=[(uuid.uuid4(), 1)])  # cart exists, line doesn't
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].quantity == 1


async def test_consume_duplicate_pairs_are_additive(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid, qty=2)
    await repo.consume_lines(user, lines=[(pid, 1), (pid, 1)])
    assert await repo.get_cart(user) is None
    assert not await client.exists(_cart_key(user))


async def test_consume_skips_non_positive_quantities(repo):
    """Defense-in-depth guard: no caller can produce one (order lines are
    persisted positive), but a non-positive qty must never inflate a line."""
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    await repo.consume_lines(user, lines=[(pid, 0)])
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].quantity == 1


async def test_consumer_gates_tombstone_and_self_heal(repo, client):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    assert await repo.refresh_product(pid, name="w2", unit_price="12.50", product_version=None) == 1
    assert await repo.refresh_product(pid, name="w3", unit_price="13.00", product_version=2) == 1
    assert await repo.refresh_product(pid, name="stale", unit_price="0.01", product_version=2) == 0
    assert await repo.refresh_product(pid, name="legacy", unit_price="0.02", product_version=None) == 0
    assert await repo.prune_product(pid) == 1
    assert await repo.refresh_product(pid, name="ghost", unit_price="99.00", product_version=9) == 0
    assert await repo.get_cart(user) is None
    assert await repo.prune_product(pid) == 0
    assert await repo.refresh_product(pid, name="ghost", unit_price="99.00", product_version=9) == 0
    assert await client.exists(_product_key(pid))


async def test_projection_work_is_constant_per_event_and_lazy_per_cart(repo, client):
    pid = uuid.uuid4()
    users = [uuid.uuid4() for _ in range(120)]
    for user in users:
        await _add(repo, user, pid)
    assert await repo.refresh_product(pid, name="new", unit_price="12.50", product_version=4) == 1
    assert await client.get(_product_key(pid))
    assert await client.hget(_cart_key(users[-1]), str(pid)) is not None  # not visited by the event
    first = await repo.get_cart(users[0])
    assert first is not None and first.items[0].unit_price == "12.50"
    assert b'"unit_price":"9.99"' in await client.hget(_cart_key(users[-1]), str(pid))
    assert await repo.prune_product(pid) == 1
    assert await repo.get_cart(users[0]) is None
    assert await repo.get_cart(users[-1]) is None
    assert await client.ttl(_product_key(pid)) == -1  # tombstone outlives cart TTL


async def test_add_after_update_does_not_reapply_old_projection(repo):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await repo.refresh_product(pid, name="old", unit_price="2.00", product_version=3)
    cart = await _add(repo, user, pid, name="current", unit_price="4.00", product_version=4)
    assert cart.items[0].product_version == 4
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].unit_price == "4.00"


async def test_add_from_stale_catalog_cache_uses_newer_projection(repo):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await repo.refresh_product(pid, name="latest", unit_price="4.00", product_version=4)
    cart = await _add(repo, user, pid, name="old", unit_price="2.00", product_version=3)
    assert cart.items[0].product_version == 4
    assert cart.items[0].name == "latest" and cart.items[0].unit_price == "4.00"
    cart = await _add(repo, user, pid, name="old", unit_price="2.00", product_version=3)
    assert cart.items[0].name == "latest" and cart.items[0].unit_price == "4.00"


async def test_add_after_legacy_update_does_not_reapply_old_projection(repo):
    user, pid = uuid.uuid4(), uuid.uuid4()
    await repo.refresh_product(pid, name="old", unit_price="2.00", product_version=None)
    await _add(repo, user, pid, name="current", unit_price="4.00")
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].unit_price == "4.00"
    assert cart.items[0].product_version == 0
    await repo.refresh_product(pid, name="new", unit_price="5.00", product_version=1)
    cart = await repo.get_cart(user)
    assert cart is not None and cart.items[0].unit_price == "5.00"


async def test_tombstone_rejects_add_and_outlives_cart_ttl(repo, client):
    from src.cart.domain.cart import ProductTombstonedError

    user, pid = uuid.uuid4(), uuid.uuid4()
    await repo.prune_product(pid)
    with pytest.raises(ProductTombstonedError):
        await _add(repo, user, pid)
    assert await client.ttl(_product_key(pid)) == -1


async def test_tombstone_prunes_one_line_without_losing_survivor(repo, client):
    user, deleted, survivor = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, deleted)
    await _add(repo, user, survivor)
    await repo.prune_product(deleted)
    cart = await repo.get_cart(user)
    assert cart is not None and [line.product_id for line in cart.items] == [str(survivor)]
    assert await client.ttl(_cart_key(user)) > 0


async def test_tombstone_rejects_increment_before_next_read(repo):
    from src.cart.domain.cart import ProductTombstonedError

    user, pid = uuid.uuid4(), uuid.uuid4()
    await _add(repo, user, pid)
    await repo.prune_product(pid)
    with pytest.raises(ProductTombstonedError):
        await _add(repo, user, pid)
    assert await repo.get_cart(user) is None
