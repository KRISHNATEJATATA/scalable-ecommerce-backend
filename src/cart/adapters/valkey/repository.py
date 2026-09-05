"""Valkey cart repository — the concrete :class:`CartRepositoryPort`.

One hash per user (``cart:{user_id}``): field ``{product_id}`` holds the line
snapshot JSON, field ``$meta`` the ISO ``updated_at``. A secondary index,
``cart:by-product:{product_id}`` (a set of user ids), lets the product-event
consumer find every cart holding a product without scanning.

Every mutation is a single Lua script (operate + stamp ``$meta`` + refresh both
TTLs + return the whole hash), so two devices mutating one cart — or two worker
replicas applying one event — cannot interleave a read-modify-write. The
version gate in :data:`_APPLY_UPDATED_LUA` mirrors
:func:`~src.cart.domain.cart.should_apply_update` (keep them in sync): a stale
or duplicate ``ProductUpdated`` is a no-op, and a refresh never resurrects a
line ``ProductDeleted`` already pruned. Consumer scripts also ``SREM`` their
own index member when the line is absent, so an index entry outliving its cart
(expiry) self-heals instead of accumulating.

Cart lines sort by ``product_id`` on read — Valkey hashes are unordered, and a
stable order keeps responses (and tests) deterministic.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from valkey.exceptions import ResponseError

from src.cart.domain.cart import Cart, CartLine
from src.shared.errors.exceptions import InvalidCartOperationError

_META_FIELD = "$meta"
_CART_PREFIX = "cart:"
_INDEX_PREFIX = "cart:by-product:"


# Every mutation script names the meta field through a Lua local, so the field
# name still has exactly one Python source (``_META_FIELD``).
_META_DECL = "local META = '" + _META_FIELD + "'\n"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _cart_key(user_id: uuid.UUID | str) -> str:
    return f"{_CART_PREFIX}{user_id}"


def _index_key(product_id: uuid.UUID | str) -> str:
    return f"{_INDEX_PREFIX}{product_id}"


# Add-or-increment, atomically. A new line past max_items errors (CART_FULL);
# an increment past the per-line cap clamps (never rejects a well-formed add).
_ADD_LUA = (
    _META_DECL
    + """
local qty_add = tonumber(ARGV[6])
local cap = tonumber(ARGV[7])
local max_items = tonumber(ARGV[8])
local cur = redis.call('hget', KEYS[1], ARGV[1])
local qty = qty_add
-- An increment keeps the stored snapshot's ordering counter: rebuilding the
-- line versionless would let a stale/legacy event re-apply over fresher data
-- (see _APPLY_UPDATED_LUA, which treats a null version as versionless).
local ver = cjson.null
if cur then
  local old = cjson.decode(cur)
  qty = old.quantity + qty_add
  if qty > cap then qty = cap end
  if old.product_version ~= nil then ver = old.product_version end
else
  if qty > cap then qty = cap end
  local n = redis.call('hlen', KEYS[1])
  if redis.call('hexists', KEYS[1], META) == 1 then n = n - 1 end
  if n >= max_items then return redis.error_reply('CART_FULL') end
end
local line = {name = ARGV[2], unit_price = ARGV[3], quantity = qty, product_version = ver}
if ARGV[5] == '1' then line.image_url = ARGV[4] end
redis.call('hset', KEYS[1], ARGV[1], cjson.encode(line))
redis.call('hset', KEYS[1], META, ARGV[10])
redis.call('sadd', KEYS[2], ARGV[11])
redis.call('expire', KEYS[1], tonumber(ARGV[9]))
redis.call('expire', KEYS[2], tonumber(ARGV[9]))
return redis.call('hgetall', KEYS[1])
"""
)

# Set-quantity (0 removes), atomically. Absent line errors (LINE_ABSENT);
# over-cap errors (CART_QTY_RANGE) — the service pre-validates, this is the belt.
_SET_LUA = (
    _META_DECL
    + """
local qty = tonumber(ARGV[2])
local cur = redis.call('hget', KEYS[1], ARGV[1])
if not cur then return redis.error_reply('LINE_ABSENT') end
if qty == 0 then
  redis.call('hdel', KEYS[1], ARGV[1])
  redis.call('srem', KEYS[2], ARGV[6])
else
  if qty > tonumber(ARGV[3]) then return redis.error_reply('CART_QTY_RANGE') end
  local line = cjson.decode(cur)
  line.quantity = qty
  redis.call('hset', KEYS[1], ARGV[1], cjson.encode(line))
end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
  return {}
else
  redis.call('hset', KEYS[1], META, ARGV[5])
  redis.call('expire', KEYS[1], tonumber(ARGV[4]))
  redis.call('expire', KEYS[2], tonumber(ARGV[4]))
  return redis.call('hgetall', KEYS[1])
end
"""
)

# Idempotent remove: an absent line is a no-op that touches nothing (no meta
# stamp, no TTL refresh — it changed nothing).
_REMOVE_LUA = (
    _META_DECL
    + """
local removed = redis.call('hdel', KEYS[1], ARGV[1])
redis.call('srem', KEYS[2], ARGV[4])
if removed == 0 then return redis.call('hgetall', KEYS[1]) end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
  return {}
else
  redis.call('hset', KEYS[1], META, ARGV[3])
  redis.call('expire', KEYS[1], tonumber(ARGV[2]))
  redis.call('expire', KEYS[2], tonumber(ARGV[2]))
  return redis.call('hgetall', KEYS[1])
end
"""
)

# Consumer refresh (ProductUpdated), per user, atomically. Mirrors
# ``should_apply_update``: versioned events apply only when strictly newer,
# legacy version-less events only when the line carries no versioned knowledge.
# Absent line → SREM the stale index member, no resurrect.
_APPLY_UPDATED_LUA = (
    _META_DECL
    + """
local cur = redis.call('hget', KEYS[1], ARGV[1])
if not cur then
  redis.call('srem', KEYS[2], ARGV[5])
  return 0
end
local line = cjson.decode(cur)
local stored = line.product_version
local stored_is_null = (stored == nil or stored == cjson.null)
if ARGV[4] == '' then
  if not stored_is_null then return 0 end
else
  if (not stored_is_null) and tonumber(ARGV[4]) <= tonumber(stored) then return 0 end
  line.product_version = tonumber(ARGV[4])
end
line.name = ARGV[2]
line.unit_price = ARGV[3]
redis.call('hset', KEYS[1], ARGV[1], cjson.encode(line))
redis.call('hset', KEYS[1], META, ARGV[7])
redis.call('expire', KEYS[1], tonumber(ARGV[6]))
redis.call('expire', KEYS[2], tonumber(ARGV[6]))
return 1
"""
)

# Consumer prune (ProductDeleted tombstone), per user, atomically. Always wins.
_APPLY_DELETED_LUA = (
    _META_DECL
    + """
local removed = redis.call('hdel', KEYS[1], ARGV[1])
redis.call('srem', KEYS[2], ARGV[2])
if removed == 0 then return 0 end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
else
  redis.call('hset', KEYS[1], META, ARGV[4])
  redis.call('expire', KEYS[1], tonumber(ARGV[3]))
  redis.call('expire', KEYS[2], tonumber(ARGV[3]))
end
return 1
"""
)

# Clear: drop the cart and every product-index entry pointing at it.
_CLEAR_LUA = (
    _META_DECL
    + """
local fields = redis.call('hkeys', KEYS[1])
for _, f in ipairs(fields) do
  if f ~= META then
    redis.call('srem', ARGV[2] .. f, ARGV[1])
  end
end
redis.call('del', KEYS[1])
return 1
"""
)


class ValkeyCartRepository:
    """Implements :class:`src.cart.ports.repository.CartRepositoryPort` over Valkey."""

    def __init__(self, valkey: Any, *, ttl_seconds: int) -> None:
        self._valkey = valkey
        self._ttl = ttl_seconds

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes | bytearray):
            return bytes(value).decode("utf-8")
        return str(value)

    def _to_cart(self, user_id: uuid.UUID | str, raw: list[Any]) -> Cart | None:
        """Map a flat ``HGETALL`` reply to the domain cart (``None`` when empty)."""
        items: list[CartLine] = []
        updated_at: str | None = None
        for pos in range(0, len(raw) - 1, 2):
            name = self._decode(raw[pos])
            if name == _META_FIELD:
                updated_at = self._decode(raw[pos + 1])
                continue
            line = json.loads(self._decode(raw[pos + 1]))
            items.append(
                CartLine(
                    product_id=name,
                    name=line["name"],
                    unit_price=str(line["unit_price"]),
                    image_url=line.get("image_url"),
                    quantity=int(line["quantity"]),
                    product_version=line.get("product_version"),
                )
            )
        if not items:
            return None
        items.sort(key=lambda line: line.product_id)
        return Cart(user_id=str(user_id), items=tuple(items), updated_at=updated_at)

    async def get_cart(self, user_id: uuid.UUID) -> Cart | None:
        """Return the cart, refreshing its rolling TTL as activity.

        The product-index TTLs refresh too, for the lines present: a user who
        reads but never writes would otherwise keep the cart alive while its
        index entries expired — and events would silently stop reaching a live
        cart. A second pipeline (no atomicity needed on a read).
        """
        key = _cart_key(user_id)
        pipe = self._valkey.pipeline()
        pipe.hgetall(key)
        pipe.expire(key, self._ttl)
        raw, _ = await pipe.execute()
        if isinstance(raw, dict):  # redis-py returns a mapping, not a flat list
            raw = [item for pair in raw.items() for item in pair]
        cart = self._to_cart(user_id, list(raw or []))
        if cart is not None:
            touch = self._valkey.pipeline()
            for line in cart.items:
                touch.expire(_index_key(line.product_id), self._ttl)
            await touch.execute()
        return cart

    async def add_item(
        self,
        user_id: uuid.UUID,
        *,
        product_id: uuid.UUID,
        name: str,
        unit_price: str,
        image_url: str | None,
        quantity: int,
        max_items: int,
        max_per_line: int,
    ) -> Cart:
        """Add or increment the line (clamped), atomically; full cart → 400."""
        try:
            raw = await self._valkey.eval(
                _ADD_LUA,
                2,
                _cart_key(user_id),
                _index_key(product_id),
                str(product_id),
                name,
                unit_price,
                image_url or "",
                "1" if image_url else "0",
                quantity,
                max_per_line,
                max_items,
                self._ttl,
                _now_iso(),
                str(user_id),
            )
        except ResponseError as exc:
            if "CART_FULL" in str(exc):
                raise InvalidCartOperationError(f"cart holds the maximum of {max_items} lines") from exc
            raise
        cart = self._to_cart(user_id, list(raw))
        assert cart is not None  # add always leaves a line behind
        return cart

    async def set_quantity(
        self, user_id: uuid.UUID, *, product_id: uuid.UUID, quantity: int, max_per_line: int
    ) -> Cart | None:
        """Set the line quantity (``0`` removes); ``None`` only when the line was absent."""
        try:
            raw = await self._valkey.eval(
                _SET_LUA,
                2,
                _cart_key(user_id),
                _index_key(product_id),
                str(product_id),
                quantity,
                max_per_line,
                self._ttl,
                _now_iso(),
                str(user_id),
            )
        except ResponseError as exc:
            message = str(exc)
            if "LINE_ABSENT" in message:
                return None
            if "CART_QTY_RANGE" in message:
                raise InvalidCartOperationError(f"quantity {quantity} out of range (0..{max_per_line})") from exc
            raise
        cart = self._to_cart(user_id, list(raw))
        if cart is None:
            # The op landed but emptied the cart — success with an empty basket,
            # not absence (only LINE_ABSENT reports that).
            return Cart(user_id=str(user_id), items=(), updated_at=_now_iso())
        return cart

    async def remove_item(self, user_id: uuid.UUID, *, product_id: uuid.UUID) -> Cart | None:
        """Remove the line idempotently; ``None`` when the cart is now/then empty."""
        raw = await self._valkey.eval(
            _REMOVE_LUA,
            2,
            _cart_key(user_id),
            _index_key(product_id),
            str(product_id),
            self._ttl,
            _now_iso(),
            str(user_id),
        )
        return self._to_cart(user_id, list(raw))

    async def clear_cart(self, user_id: uuid.UUID) -> None:
        """Empty the whole cart and drop its product-index entries."""
        await self._valkey.eval(_CLEAR_LUA, 1, _cart_key(user_id), str(user_id), _INDEX_PREFIX)

    async def refresh_product(
        self, product_id: uuid.UUID, *, name: str, unit_price: str, product_version: int | None
    ) -> int:
        """Fan a ``ProductUpdated`` out to every cart holding the product."""
        members = await self._valkey.smembers(_index_key(product_id))
        touched = 0
        now = _now_iso()
        for member in members:
            user = self._decode(member)
            applied = await self._valkey.eval(
                _APPLY_UPDATED_LUA,
                2,
                _cart_key(user),
                _index_key(product_id),
                str(product_id),
                name,
                unit_price,
                "" if product_version is None else product_version,
                user,
                self._ttl,
                now,
            )
            touched += int(applied)
        return touched

    async def prune_product(self, product_id: uuid.UUID) -> int:
        """Fan a ``ProductDeleted`` tombstone out to every cart holding the product."""
        members = await self._valkey.smembers(_index_key(product_id))
        touched = 0
        now = _now_iso()
        for member in members:
            user = self._decode(member)
            applied = await self._valkey.eval(
                _APPLY_DELETED_LUA,
                2,
                _cart_key(user),
                _index_key(product_id),
                str(product_id),
                user,
                self._ttl,
                now,
            )
            touched += int(applied)
        return touched
