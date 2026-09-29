"""Valkey cart repository — the concrete :class:`CartRepositoryPort`.

One hash per user (``cart:{user_id}``): field ``{product_id}`` holds the line
snapshot JSON, field ``$meta`` the ISO ``updated_at``. Product events write
one durable ``cart:product:{product_id}`` projection; cart reads reconcile
only that caller's capped lines. Tombstones must outlive rolling cart TTLs.

Each mutation is a Lua script, so two devices mutating a cart cannot
interleave a read-modify-write. The lazy version gate mirrors
:func:`~src.cart.domain.cart.should_apply_update`; tombstones never
resurrect pruned lines. Legacy product-index sets expire naturally.

Cart lines sort by ``product_id`` on read — Valkey hashes are unordered, and a
stable order keeps responses (and tests) deterministic.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from valkey.exceptions import ResponseError

from src.cart.domain.cart import Cart, CartLine, ProductTombstonedError, should_apply_update
from src.shared.errors.exceptions import InvalidCartOperationError

_META_FIELD = "$meta"
_CART_PREFIX = "cart:"
_PRODUCT_PREFIX = "cart:product:"


# Every mutation script names the meta field through a Lua local, so the field
# name still has exactly one Python source (``_META_FIELD``).
_META_DECL = "local META = '" + _META_FIELD + "'\n"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _cart_key(user_id: uuid.UUID | str) -> str:
    return f"{_CART_PREFIX}{user_id}"


def _product_key(product_id: uuid.UUID | str) -> str:
    return f"{_PRODUCT_PREFIX}{product_id}"


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
local projection = redis.call('get', KEYS[2])
local state = projection and cjson.decode(projection)
if state and state.deleted then return redis.error_reply('PRODUCT_DELETED') end
local incoming = ARGV[11] ~= '' and tonumber(ARGV[11]) or nil
local name = ARGV[2]
local price = ARGV[3]
-- An increment keeps the stored snapshot's ordering counter: rebuilding the
-- line versionless would let a stale/legacy event re-apply over fresher data
-- (see _APPLY_UPDATED_LUA, which treats a null version as versionless).
local ver = cjson.null
if cur then
  local old = cjson.decode(cur)
  qty = old.quantity + qty_add
  if qty > cap then qty = cap end
  if old.product_version ~= nil then ver = old.product_version end
  if incoming then
    if ver ~= cjson.null and incoming < tonumber(ver) then
      name = old.name
      price = old.unit_price
    elseif ver == cjson.null or incoming > tonumber(ver) then
      ver = incoming
    end
    if state and state.version ~= cjson.null and
       incoming < tonumber(state.version) and
       (ver == cjson.null or tonumber(ver) < tonumber(state.version)) then
      ver = state.version
      name = state.name
      price = state.price
    end
  end
else
  if qty > cap then qty = cap end
  local n = redis.call('hlen', KEYS[1])
  if redis.call('hexists', KEYS[1], META) == 1 then n = n - 1 end
  if n >= max_items then return redis.error_reply('CART_FULL') end
  if state then
    if state.version ~= cjson.null then
      if incoming and incoming >= tonumber(state.version) then
        ver = incoming
      else
        ver = state.version
        if incoming then
          name = state.name
          price = state.price
        end
      end
    else
      ver = incoming or 0
    end
  elseif incoming then
    ver = incoming
  end
end
local line = {name = name, unit_price = price, quantity = qty, product_version = ver}
if ARGV[5] == '1' then line.image_url = ARGV[4] end
redis.call('hset', KEYS[1], ARGV[1], cjson.encode(line))
redis.call('hset', KEYS[1], META, ARGV[10])
redis.call('expire', KEYS[1], tonumber(ARGV[9]))
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
if removed == 0 then return redis.call('hgetall', KEYS[1]) end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
  return {}
else
  redis.call('hset', KEYS[1], META, ARGV[3])
  redis.call('expire', KEYS[1], tonumber(ARGV[2]))
  return redis.call('hgetall', KEYS[1])
end
"""
)

# Lazy refresh (ProductUpdated), per user, atomically. Mirrors
# ``should_apply_update``: versioned events apply only when strictly newer,
# legacy version-less events only when the line carries no versioned knowledge.
# Absent lines are a no-op, never resurrected.
_APPLY_UPDATED_LUA = (
    _META_DECL
    + """
local cur = redis.call('hget', KEYS[1], ARGV[1])
if not cur then
  return 0
end
local line = cjson.decode(cur)
local stored = line.product_version
local stored_is_null = (stored == nil or stored == cjson.null)
if ARGV[4] == '' then
  if not stored_is_null then return 0 end
  if line.name == ARGV[2] and line.unit_price == ARGV[3] then return 0 end
else
  if (not stored_is_null) and tonumber(ARGV[4]) <= tonumber(stored) then return 0 end
  line.product_version = tonumber(ARGV[4])
end
line.name = ARGV[2]
line.unit_price = ARGV[3]
redis.call('hset', KEYS[1], ARGV[1], cjson.encode(line))
redis.call('hset', KEYS[1], META, ARGV[6])
redis.call('expire', KEYS[1], tonumber(ARGV[5]))
return 1
"""
)

# Lazy prune (ProductDeleted tombstone), per user, atomically. Always wins.
_APPLY_DELETED_LUA = (
    _META_DECL
    + """
local removed = redis.call('hdel', KEYS[1], ARGV[1])
if removed == 0 then return 0 end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
else
  redis.call('hset', KEYS[1], META, ARGV[3])
  redis.call('expire', KEYS[1], tonumber(ARGV[2]))
end
return 1
"""
)


_PROJECT_LUA = """
local old = redis.call('get', KEYS[1])
if old then
  local state = cjson.decode(old)
  if state.deleted or (ARGV[1] == '' and state.version ~= cjson.null) then return 0 end
  if ARGV[1] ~= '' and state.version ~= cjson.null and
     tonumber(ARGV[1]) <= tonumber(state.version) then return 0 end
end
redis.call('set', KEYS[1], ARGV[2])
return 1
"""

_TOMBSTONE_LUA = """
local old = redis.call('get', KEYS[1])
if old and cjson.decode(old).deleted then return 0 end
redis.call('set', KEYS[1], '{"deleted":true}')
return 1
"""

# Clear the cart in one operation.
_CLEAR_LUA = "return redis.call('del', KEYS[1])"

# Checkout consumption: subtract exactly the purchased quantities, atomically.
# A zeroed line is dropped; a cart left holding only meta is deleted — but
# lines added while checkout ran survive.
# Non-positive purchased quantities are skipped: no caller can produce one
# (order lines are persisted positive), the guard is defense-in-depth.
# ARGV: 1=user, 2=ttl, 3=now, then alternating
# product_id/quantity pairs.
_CONSUME_LUA = (
    _META_DECL
    + """
local i = 4
while ARGV[i] do
  local qty = tonumber(ARGV[i + 1])
  if qty and qty > 0 then
    local cur = redis.call('hget', KEYS[1], ARGV[i])
    if cur then
      local line = cjson.decode(cur)
      line.quantity = line.quantity - qty
      if line.quantity <= 0 then
        redis.call('hdel', KEYS[1], ARGV[i])
      else
        redis.call('hset', KEYS[1], ARGV[i], cjson.encode(line))
      end
    end
  end
  i = i + 2
end
if redis.call('hlen', KEYS[1]) <= 1 then
  redis.call('del', KEYS[1])
  return 0
end
redis.call('hset', KEYS[1], META, ARGV[3])
redis.call('expire', KEYS[1], tonumber(ARGV[2]))
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

        A product event only writes a single projection key. Reconcile this
        user's lines on demand, with work bounded by the cart line cap.
        """
        key = _cart_key(user_id)
        pipe = self._valkey.pipeline()
        pipe.hgetall(key)
        pipe.expire(key, self._ttl)
        raw, _ = await pipe.execute()
        if isinstance(raw, dict):  # redis-py returns a mapping, not a flat list
            raw = [item for pair in raw.items() for item in pair]
        cart = self._to_cart(user_id, list(raw or []))
        if cart is None:
            return None
        states = await self._valkey.mget([_product_key(line.product_id) for line in cart.items])
        changed = False
        now = _now_iso()
        for line, state in zip(cart.items, states, strict=True):
            if state is None:
                continue
            projection = json.loads(self._decode(state))
            if projection["deleted"]:
                applied = await self._valkey.eval(
                    _APPLY_DELETED_LUA,
                    1,
                    key,
                    line.product_id,
                    self._ttl,
                    now,
                )
            else:
                if not should_apply_update(line.product_version, projection["version"]):
                    continue
                if (
                    projection["version"] is None
                    and line.name == projection["name"]
                    and line.unit_price == projection["price"]
                ):
                    continue
                applied = await self._valkey.eval(
                    _APPLY_UPDATED_LUA,
                    1,
                    key,
                    line.product_id,
                    projection["name"],
                    projection["price"],
                    "" if projection["version"] is None else projection["version"],
                    self._ttl,
                    now,
                )
            changed |= bool(applied)
        if changed:
            raw = await self._valkey.hgetall(key)
            if isinstance(raw, dict):
                raw = [item for pair in raw.items() for item in pair]
            return self._to_cart(user_id, list(raw or []))
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
        product_version: int | None = None,
    ) -> Cart:
        """Add or increment the line (clamped), atomically; full cart → 400."""
        try:
            raw = await self._valkey.eval(
                _ADD_LUA,
                2,
                _cart_key(user_id),
                _product_key(product_id),
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
                "" if product_version is None else product_version,
            )
        except ResponseError as exc:
            if "CART_FULL" in str(exc):
                raise InvalidCartOperationError(f"cart holds the maximum of {max_items} lines") from exc
            if "PRODUCT_DELETED" in str(exc):
                raise ProductTombstonedError from exc
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
                1,
                _cart_key(user_id),
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
            1,
            _cart_key(user_id),
            str(product_id),
            self._ttl,
            _now_iso(),
            str(user_id),
        )
        return self._to_cart(user_id, list(raw))

    async def clear_cart(self, user_id: uuid.UUID) -> None:
        """Empty the whole cart."""
        await self._valkey.eval(_CLEAR_LUA, 1, _cart_key(user_id))

    async def consume_lines(self, user_id: uuid.UUID, *, lines: list[tuple[uuid.UUID, int]]) -> None:
        """Subtract the purchased quantities, atomically; a zeroed line (and an
        emptied cart) is removed, while lines added after the snapshot survive."""
        args: list[Any] = [str(user_id), self._ttl, _now_iso()]
        for product_id, quantity in lines:
            args.extend([str(product_id), quantity])
        await self._valkey.eval(_CONSUME_LUA, 1, _cart_key(user_id), *args)

    async def refresh_product(
        self, product_id: uuid.UUID, *, name: str, unit_price: str, product_version: int | None
    ) -> int:
        """Record the latest event once; cart reads apply it on demand."""
        return int(
            await self._valkey.eval(
                _PROJECT_LUA,
                1,
                _product_key(product_id),
                "" if product_version is None else product_version,
                json.dumps({"deleted": False, "version": product_version, "name": name, "price": unit_price}),
            )
        )

    async def prune_product(self, product_id: uuid.UUID) -> int:
        """Record a permanent tombstone; cart reads prune their own lines."""
        return int(await self._valkey.eval(_TOMBSTONE_LUA, 1, _product_key(product_id)))
