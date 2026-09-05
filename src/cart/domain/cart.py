"""Cart domain — pure Python, imports nothing outward.

A cart is pre-checkout, ephemeral state: it lives in Valkey, never Postgres
(see ``src/cart/CONTEXT.md``). These types are the shared vocabulary between
the application service, the Valkey adapter (whose Lua scripts mirror
:func:`should_apply_update`), and the product-event consumer.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CartLine:
    """One product in a cart — a snapshot, not a reference.

    ``name``/``unit_price``/``image_url`` are copied from the catalog at add
    time and refreshed by ``ProductUpdated``; ``product_version`` is the
    catalog aggregate's ``version_id`` the snapshot was last aligned to
    (``None`` for lines added before any versioned event was applied).
    """

    product_id: str
    name: str
    unit_price: str  # decimal-as-string, the wire shape
    image_url: str | None
    quantity: int
    product_version: int | None


@dataclass(frozen=True, slots=True)
class Cart:
    """A user's pre-checkout basket."""

    user_id: str
    items: tuple[CartLine, ...]
    updated_at: str | None  # ISO-8601, stamped on every mutation


def should_apply_update(stored_version: int | None, incoming_version: int | None) -> bool:
    """Whether a ``ProductUpdated`` payload supersedes a cart line's snapshot.

    The ordering gate both the consumer and the Valkey adapter enforce (the
    Lua mirrors this predicate — keep them in sync): a versioned event applies
    only when strictly newer than what the line already reflects, and a legacy
    version-less (v1) event applies only when the line carries no versioned
    knowledge at all. Anything else is a stale or duplicate delivery and must
    not overwrite fresher data. Deletion is not gated here — a tombstone always
    wins (see ``apply`` on the consumer path).
    """
    if incoming_version is None:
        return stored_version is None
    if stored_version is None:
        return True
    return incoming_version > stored_version
