"""Inventory domain entity — pure Python, imports nothing outward.

Frozen slotted dataclass mirroring the ``inventory.inventory`` row (``version``
is the manual-CAS optimistic-lock column).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Inventory:
    """The inventory stock row (immutable); ``version`` is the optimistic lock."""

    sku: str
    on_hand: int
    reserved: int
    version: int
