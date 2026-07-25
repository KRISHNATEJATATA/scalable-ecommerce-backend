"""Inventory mapper: ORM ``Inventory`` row → domain ``Inventory``.

Typed ``Any`` to avoid an application → adapters import (the row is duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.inventory.domain.inventory import Inventory


def to_domain(row: Any) -> Inventory:
    """Map an ORM ``Inventory`` row to a domain ``Inventory``."""
    return Inventory(sku=row.sku, on_hand=row.on_hand, reserved=row.reserved, version=row.version)
