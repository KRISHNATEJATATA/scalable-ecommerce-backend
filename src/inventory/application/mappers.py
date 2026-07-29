"""Inventory mappers: ORM rows → domain entities.

Typed ``Any`` to avoid an application → adapters import (rows are duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.inventory.domain.inventory import Inventory
from src.inventory.domain.reservation import Reservation, ReservationStatus


def to_domain(row: Any) -> Inventory:
    """Map an ORM ``Inventory`` row to a domain ``Inventory``."""
    return Inventory(sku=row.sku, on_hand=row.on_hand, reserved=row.reserved, version=row.version)


def reservation_to_domain(row: Any) -> Reservation:
    """Map an ORM ``Reservation`` row to a domain ``Reservation``."""
    return Reservation(
        id=row.id,
        sku=row.sku,
        qty=row.qty,
        order_id=row.order_id,
        status=ReservationStatus(row.status),
        expires_at=row.expires_at,
    )
