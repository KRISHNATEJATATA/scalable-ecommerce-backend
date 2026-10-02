"""Inventory ORM → domain mappers.

The repository maps every row it returns, so the application layer only ever sees
frozen snapshots, never a live ORM row. That matters here more than elsewhere: the
oversell guard and the reaper are guarded raw ``UPDATE``s that bypass the ORM unit
of work, so an identity-map copy of a stock/reservation row can be stale the moment
it is read — a snapshot taken at return time is the only honest answer.
"""

from __future__ import annotations

from src.inventory.adapters.db.models import Inventory as InventoryRow
from src.inventory.adapters.db.models import Reservation as ReservationRow
from src.inventory.domain.inventory import Inventory
from src.inventory.domain.reservation import Reservation, ReservationStatus


def to_domain(row: InventoryRow) -> Inventory:
    """Map an ORM ``inventory`` row to a domain ``Inventory`` snapshot."""
    return Inventory(sku=row.sku, on_hand=row.on_hand, reserved=row.reserved, version=row.version)


def reservation_to_domain(row: ReservationRow) -> Reservation:
    """Map an ORM ``reservations`` row to a domain ``Reservation`` snapshot."""
    return Reservation(
        id=row.id,
        sku=row.sku,
        qty=row.qty,
        order_id=row.order_id,
        status=ReservationStatus(row.status),
        expires_at=row.expires_at,
    )
