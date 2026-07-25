"""Inventory repository — the oversell guard drops to a raw atomic CAS.

``try_reserve_decrement`` is the single-statement conditional decrement that
prevents overselling: it only bumps ``reserved`` when free stock covers the
qty, so a losing concurrent racer matches 0 rows and is rejected. Returns the
affected rowcount (1 = reserved, 0 = rejected). Ticket 10 wraps this with the
reservation-row INSERT + expiry + events inside one transaction.

returns the ORM ``Inventory`` row / a rowcount int, not a domain
entity — added when inventory domain behavior arrives.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.inventory.adapters.db.models import SCHEMA, Inventory

_DECREMENT_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608 — SCHEMA is a fixed module constant, not user input
    "SET reserved = reserved + :qty, version = version + 1 "
    "WHERE sku = :sku AND on_hand - reserved >= :qty"
)


class InventoryRepository:
    """Implements :class:`src.inventory.ports.repository.InventoryRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_sku(self, sku: str) -> Inventory | None:
        stmt = select(Inventory).where(Inventory.sku == sku)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def try_reserve_decrement(self, sku: str, qty: int) -> int:
        result = await self._session.execute(_DECREMENT_SQL, {"sku": sku, "qty": qty})
        return result.rowcount
