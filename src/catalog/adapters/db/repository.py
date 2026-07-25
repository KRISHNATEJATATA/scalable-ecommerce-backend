"""Catalog read repository — the hottest read, so it drops to raw SQL.

``list_products`` hand-writes its keyset ``WHERE``/``ORDER BY`` over raw
``text()`` SQL and maps Core rows into the lightweight :class:`ProductRow`
read model (skips ORM hydration). It still reuses the shared cursor codec and
:func:`build_page`. ``get_product`` is a plain soft-delete-filtered ORM fetch.

ports/repos return ORM models / this read-model dataclass, not
hand-mapped domain entities — those would be anemic pass-throughs today. Add a
domain layer when real catalog behavior arrives. Services (ticket 03) map these
to Pydantic response schemas.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import String, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.catalog.adapters.db.models import SCHEMA, Product
from src.shared.db.pagination import Page, PageParams, build_page, check_filters, decode_cursor
from src.shared.errors.exceptions import InvalidQueryParamError

# Whitelist: sort field -> (column, Postgres cast type for the cursor value).
# The authoritative injection gate — only these names ever reach the SQL string.
_SORT_COLUMNS: dict[str, tuple[str, str]] = {
    "created_at": ("created_at", "timestamptz"),
    "price": ("price", "numeric"),
    "name": ("name", "text"),
}
_FILTERS: frozenset[str] = frozenset({"category", "merchant_id"})

_SELECT_COLS = "id, merchant_id, name, description, category, price, image_key, created_at, updated_at"


@dataclass(slots=True)
class ProductRow:
    """Lightweight read model for the raw catalog list (rows are not hydrated ORM)."""

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    created_at: datetime
    updated_at: datetime


class CatalogRepository:
    """Implements :class:`src.catalog.ports.repository.CatalogRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_products(self, params: PageParams, filters: dict[str, object] | None = None) -> Page[ProductRow]:
        filters = filters or {}
        check_filters(filters, _FILTERS)
        if params.sort_field not in _SORT_COLUMNS:
            raise InvalidQueryParamError("sort", params.sort_field)
        column, cast_type = _SORT_COLUMNS[params.sort_field]
        direction = "DESC" if params.descending else "ASC"
        op = "<" if params.descending else ">"

        where = ["deleted_at IS NULL"]
        binds: dict[str, object] = {"limit": params.limit + 1}
        for key, value in filters.items():
            where.append(f"{key} = :{key}")  # key is whitelist-validated above
            binds[key] = value
        if params.cursor:
            cursor_sort, cursor_id = decode_cursor(params.cursor)
            where.append(f"({column}, id) {op} (CAST(:cursor_sort AS {cast_type}), CAST(:cursor_id AS uuid))")
            binds["cursor_sort"] = cursor_sort
            binds["cursor_id"] = cursor_id

        sql = text(
            f"SELECT {_SELECT_COLS} FROM {SCHEMA}.products "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY {column} {direction}, id {direction} LIMIT :limit"
        )
        if params.cursor:
            # Type the cursor params as text so asyncpg sends them as text and
            # the SQL CAST does the conversion — otherwise asyncpg infers the
            # CAST target type and rejects the string value.
            sql = sql.bindparams(
                bindparam("cursor_sort", type_=String()),
                bindparam("cursor_id", type_=String()),
            )
        result = await self._session.execute(sql, binds)
        rows = [ProductRow(**mapping) for mapping in result.mappings().all()]
        return build_page(rows, params, key_of=lambda row: (getattr(row, params.sort_field), row.id))

    async def get_product(self, product_id: uuid.UUID) -> Product | None:
        stmt = select(Product).where(Product.id == product_id, Product.deleted_at.is_(None))
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
