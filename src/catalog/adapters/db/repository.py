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
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import String, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.catalog.adapters.db.models import SCHEMA, Outbox, Product
from src.catalog.domain.image_status import ImageStatus
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

_SELECT_COLS = "id, merchant_id, name, description, category, price, image_key, image_status, created_at, updated_at"


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
    image_status: str
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

    # --- writes: state change + outbox row committed in ONE transaction -------

    async def create_product(
        self,
        *,
        product_id: uuid.UUID,
        merchant_id: uuid.UUID,
        name: str,
        description: str | None,
        category: str | None,
        price: Decimal,
        image_key: str | None,
        outbox: tuple[str, str],
    ) -> Product:
        """Insert a product and its ``ProductCreated`` outbox row atomically."""
        product = Product(
            id=product_id,
            merchant_id=merchant_id,
            name=name,
            description=description,
            category=category,
            price=price,
            image_key=image_key,
        )
        self._session.add(product)
        self._session.add(self._outbox_row(outbox))
        await self._session.commit()
        await self._session.refresh(product)
        return product

    async def update_product(self, product: Product, changes: dict[str, object], outbox: tuple[str, str]) -> Product:
        """Apply ``changes`` to an already-loaded product + emit its outbox row.

        The product is mutated through the ORM so ``version_id`` auto-bumps
        (optimistic lock): a concurrent edit that already advanced the version
        makes this commit raise ``StaleDataError`` instead of silently clobbering.
        """
        for field, value in changes.items():
            setattr(product, field, value)
        self._session.add(self._outbox_row(outbox))
        await self._session.commit()
        await self._session.refresh(product)
        return product

    async def soft_delete_product(self, product: Product, outbox: tuple[str, str]) -> None:
        """Soft-delete (``deleted_at``) + emit the ``ProductDeleted`` outbox row."""
        product.deleted_at = datetime.now(UTC)
        self._session.add(self._outbox_row(outbox))
        await self._session.commit()

    # --- image pipeline state (presign sets pending; the worker marks ready/failed) ---

    async def set_image_pending(self, product: Product, upload_token: str) -> None:
        """Mark an owned product's image as awaiting upload and record which upload.

        ``upload_token`` is the freshly-minted upload's token; storing it lets the
        worker reject a late event for a **superseded** upload (only the token that
        matches the current pending upload may flip the image state).
        """
        product.image_status = ImageStatus.PENDING.value
        product.image_upload_token = upload_token
        await self._session.commit()

    async def mark_image_ready(self, product_id: uuid.UUID, upload_token: str, image_key: str) -> bool:
        """Worker path: attach the processed key and flip to ``ready`` (idempotent).

        Raw UPDATE (not the ORM unit-of-work) because the worker owns its own
        session and re-processing the same object must be safe to repeat. Guarded
        on ``image_upload_token`` so a stale event for a superseded upload updates
        zero rows (returns ``False``) instead of clobbering newer image state.
        """
        result = await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products "
                "SET image_key = :key, image_status = :ready, updated_at = now() "
                "WHERE id = :id AND image_upload_token = :token AND deleted_at IS NULL"
            ),
            {"key": image_key, "id": product_id, "token": upload_token, "ready": ImageStatus.READY.value},
        )
        await self._session.commit()
        return result.rowcount > 0

    async def mark_image_failed(self, product_id: uuid.UUID, upload_token: str) -> bool:
        """Worker path: flip to ``failed`` when the upload doesn't pass sniff/re-encode.

        Token-guarded like :meth:`mark_image_ready` — a stale failure can't
        overwrite a newer pending/ready image.
        """
        result = await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products "
                "SET image_status = :failed, updated_at = now() "
                "WHERE id = :id AND image_upload_token = :token AND deleted_at IS NULL"
            ),
            {"id": product_id, "token": upload_token, "failed": ImageStatus.FAILED.value},
        )
        await self._session.commit()
        return result.rowcount > 0

    @staticmethod
    def _outbox_row(outbox: tuple[str, str]) -> Outbox:
        event_type, payload = outbox
        return Outbox(event_type=event_type, payload=payload)
