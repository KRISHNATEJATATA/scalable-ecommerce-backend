"""Catalog read repository — the hottest read, so it drops to raw SQL.

``list_products`` hand-writes its keyset ``WHERE``/``ORDER BY`` over raw
``text()`` SQL and maps Core rows into the lightweight :class:`ProductRow`
read model (skips ORM hydration). It still reuses the shared cursor codec and
:func:`build_page`. ``get_product`` is a plain soft-delete-filtered ORM fetch.

ports/repos return ORM models / this read-model dataclass, not
hand-mapped domain entities — those would be anemic pass-throughs today. Add a
domain layer when real catalog behavior arrives. Services map these
to Pydantic response schemas.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Result, String, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql import text

from src.catalog.adapters.db.models import SCHEMA, ImageReclaim, Outbox, Product
from src.catalog.domain.image_status import ImageStatus
from src.catalog.ports.repository import ImageFlip, ImageOutboxFactory, ImageReclaimTask, PendingUpload
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import Page, PageParams, build_page, check_filters, decode_cursor
from src.shared.errors.exceptions import ConcurrentUpdateError, InvalidQueryParamError

# Whitelist: sort field -> (column, Postgres cast type for the cursor value).
# The authoritative injection gate — only these names ever reach the SQL string.
_SORT_COLUMNS: dict[str, tuple[str, str]] = {
    "created_at": ("created_at", "timestamptz"),
    "price": ("price", "numeric"),
    "name": ("name", "text"),
}
_FILTERS: frozenset[str] = frozenset({"category", "merchant_id"})

_SELECT_COLS = "id, merchant_id, name, description, category, price, image_key, image_status, created_at, updated_at"

# Fields the image-flip UPDATEs return so the ``ProductUpdated`` payload is built
# from post-update state inside the same transaction (no read-then-publish race).
# ``version_id`` is the post-increment value, so the event carries the same
# monotonic counter an ORM-mediated edit publishes.
_EVENT_COLS = "id AS product_id, merchant_id, name, price, category, version_id AS product_version"
# Same columns, qualified: the ready-flip joins the pre-update row (see
# :meth:`CatalogRepository.mark_image_ready`), which makes bare ``id`` ambiguous.
_EVENT_COLS_Q = "p.id AS product_id, p.merchant_id, p.name, p.price, p.category, p.version_id AS product_version"


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
            cursor_sort, cursor_id = decode_cursor(params.cursor, cast_type)
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

    async def _commit_versioned(self) -> None:
        """Commit an ORM write on the versioned ``Product`` aggregate.

        ``version_id_col`` turns a lost update into ``StaleDataError`` at flush.
        Translate it at the adapter boundary (and roll back the now-unusable
        transaction) so the API answers a retryable **409** instead of letting a
        SQLAlchemy exception reach the 500 handler.
        """
        try:
            await self._session.commit()
        except StaleDataError as exc:
            await self._session.rollback()
            raise ConcurrentUpdateError("product") from exc

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
        outbox: OutboxMessage,
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

    async def update_product(self, product: Product, changes: dict[str, object], outbox: OutboxMessage) -> Product:
        """Apply ``changes`` to an already-loaded product + emit its outbox row.

        The product is mutated through the ORM so ``version_id`` auto-bumps
        (optimistic lock): a concurrent edit that already advanced the version
        makes this commit raise ``StaleDataError`` instead of silently clobbering,
        which :meth:`_commit_versioned` turns into a retryable 409.
        """
        for field, value in changes.items():
            setattr(product, field, value)
        self._session.add(self._outbox_row(outbox))
        await self._commit_versioned()
        await self._session.refresh(product)
        return product

    async def soft_delete_product(self, product: Product, outbox: OutboxMessage) -> None:
        """Soft-delete (``deleted_at``) + emit the ``ProductDeleted`` outbox row."""
        product.deleted_at = datetime.now(UTC)
        self._session.add(self._outbox_row(outbox))
        await self._commit_versioned()

    # --- image pipeline state (presign sets pending; the worker marks ready/failed) ---

    async def set_image_pending(
        self,
        product: Product,
        upload_token: str,
        *,
        expires_at: datetime,
        outbox: OutboxMessage | None = None,
    ) -> None:
        """Mark an owned product's image as awaiting upload and record which upload.

        ``upload_token`` is the freshly-minted upload's token; storing it lets the
        worker reject a late event for a **superseded** upload (only the token that
        matches the current pending upload may flip the image state).

        ``expires_at`` is when the presigned POST stops being accepted. Without it a
        client that never uploads would strand the product in ``pending`` — and
        ``image_url`` null — forever; it is what lets
        :meth:`expire_abandoned_uploads` put the row back the way it was.

        Flipping ``ready`` → ``pending`` changes the product's public image state
        (``image_url`` drops), so a supplied ``outbox`` (``ProductUpdated``) row is
        written in the **same transaction** to invalidate the read-cache — otherwise
        a cached ready-image response would linger stale after a re-upload starts.
        """
        product.image_status = ImageStatus.PENDING.value
        product.image_upload_token = upload_token
        product.image_upload_expires_at = expires_at
        if outbox is not None:
            self._session.add(self._outbox_row(outbox))
        await self._commit_versioned()

    async def mark_image_ready(
        self, product_id: uuid.UUID, upload_token: str, image_key: str, outbox: ImageOutboxFactory | None = None
    ) -> ImageFlip:
        """Worker path: attach the processed key and flip to ``ready`` (idempotent).

        Returns an :class:`~src.catalog.ports.repository.ImageFlip` — whether the
        guarded UPDATE landed, plus the ``image_key`` it *replaced* (read in the same
        statement, ``FOR UPDATE``-locked, so it can't be a torn read). That key's now
        unreferenced renditions are queued for deletion in ``catalog.image_reclaim``
        **in this same transaction**, so an S3 outage or a crash can only leave the
        cleanup pending, never lose it: ``public/`` is live CDN content and sits
        outside the ``uploads/`` lifecycle rule, so nothing else ever would.

        Raw UPDATE (not the ORM unit-of-work) because the worker owns its own
        session and re-processing the same object must be safe to repeat. Guarded
        on ``image_upload_token`` so a stale event for a superseded upload updates
        zero rows (``applied=False``) instead of clobbering newer image state.

        When the flip actually lands and ``outbox`` is supplied, the factory is
        called with the UPDATE's ``RETURNING`` row and its (``ProductUpdated``) row
        is written in the **same transaction** — so the payload carries the
        post-update state (never a value read before the write, which a concurrent
        merchant edit could have already replaced) and the image becoming ready
        invalidates the read-cache through the normal outbox → relay →
        ``catalog-cache`` path.

        Guarded on ``image_status = 'pending'`` as well as the token, so a
        **redelivery** of the same event (the row is already ``ready``) updates zero
        rows and does not emit a *duplicate* ``ProductUpdated`` outbox row.

        Bumps ``version_id`` by hand: this write bypasses the ORM unit-of-work, so
        nothing else would advance the aggregate's counter, and two flips (or a flip
        and a merchant edit) would publish events sharing a version — leaving a
        downstream projector unable to order them.
        """
        result = await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products p "
                "SET image_key = :key, image_status = :ready, image_upload_expires_at = NULL, "
                "version_id = p.version_id + 1, updated_at = now() "
                f"FROM (SELECT id, image_key AS previous_key FROM {SCHEMA}.products "
                "WHERE id = :id FOR UPDATE) prev "
                "WHERE p.id = prev.id AND p.image_upload_token = :token "
                "AND p.image_status = :pending AND p.deleted_at IS NULL "
                f"RETURNING {_EVENT_COLS_Q}, prev.previous_key"
            ),
            {
                "key": image_key,
                "id": product_id,
                "token": upload_token,
                "ready": ImageStatus.READY.value,
                "pending": ImageStatus.PENDING.value,
            },
        )
        row = result.mappings().first()
        previous_key = row["previous_key"] if row is not None else None
        if row is not None:
            if outbox is not None:
                self._session.add(self._outbox_row(outbox(row)))
            if previous_key and previous_key != image_key:
                # Same transaction as the flip that orphaned it: after this commits,
                # "the product no longer references that image" and "those objects
                # are scheduled for deletion" are one atomic fact. A crash can only
                # leave work to do, never silently drop it (outbox pattern, for S3).
                self._session.add(ImageReclaim(product_id=product_id, object_key=previous_key))
        await self._session.commit()
        return ImageFlip(row is not None, previous_key)

    async def mark_image_failed(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool:
        """Worker path: flip to ``failed`` when the upload doesn't pass sniff/re-encode.

        Token- and ``pending``-guarded like :meth:`mark_image_ready` — a stale
        failure can't overwrite a newer pending/ready image, and a redelivery of an
        already-``failed`` row updates zero rows so no duplicate outbox row is
        emitted. The ``outbox`` factory is fed the same transaction's ``RETURNING``
        row, so the status change invalidates the read-cache with post-update state.
        ``version_id`` is bumped here too (see :meth:`mark_image_ready`).
        """
        result = await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products "
                "SET image_status = :failed, image_upload_expires_at = NULL, "
                "version_id = version_id + 1, updated_at = now() "
                "WHERE id = :id AND image_upload_token = :token "
                "AND image_status = :pending AND deleted_at IS NULL "
                f"RETURNING {_EVENT_COLS}"
            ),
            {
                "id": product_id,
                "token": upload_token,
                "failed": ImageStatus.FAILED.value,
                "pending": ImageStatus.PENDING.value,
            },
        )
        return await self._commit_image_flip(result, outbox) is not None

    async def due_pending_uploads(self, *, grace_seconds: int, batch_size: int) -> list[PendingUpload]:
        """Products whose presigned upload passed its deadline (plus grace).

        Only *candidates*: whether the bytes actually arrived is a question for S3,
        not the DB, so the caller probes the raw object before anything is reset (see
        :meth:`expire_abandoned_upload`). Deliberately a plain read holding no locks
        — the probe is a network call, and pinning rows across it would serialise
        every worker on the slowest HEAD.

        ``grace_seconds`` is added to the presign deadline so an upload that landed
        just before expiry — and is still queued or mid-processing — isn't considered
        at all; it must exceed the queue's visibility timeout (enforced in settings).
        """
        rows = (
            await self._session.execute(
                text(
                    f"SELECT id, image_upload_token FROM {SCHEMA}.products WHERE image_status = :pending "
                    "AND image_upload_token IS NOT NULL "
                    "AND image_upload_expires_at < now() - make_interval(secs => :grace) "
                    "AND deleted_at IS NULL ORDER BY image_upload_expires_at LIMIT :batch"
                ),
                {"pending": ImageStatus.PENDING.value, "grace": grace_seconds, "batch": batch_size},
            )
        ).all()
        return [PendingUpload(r.id, r.image_upload_token) for r in rows]

    async def expire_abandoned_upload(
        self, product_id: uuid.UUID, upload_token: str, outbox: ImageOutboxFactory | None = None
    ) -> bool:
        """Restore one product whose presigned upload expired with nothing uploaded.

        Presigning flips the row to ``pending`` immediately, which drops
        ``image_url`` from every response. A client that never uploads (closed tab,
        crash, lost network) therefore leaves the product — **and whatever image it
        was already serving** — unavailable forever, since only an upload event ever
        moves it out of ``pending``. This is the backstop, the same shape as the
        inventory reservation reaper: a TTL plus a sweep, not a hope.

        The row goes back to what it was: ``ready`` if a processed ``image_key``
        survived the re-upload attempt, else ``none``. The token is cleared, so a
        very late event for that upload can no longer flip anything (its renditions,
        if it wrote any, are reclaimed by the ingest's stale path).

        Guarded on that same token (compare-and-set) because the candidate was read
        without a lock: if the upload landed, or the merchant re-presigned, in the
        meantime, this updates zero rows and reports ``False`` rather than undoing
        newer state. ``version_id`` is bumped and a ``ProductUpdated`` outbox row is
        written in the same txn — the image state changed, so the read-cache must be
        invalidated.
        """
        result = await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products SET image_status = CASE WHEN image_key IS NULL THEN :none ELSE :ready END, "
                "image_upload_token = NULL, image_upload_expires_at = NULL, "
                "version_id = version_id + 1, updated_at = now() "
                "WHERE id = :id AND image_upload_token = :token AND image_status = :pending "
                f"AND deleted_at IS NULL RETURNING {_EVENT_COLS}"
            ),
            {
                "id": product_id,
                "token": upload_token,
                "none": ImageStatus.NONE.value,
                "ready": ImageStatus.READY.value,
                "pending": ImageStatus.PENDING.value,
            },
        )
        return await self._commit_image_flip(result, outbox) is not None

    async def defer_upload_expiry(self, product_id: uuid.UUID, upload_token: str, *, delay_seconds: int) -> None:
        """Push an expired-but-*uploaded* product's deadline out, keeping it ``pending``.

        The raw object exists, so the bytes arrived and only the event is late (queue
        backlog, redrive, a DLQ replay still to come). Reaping would clear the token
        that event's flip is guarded on and silently discard a valid image, so the
        row is left alone — and re-checked later rather than HEAD-probed on every
        poll. Once the raw upload is lifecycle-expired the probe finally answers
        "gone" and the product is restored normally.
        """
        await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.products SET image_upload_expires_at = now() + make_interval(secs => :delay) "
                "WHERE id = :id AND image_upload_token = :token AND image_status = :pending"
            ),
            {"id": product_id, "token": upload_token, "delay": delay_seconds, "pending": ImageStatus.PENDING.value},
        )
        await self._session.commit()
        await self._session.commit()

    async def current_image_key(self, product_id: uuid.UUID) -> str | None:
        """The **live** product's current ``image_key`` (``None`` if absent/deleted).

        Read by the image worker only on a **stale** flip, to tell "these
        just-written renditions are garbage" apart from "this is a redelivery of the
        object the product actually serves". Compared by *key*, not by upload token:
        one token can produce several public keys (a presigned POST is replayable
        with different bytes), so only the key the DB points at is live.

        Scoped to ``deleted_at IS NULL`` on purpose, matching the guard on
        ``mark_image_ready``: a soft-deleted product can never serve those objects,
        so returning its key would mislabel the renditions as live and leak them
        into ``public/`` (which the ``uploads/`` lifecycle rule does not cover).
        """
        result = await self._session.execute(
            text(f"SELECT image_key FROM {SCHEMA}.products WHERE id = :id AND deleted_at IS NULL"),
            {"id": product_id},
        )
        return result.scalar_one_or_none()

    async def schedule_image_reclaim(self, product_id: uuid.UUID, object_key: str) -> None:
        """Durably queue a public main key's renditions for deletion.

        Used for the keys a *stale* flip wrote: there is no state change to attach
        them to (the UPDATE deliberately did nothing), so the intent is its own
        committed row. ``ON CONFLICT DO NOTHING`` — the unique key makes scheduling
        idempotent under redelivery.
        """
        await self._session.execute(
            text(
                f"INSERT INTO {SCHEMA}.image_reclaim (product_id, object_key) "
                "VALUES (:pid, :key) ON CONFLICT (object_key) DO NOTHING"
            ),
            {"pid": product_id, "key": object_key},
        )
        await self._session.commit()

    async def claim_image_reclaims(self, *, batch_size: int, lease_seconds: int = 300) -> list[ImageReclaimTask]:
        """Lease up to ``batch_size`` due reclaim rows for this worker.

        ``FOR UPDATE SKIP LOCKED`` like the reservation reaper, so N image workers
        split the queue instead of racing to delete the same objects — but the claim
        also **pushes ``next_attempt_at`` forward and commits**, so the lease outlives
        the transaction. Holding the locks across the sweep instead would mean the
        first per-row commit silently released every other row in the batch.

        A crash mid-sweep therefore costs one lease interval, after which the row is
        re-claimed and retried; the deletes themselves are idempotent, so a repeat is
        harmless. ``attempts`` is bumped on claim, which doubles as a poison counter.
        """
        rows = (
            await self._session.execute(
                text(
                    f"UPDATE {SCHEMA}.image_reclaim r SET attempts = r.attempts + 1, "
                    "next_attempt_at = now() + make_interval(secs => :lease) "
                    f"FROM (SELECT id FROM {SCHEMA}.image_reclaim WHERE next_attempt_at <= now() "
                    "ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT :batch) due "
                    "WHERE r.id = due.id RETURNING r.id, r.product_id, r.object_key, r.attempts"
                ),
                {"batch": batch_size, "lease": lease_seconds},
            )
        ).all()
        await self._session.commit()
        return [ImageReclaimTask(r.id, r.product_id, r.object_key, r.attempts) for r in rows]

    async def finish_image_reclaim(self, ids: list[int]) -> None:
        """Drop reclaim rows whose objects are gone — the work is done."""
        await self._session.execute(
            text(f"DELETE FROM {SCHEMA}.image_reclaim WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
        await self._session.commit()

    async def defer_image_reclaim(self, task_id: int, *, delay_seconds: int, error: str) -> None:
        """Record why one leased reclaim failed and set its retry time.

        The claim already leased the row, so this only *shortens* the wait to the
        configured backoff and keeps the reason for the runbook.
        """
        await self._session.execute(
            text(
                f"UPDATE {SCHEMA}.image_reclaim SET last_error = :err, "
                "next_attempt_at = now() + make_interval(secs => :delay) WHERE id = :id"
            ),
            {"id": task_id, "delay": delay_seconds, "err": error[:512]},
        )
        await self._session.commit()

    async def _commit_image_flip(
        self, result: Result[Any], outbox: ImageOutboxFactory | None
    ) -> Mapping[str, Any] | None:
        """Commit a guarded image UPDATE, emitting the event built from its own row.

        Returns the ``RETURNING`` row (``None`` when the guards rejected the write).
        """
        row = result.mappings().first()
        if row is not None and outbox is not None:
            self._session.add(self._outbox_row(outbox(row)))
        await self._session.commit()
        return row

    @staticmethod
    def _outbox_row(outbox: OutboxMessage) -> Outbox:
        event_type, payload = outbox
        return Outbox(event_type=event_type, payload=payload)
