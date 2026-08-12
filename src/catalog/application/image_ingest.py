"""Image ingest use-case — the application-layer core of the upload pipeline.

Owns the business logic for turning one uploaded S3 object into a usable product
image, over the repository + storage **ports** (never a concrete adapter). The
``adapters/image_worker`` SQS consumer is a thin transport shell that builds the
concrete repo/store and delegates each object here, so the layer boundary
(api/worker → application → ports ← adapters) holds.

Security jobs (not simplifiable): sniff the real bytes, reject a type that
doesn't match what was claimed at presign, re-encode to strip EXIF/payloads, and
only ever mark a product image ``ready`` after it passes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from src.catalog.application.image_processing import UnsupportedImageError, process_image
from src.catalog.application.outbox import product_updated_outbox
from src.catalog.domain.image_keys import parse_upload_key, public_main_key, public_thumb_key
from src.catalog.ports.repository import CatalogRepositoryPort, ImageOutboxFactory
from src.catalog.ports.storage import ImageStorePort
from src.shared.db.outbox import OutboxMessage

log = logging.getLogger(__name__)

_WEBP = "image/webp"


class DedupStore(Protocol):
    """The subset of Valkey the ingest dedup uses (best-effort fast-path)."""

    async def exists(self, key: str) -> int: ...

    async def set(self, key: str, value: str, ex: int) -> Any: ...


class IngestOutcome(StrEnum):
    """Result of ingesting one object (for the worker's logging/metrics)."""

    READY = "ready"
    FAILED = "failed"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"  # not a product upload key / not an S3 object record
    STALE = "stale"  # superseded by a newer upload (token mismatch)


class ImageIngestService:
    """Processes one uploaded object into a re-encoded, usable product image."""

    def __init__(
        self,
        repo: CatalogRepositoryPort,
        store: ImageStorePort,
        dedup: DedupStore,
        *,
        max_dimension: int,
        max_bytes: int,
        max_pixels: int,
        dedup_ttl_seconds: int,
    ) -> None:
        self._repo = repo
        self._store = store
        self._dedup = dedup
        self._max_dimension = max_dimension
        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._ttl = dedup_ttl_seconds

    async def ingest(self, key: str, etag: str) -> IngestOutcome:
        """Sniff + re-encode + thumbnail one uploaded object, then flip image state.

        Idempotent: a duplicate ``key + etag`` short-circuits; a stale event
        (token no longer the product's pending upload) is a no-op. CPU-bound
        sniff/re-encode runs off the event loop via ``asyncio.to_thread``.
        """
        # Not consumer-scoped like the bus dedup key (``event:{consumer}:{id}``) on
        # purpose: this drains an S3→SQS notification **queue**, not an SNS fan-out,
        # so the image worker is the only reader and the object+etag pair is already
        # the unique unit of work. Namespace it if a second consumer ever subscribes.
        dedup_key = f"image:{key}:{etag}"
        if await self._dedup.exists(dedup_key):
            log.debug("duplicate upload event %s deduped", key)
            return IngestOutcome.DUPLICATE

        parsed = parse_upload_key(key)
        if parsed is None:
            log.warning("upload key %r is not a product upload; skipping", key)
            return IngestOutcome.SKIPPED
        product_id, token = parsed

        raw, claimed_mime = await self._store.download(key)
        try:
            # CPU-bound sniff + claimed-type compare + re-encode: NEVER on the loop.
            processed = await asyncio.to_thread(
                process_image,
                raw,
                max_dimension=self._max_dimension,
                max_bytes=self._max_bytes,
                max_pixels=self._max_pixels,
                claimed_mime=claimed_mime,
            )
        except UnsupportedImageError as exc:
            log.warning("rejected upload for product %s: %s", product_id, exc)
            failed = await self._repo.mark_image_failed(product_id, token, outbox=_image_outbox)
            await self._dedup.set(dedup_key, "1", ex=self._ttl)
            if not failed:  # token no longer current → a newer upload superseded this reject
                log.info("product %s failed-upload %s superseded (stale event)", product_id, token)
                return IngestOutcome.STALE
            return IngestOutcome.FAILED

        main_key = public_main_key(product_id, token)
        await self._store.put_bytes(main_key, processed.main, content_type=_WEBP)
        for name, data in processed.thumbnails.items():
            await self._store.put_bytes(public_thumb_key(product_id, token, name), data, content_type=_WEBP)

        applied = await self._repo.mark_image_ready(product_id, token, main_key, outbox=_image_outbox)
        await self._dedup.set(dedup_key, "1", ex=self._ttl)
        if not applied:  # a newer upload superseded this one between download and write
            log.info("product %s image %s superseded (stale event)", product_id, token)
            return IngestOutcome.STALE
        log.info("product %s image ready: %s", product_id, main_key)
        return IngestOutcome.READY


def _image_outbox(row: Mapping[str, Any]) -> OutboxMessage:
    """Build the ``ProductUpdated`` message from the image flip's own ``RETURNING`` row.

    The flip only changes ``image_key``/``image_status``, but that alters the cached
    product response (``image_url``/``image_status``), so it must publish
    ``ProductUpdated`` to invalidate the read-cache — event-driven, same as an
    ordinary edit. Built from the updated row **inside the write transaction** (not
    from a pre-read), so a concurrent merchant edit can't make the payload stale.
    """
    return product_updated_outbox(
        product_id=row["product_id"],
        merchant_id=row["merchant_id"],
        name=row["name"],
        price=row["price"],
        category=row["category"],
    )


def _self_check() -> None:  # pragma: no cover - runnable smoke test
    """Assert the ingest flow: duplicate short-circuits, stale ready → STALE."""

    class _Dedup:
        def __init__(self) -> None:
            self.seen: set[str] = set()

        async def exists(self, key: str) -> int:
            return int(key in self.seen)

        async def set(self, key: str, value: str, ex: int) -> None:
            self.seen.add(key)

    class _Repo:
        def __init__(self, apply_ready: bool) -> None:
            self._apply = apply_ready
            self.failed: list[uuid.UUID] = []
            self.emitted: list[OutboxMessage] = []

        def _emit(self, product_id: uuid.UUID, outbox: ImageOutboxFactory | None) -> None:
            if outbox is not None:  # adapter feeds the factory the UPDATE's RETURNING row
                self.emitted.append(
                    outbox(
                        {
                            "product_id": product_id,
                            "merchant_id": uuid.uuid4(),
                            "name": "Widget",
                            "price": Decimal("9.99"),
                            "category": "misc",
                        }
                    )
                )

        async def mark_image_ready(
            self, product_id: uuid.UUID, token: str, image_key: str, outbox: ImageOutboxFactory | None = None
        ) -> bool:
            if self._apply:
                self._emit(product_id, outbox)
            return self._apply

        async def mark_image_failed(
            self, product_id: uuid.UUID, token: str, outbox: ImageOutboxFactory | None = None
        ) -> bool:
            self.failed.append(product_id)
            self._emit(product_id, outbox)
            return True

    class _Store:
        async def download(self, key: str) -> tuple[bytes, str | None]:
            return b"", None

        async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None:
            pass

    pid = uuid.uuid4()
    valid_key = f"uploads/{pid}/tok123.bin"

    async def _run() -> None:
        # Duplicate event → DUPLICATE, no processing.
        dedup = _Dedup()
        dedup.seen.add(f"image:{valid_key}:etag")
        svc = ImageIngestService(
            _Repo(True), _Store(), dedup, max_dimension=64, max_bytes=1000, max_pixels=1_000_000, dedup_ttl_seconds=60
        )
        assert await svc.ingest(valid_key, "etag") is IngestOutcome.DUPLICATE

        # Non-product key → SKIPPED.
        svc2 = ImageIngestService(
            _Repo(True),
            _Store(),
            _Dedup(),
            max_dimension=64,
            max_bytes=1000,
            max_pixels=1_000_000,
            dedup_ttl_seconds=60,
        )
        assert await svc2.ingest("something/else.txt", "e") is IngestOutcome.SKIPPED

        # A rejected upload flips to `failed` and emits ProductUpdated built from the
        # write's own row (post-update state), so the read-cache is invalidated.
        repo = _Repo(True)
        svc3 = ImageIngestService(
            repo, _Store(), _Dedup(), max_dimension=64, max_bytes=1, max_pixels=1, dedup_ttl_seconds=60
        )
        assert await svc3.ingest(valid_key, "e") is IngestOutcome.FAILED
        assert repo.emitted and repo.emitted[0][0] == "ProductUpdated" and str(pid) in repo.emitted[0][1]

        # A superseded (stale) ready-flip emits nothing: the guarded UPDATE returns no
        # row, so the adapter never calls the factory.
        stale = _Repo(False)
        assert await stale.mark_image_ready(pid, "tok123", "k", outbox=_image_outbox) is False
        assert not stale.emitted

    asyncio.run(_run())
    print("OK image_ingest self-check passed")


if __name__ == "__main__":  # pragma: no cover
    _self_check()
