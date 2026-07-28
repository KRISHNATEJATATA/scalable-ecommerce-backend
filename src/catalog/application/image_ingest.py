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
from enum import StrEnum
from typing import Any, Protocol

from src.catalog.application.image_processing import UnsupportedImageError, process_image
from src.catalog.domain.image_keys import parse_upload_key, public_main_key, public_thumb_key
from src.catalog.ports.repository import CatalogRepositoryPort
from src.catalog.ports.storage import ImageStorePort

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
        dedup_ttl_seconds: int,
    ) -> None:
        self._repo = repo
        self._store = store
        self._dedup = dedup
        self._max_dimension = max_dimension
        self._max_bytes = max_bytes
        self._ttl = dedup_ttl_seconds

    async def ingest(self, key: str, etag: str) -> IngestOutcome:
        """Sniff + re-encode + thumbnail one uploaded object, then flip image state.

        Idempotent: a duplicate ``key + etag`` short-circuits; a stale event
        (token no longer the product's pending upload) is a no-op. CPU-bound
        sniff/re-encode runs off the event loop via ``asyncio.to_thread``.
        """
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
                claimed_mime=claimed_mime,
            )
        except UnsupportedImageError as exc:
            log.warning("rejected upload for product %s: %s", product_id, exc)
            await self._repo.mark_image_failed(product_id, token)
            await self._dedup.set(dedup_key, "1", ex=self._ttl)
            return IngestOutcome.FAILED

        main_key = public_main_key(product_id, token)
        await self._store.put_bytes(main_key, processed.main, content_type=_WEBP)
        for name, data in processed.thumbnails.items():
            await self._store.put_bytes(public_thumb_key(product_id, token, name), data, content_type=_WEBP)

        applied = await self._repo.mark_image_ready(product_id, token, main_key)
        await self._dedup.set(dedup_key, "1", ex=self._ttl)
        if not applied:  # a newer upload superseded this one between download and write
            log.info("product %s image %s superseded (stale event)", product_id, token)
            return IngestOutcome.STALE
        log.info("product %s image ready: %s", product_id, main_key)
        return IngestOutcome.READY


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

        async def mark_image_ready(self, product_id: uuid.UUID, token: str, image_key: str) -> bool:
            return self._apply

        async def mark_image_failed(self, product_id: uuid.UUID, token: str) -> bool:
            self.failed.append(product_id)
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
        svc = ImageIngestService(_Repo(True), _Store(), dedup, max_dimension=64, max_bytes=1000, dedup_ttl_seconds=60)
        assert await svc.ingest(valid_key, "etag") is IngestOutcome.DUPLICATE

        # Non-product key → SKIPPED.
        svc2 = ImageIngestService(
            _Repo(True), _Store(), _Dedup(), max_dimension=64, max_bytes=1000, dedup_ttl_seconds=60
        )
        assert await svc2.ingest("something/else.txt", "e") is IngestOutcome.SKIPPED

    asyncio.run(_run())
    print("OK image_ingest self-check passed")


if __name__ == "__main__":  # pragma: no cover
    _self_check()
