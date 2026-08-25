"""Image ingest use-case — the application-layer core of the upload pipeline.

Owns the business logic for turning one uploaded S3 object into a usable product
image, over the repository + storage **ports** (never a concrete adapter). The
``adapters/image_worker`` SQS consumer is a thin transport shell that builds the
concrete repo/store and delegates each object here, so the layer boundary
(api/worker → application → ports ← adapters) holds.

Security jobs (not simplifiable): sniff the real bytes, reject a type that
doesn't match what was claimed at presign, re-encode to strip EXIF/payloads, and
only ever mark a product image ``ready`` after it passes.

Orchestration is covered by ``tests/unit/test_image_ingest.py`` (fakes for the
repo/store/dedup ports, so it needs neither libmagic nor S3).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Protocol

from src.catalog.application.image_processing import UnsupportedImageError, process_image
from src.catalog.application.outbox import product_updated_outbox
from src.catalog.domain.image_keys import (
    content_version,
    parse_upload_key,
    public_main_key,
    public_rendition_keys,
    public_thumb_key,
    upload_key,
)
from src.catalog.ports.repository import CatalogRepositoryPort
from src.catalog.ports.storage import (
    ImageStorePort,
    ObjectChangedError,
    ObjectNotFoundError,
    ObjectTooLargeError,
)
from src.shared.config.setting import AppSettings
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
        upload_reaper_grace_seconds: int | None = None,
    ) -> None:
        self._repo = repo
        self._store = store
        self._dedup = dedup
        self._max_dimension = max_dimension
        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._ttl = dedup_ttl_seconds
        self._reaper_grace = (
            upload_reaper_grace_seconds
            if upload_reaper_grace_seconds is not None
            else AppSettings.model_fields["image_upload_reaper_grace_seconds"].default
        )

    async def ingest(self, key: str, etag: str) -> IngestOutcome:
        """Sniff + re-encode + thumbnail one uploaded object, then flip image state.

        Idempotent: a duplicate ``key + etag`` short-circuits; a stale event
        (token no longer the product's pending upload) is a no-op. CPU-bound
        sniff/re-encode runs off the event loop via ``asyncio.to_thread``. The
        Valkey dedup is best-effort — an outage degrades to reprocessing (the DB
        token guards keep that safe), it never fails an otherwise-valid upload.
        A **vanished** raw object (lifecycle-expired) is terminal, not a retry: the
        image flips to ``failed`` so the message is acked instead of re-DLQ'ing.
        The read is **``IfMatch``-pinned** to the event's ETag and **bounded** by
        ``max_bytes``, and public keys are **content-addressed**, so a replayed
        presigned POST can neither be processed under the wrong event nor overwrite
        the bytes a ``ready`` product serves. The image a successful flip *replaces*
        is reclaimed, so re-uploads don't leak renditions.
        """
        # Not consumer-scoped like the bus dedup key (``event:{consumer}:{id}``) on
        # purpose: this drains an S3→SQS notification **queue**, not an SNS fan-out,
        # so the image worker is the only reader and the object+etag pair is already
        # the unique unit of work. Namespace it if a second consumer ever subscribes.
        dedup_key = f"image:{key}:{etag}"
        if await self._seen(dedup_key):
            log.debug("duplicate upload event %s deduped", key)
            return IngestOutcome.DUPLICATE

        parsed = parse_upload_key(key)
        if parsed is None:
            log.warning("upload key %r is not a product upload; skipping", key)
            return IngestOutcome.SKIPPED
        product_id, token = parsed

        try:
            obj = await self._store.download(key, max_bytes=self._max_bytes, expected_etag=etag or None)
        except ObjectNotFoundError:
            # The raw upload is gone (lifecycle-expired past IMAGE_UPLOAD_RETENTION_DAYS,
            # or deleted). Retrying can never recover it, so redriving this message
            # would just re-DLQ it forever and leave the product stuck `pending`.
            # Terminal `failed` instead: the merchant can re-presign and re-upload.
            log.warning("upload object %s is gone; marking product %s failed", key, product_id)
            return await self._fail(product_id, token, dedup_key)
        except ObjectChangedError:
            # The key now holds *newer* bytes than this event described (the presigned
            # POST was replayed). Processing them under this event would attach content
            # the event never described; the newer bytes have their own ObjectCreated
            # event, so this one is finished — ack it, don't retry.
            log.info("upload object %s changed since its event; skipping stale read", key)
            await self._remember(dedup_key)
            return IngestOutcome.STALE
        except ObjectTooLargeError as exc:
            # Never fully read (the adapter bounds the read), so this is cheap and safe.
            log.warning("rejected oversize upload for product %s: %s", product_id, exc)
            return await self._fail(product_id, token, dedup_key)

        # A presigned POST stays usable for its whole TTL, so the same token can be
        # re-uploaded with *different* bytes. Keying the public objects by content
        # (not by token alone) makes that write a new key instead of silently
        # mutating an already-``ready``, ``immutable``-cached image. Hashed from the
        # bytes we actually **read** (``IfMatch``-pinned to the event's ETag): the
        # event's own ETag is an MD5, whose collisions are cheap to craft.
        version = await asyncio.to_thread(content_version, obj.data)
        try:
            # CPU-bound sniff + claimed-type compare + re-encode: NEVER on the loop.
            processed = await asyncio.to_thread(
                process_image,
                obj.data,
                max_dimension=self._max_dimension,
                max_bytes=self._max_bytes,
                max_pixels=self._max_pixels,
                claimed_mime=obj.content_type,
            )
        except UnsupportedImageError as exc:
            log.warning("rejected upload for product %s: %s", product_id, exc)
            return await self._fail(product_id, token, dedup_key)

        main_key = public_main_key(product_id, token, version)
        await self._store.put_bytes(main_key, processed.main, content_type=_WEBP)
        for name, data in processed.thumbnails.items():
            await self._store.put_bytes(public_thumb_key(product_id, token, version, name), data, content_type=_WEBP)

        flip = await self._repo.mark_image_ready(product_id, token, main_key, outbox=_image_outbox)
        if not flip.applied:  # a newer upload superseded this one between download and write
            log.info("product %s image %s superseded (stale event)", product_id, token)
            # Queue the cleanup *before* the dedup marker. The marker is what makes a
            # redelivery a no-op, so remembering first would turn a failed enqueue
            # into a permanent leak: SQS would redeliver, dedup would ack instantly,
            # and the renditions we just wrote would never be reclaimed by anyone.
            await self._schedule_reclaim(product_id, main_key)
            await self._remember(dedup_key)
            return IngestOutcome.STALE
        # A replaced image's renditions were queued for deletion by the flip itself,
        # in the same transaction (``catalog.image_reclaim``); ``drain_reclaims``
        # performs the actual S3 deletes with retries.
        await self._remember(dedup_key)
        log.info("product %s image ready: %s", product_id, main_key)
        return IngestOutcome.READY

    async def _fail(self, product_id: uuid.UUID, token: str, dedup_key: str) -> IngestOutcome:
        """Terminally flip the image to ``failed`` (token-guarded) and remember the event."""
        failed = await self._repo.mark_image_failed(product_id, token, outbox=_image_outbox)
        await self._remember(dedup_key)
        if not failed:  # token no longer current → a newer upload superseded this reject
            log.info("product %s failed-upload %s superseded (stale event)", product_id, token)
            return IngestOutcome.STALE
        return IngestOutcome.FAILED

    async def _seen(self, dedup_key: str) -> bool:
        """Has this ``key + etag`` already been ingested? Best-effort.

        A Valkey fault must not fail the job: the dedup is only a fast-path that
        saves a re-download/re-encode. Durable idempotency lives in the DB — the
        token-guarded ``mark_image_*`` UPDATEs make a replay a no-op (``STALE``) —
        so on an outage we degrade to reprocessing, never to a DLQ'd valid upload.
        """
        try:
            return bool(await self._dedup.exists(dedup_key))
        except Exception:  # cache boundary: Valkey down / bad reply → reprocess instead of failing
            log.warning("image dedup lookup failed for %s; processing anyway", dedup_key, exc_info=True)
            return False

    async def _remember(self, dedup_key: str) -> None:
        """Record this ``key + etag`` as ingested. Best-effort (see ``_seen``)."""
        try:
            await self._dedup.set(dedup_key, "1", ex=self._ttl)
        except Exception:  # cache boundary: losing the marker only costs a redundant reprocess
            log.warning("image dedup write failed for %s; continuing", dedup_key, exc_info=True)

    async def _schedule_reclaim(self, product_id: uuid.UUID, main_key: str) -> None:
        """Queue renditions written for a flip that didn't land — they are garbage.

        The public objects are written *before* the guarded UPDATE (the flip must
        attach an object that already exists), so a stale event leaves unreferenced
        keys behind that nothing will ever reclaim: ``public/`` is live CDN content,
        deliberately outside the ``uploads/`` lifecycle rule.

        The guard compares the product's **current ``image_key``** with what we just
        wrote, not the upload token: a stale flip has three causes — a *superseded*
        token, a *replayed* presigned POST carrying different bytes under the same
        token (a different content version ⇒ a different key), and a plain
        *redelivery* of the already-``ready`` object (the ``pending`` guard). Only
        the last one wrote the keys the product actually serves, and only there does
        the key match. A missing/soft-deleted product reports ``None``, which also
        never matches, so its renditions are reclaimed too.

        Unlike a replaced image (queued inside the flip's own transaction), there is
        no state change to attach this to, so the intent is its own committed row —
        which still makes it durable: the caller schedules **before** writing the
        dedup marker, so if this raises, the message is left unacked *and* a
        redelivery actually reprocesses it instead of being deduped away. The
        enqueue is idempotent, so that retry costs nothing.
        """
        if await self._repo.current_image_key(product_id) == main_key:
            return  # redelivery of the live object → those keys are in use
        await self._repo.schedule_image_reclaim(product_id, main_key)

    async def reap_abandoned_uploads(self, *, batch_size: int = 100, recheck_seconds: int = 3600) -> int:
        """Restore products whose presigned upload expired with nothing uploaded.

        Presigning is a state change (``ready``/``none`` → ``pending``, and
        ``image_url`` disappears), but only an *upload* ever changes it back. A
        client that closes the tab therefore leaves the product — including the
        image it was already serving — unavailable indefinitely. This sweep is the
        backstop, in the same spirit as the inventory reservation reaper: the row
        goes back to ``ready`` if a processed key survived, else ``none``.

        **Time alone can't tell "never uploaded" from "uploaded, event delayed".**
        A queue backlog, a redrive, or a message parked in the DLQ awaiting a fix
        can all outlive the grace by hours; reaping then clears the token the
        eventual flip is guarded on, so the replay lands as *stale* and a perfectly
        valid image is discarded. So each candidate is checked against the one
        authority on whether the bytes arrived — the raw object itself. If it is
        there, the deadline is pushed out (``recheck_seconds``) and the product
        stays ``pending``: the image is still coming. Only once the raw upload is
        genuinely absent — never written, or lifecycle-expired past
        ``IMAGE_UPLOAD_RETENTION_DAYS``, at which point no replay could succeed
        anyway — is the product restored.

        A probe that errors (S3 outage, permissions) reaps nothing: not restoring is
        always recoverable, discarding a live image is not.
        """
        candidates = await self._repo.due_pending_uploads(grace_seconds=self._reaper_grace, batch_size=batch_size)
        restored = 0
        for product_id, token in candidates:
            try:
                uploaded = await self._store.exists(upload_key(product_id, token))
            except Exception:  # boundary: a failed probe must never be read as "no upload"
                log.warning("could not probe upload for product %s; not reaping", product_id, exc_info=True)
                continue
            if uploaded:
                log.info("product %s upload arrived but is unprocessed; deferring reap", product_id)
                await self._repo.defer_upload_expiry(product_id, token, delay_seconds=recheck_seconds)
                continue
            if await self._repo.expire_abandoned_upload(product_id, token, outbox=_image_outbox):
                restored += 1
        if restored:
            log.info("reaped %s abandoned image upload(s)", restored)
        return restored

    async def drain_reclaims(self, *, batch_size: int = 20, retry_backoff_seconds: int = 60) -> int:
        """Delete queued unreferenced renditions from S3; returns how many finished.

        The durable half of cleanup (the outbox pattern, for object storage). Rows
        are claimed ``FOR UPDATE SKIP LOCKED`` so N workers split the queue; a
        failing delete is backed off and retried instead of being swallowed, so an
        S3 outage postpones cleanup rather than leaking objects forever. Deletes are
        idempotent, so a crash mid-batch only costs a repeat.

        The product's *current* key is re-checked at delete time: the claim may have
        been written long before this runs, and nothing else guarantees the key
        hasn't since become live again.
        """
        tasks = await self._repo.claim_image_reclaims(batch_size=batch_size)
        if not tasks:
            return 0
        done: list[int] = []
        for task in tasks:
            try:
                if await self._repo.current_image_key(task.product_id) == task.object_key:
                    log.info("reclaim %s is live again; dropping the request", task.object_key)
                    done.append(task.id)
                    continue
                for key in public_rendition_keys(task.object_key):
                    await self._store.delete(key)
                done.append(task.id)
                log.info("reclaimed renditions: %s", task.object_key)
            except Exception as exc:  # transient S3/DB fault: back off, keep the row
                log.warning("reclaim of %s failed (attempt %s); retrying", task.object_key, task.attempts)
                await self._repo.defer_image_reclaim(
                    task.id, delay_seconds=retry_backoff_seconds, error=f"{type(exc).__name__}: {exc}"
                )
        if done:
            await self._repo.finish_image_reclaim(done)
        return len(done)


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
        product_version=row["product_version"],
    )
