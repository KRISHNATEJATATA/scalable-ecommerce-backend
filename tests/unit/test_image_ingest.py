"""Image ingest + worker orchestration tests.

Covers the pipeline's correctness invariants with fakes for the repository,
storage and dedup **ports**, so it runs anywhere (no libmagic, no S3, no DB):
replay safety, bounded/pinned reads, terminal-vs-retryable failures, and the
reclaim rules that keep ``public/`` from leaking objects.
"""

from __future__ import annotations

import json
import uuid

import pytest

from src.catalog.adapters import image_worker as worker_mod
from src.catalog.adapters.image_worker import ImageWorker
from src.catalog.application import image_ingest as ingest_mod
from src.catalog.application.image_ingest import ImageIngestService, IngestOutcome
from src.catalog.application.image_processing import UnsupportedImageError
from src.catalog.domain.image_keys import content_version, public_main_key, public_rendition_keys, upload_key
from src.catalog.ports.repository import ImageFlip, ImageReclaimTask, PendingUpload
from src.catalog.ports.storage import (
    DownloadedObject,
    ObjectChangedError,
    ObjectNotFoundError,
    ObjectTooLargeError,
)

PID = uuid.UUID("11111111-2222-3333-4444-555555555555")
TOKEN = "tok123"
KEY = upload_key(PID, TOKEN)
ETAG = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
RAW = b"rawbytes"
VERSION = content_version(RAW)


class FakeDedup:
    def __init__(self) -> None:
        self.seen: set[str] = set()

    async def exists(self, key: str) -> int:
        return int(key in self.seen)

    async def set(self, key: str, value: str, ex: int) -> None:
        self.seen.add(key)


class BrokenDedup:
    async def exists(self, key: str) -> int:
        raise ConnectionError("valkey down")

    async def set(self, key: str, value: str, ex: int) -> None:
        raise ConnectionError("valkey down")


class FakeRepo:
    def __init__(self, *, applied: bool = True, previous_key: str | None = None, live_key: str | None = None) -> None:
        self._flip = ImageFlip(applied, previous_key)
        self.live_key = live_key
        self.failed: list[uuid.UUID] = []
        self.events: list[tuple[str, str]] = []
        self.ready_calls: list[str] = []
        self.reclaims: list[ImageReclaimTask] = []
        self.deferred: list[tuple[int, int, str]] = []
        self.reaper_calls: list[tuple[int, int]] = []
        self.due_uploads: list[PendingUpload] = []
        self.expired: list[tuple[uuid.UUID, str]] = []
        self.deferred_uploads: list[tuple[uuid.UUID, str, int]] = []
        self._next_id = 0

    async def mark_image_ready(self, product_id, upload_token, image_key, outbox=None) -> ImageFlip:
        self.ready_calls.append(image_key)
        if self._flip.applied:
            if outbox is not None:
                self.events.append(outbox(_event_row(product_id)))
            # The real repository queues the replaced key in the flip's own txn.
            if self._flip.previous_key and self._flip.previous_key != image_key:
                await self.schedule_image_reclaim(product_id, self._flip.previous_key)
        return self._flip

    async def mark_image_failed(self, product_id, upload_token, outbox=None) -> bool:
        self.failed.append(product_id)
        if outbox is not None:
            self.events.append(outbox(_event_row(product_id)))
        return True

    async def current_image_key(self, product_id) -> str | None:
        return self.live_key

    async def schedule_image_reclaim(self, product_id, object_key: str) -> None:
        if any(t.object_key == object_key for t in self.reclaims):
            return  # ON CONFLICT DO NOTHING
        self._next_id += 1
        self.reclaims.append(ImageReclaimTask(self._next_id, product_id, object_key, 0))

    async def claim_image_reclaims(self, *, batch_size: int) -> list[ImageReclaimTask]:
        return self.reclaims[:batch_size]

    async def finish_image_reclaim(self, ids: list[int]) -> None:
        self.reclaims = [t for t in self.reclaims if t.id not in ids]

    async def defer_image_reclaim(self, task_id: int, *, delay_seconds: int, error: str) -> None:
        self.deferred.append((task_id, delay_seconds, error))

    async def expire_abandoned_upload(self, product_id, upload_token: str, outbox=None) -> bool:
        self.expired.append((product_id, upload_token))
        return True

    async def due_pending_uploads(self, *, grace_seconds: int, batch_size: int):
        self.reaper_calls.append((grace_seconds, batch_size))
        return list(self.due_uploads)

    async def defer_upload_expiry(self, product_id, upload_token: str, *, delay_seconds: int) -> None:
        self.deferred_uploads.append((product_id, upload_token, delay_seconds))


def _event_row(product_id: uuid.UUID) -> dict:
    return {
        "product_id": product_id,
        "merchant_id": uuid.uuid4(),
        "name": "Widget",
        "price": "9.99",
        "category": "misc",
        "product_version": 2,
    }


class FakeStore:
    """In-memory object store; records the arguments the ingest passes to ``download``."""

    def __init__(self, *, data: bytes = RAW, etag: str = ETAG, error: Exception | None = None) -> None:
        self._data = data
        self._etag = etag
        self._error = error
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.probed: list[str] = []
        self.probe_error: Exception | None = None
        self.download_calls: list[tuple[str, int, str | None]] = []

    async def download(self, key: str, *, max_bytes: int, expected_etag: str | None = None) -> DownloadedObject:
        self.download_calls.append((key, max_bytes, expected_etag))
        if self._error is not None:
            raise self._error
        return DownloadedObject(self._data, "image/jpeg", self._etag)

    async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None:
        self.objects[key] = data

    async def exists(self, key: str) -> bool:
        self.probed.append(key)
        if self.probe_error is not None:
            raise self.probe_error
        return key in self.objects

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.objects.pop(key, None)


class FakeProcessed:
    main = b"webp-main"
    thumbnails = {"thumb_256": b"t256", "thumb_64": b"t64"}
    mime = "image/jpeg"


@pytest.fixture
def processed(monkeypatch):
    """Replace the CPU-bound sniff/re-encode (needs libmagic) with a stub."""

    def _fake(raw, **kwargs):
        return FakeProcessed()

    monkeypatch.setattr(ingest_mod, "process_image", _fake)
    return FakeProcessed


def build(repo: FakeRepo, store: FakeStore, dedup=None) -> ImageIngestService:
    return ImageIngestService(
        repo,
        store,
        dedup or FakeDedup(),
        max_dimension=2048,
        max_bytes=5_000_000,
        max_pixels=40_000_000,
        dedup_ttl_seconds=60,
        upload_reaper_grace_seconds=900,
    )


async def test_ingest_writes_content_addressed_renditions_and_emits_event(processed):
    repo, store = FakeRepo(), FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.READY

    main = public_main_key(PID, TOKEN, VERSION)
    assert sorted(store.objects) == sorted(public_rendition_keys(main))
    assert repo.ready_calls == [main]
    assert repo.events and repo.events[0][0] == "ProductUpdated"


async def test_download_is_pinned_to_the_event_etag_and_bounded(processed):
    store = FakeStore()
    await build(FakeRepo(), store).ingest(KEY, ETAG)
    key, max_bytes, expected = store.download_calls[0]
    assert (key, expected) == (KEY, ETAG)
    assert max_bytes == 5_000_000


async def test_key_is_addressed_by_the_bytes_read_not_the_event(processed):
    """A replay wins the race: the event's ETag is stale, the bytes read decide.

    Hashed with SHA-256 rather than reusing S3's (MD5) ETag — crafted collisions
    would otherwise let two different files share one public key.
    """
    store = FakeStore(data=b"other-bytes", etag="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    repo = FakeRepo()
    await build(repo, store).ingest(KEY, ETAG)
    assert repo.ready_calls == [public_main_key(PID, TOKEN, content_version(b"other-bytes"))]


async def test_changed_object_is_acked_without_processing(processed):
    """IfMatch rejected the read: the newer bytes have their own event."""
    repo, store = FakeRepo(), FakeStore(error=ObjectChangedError(KEY))
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.STALE
    assert not store.objects and not repo.ready_calls and not repo.failed


async def test_vanished_object_is_terminal_not_a_retry(processed):
    repo, store = FakeRepo(), FakeStore(error=ObjectNotFoundError(KEY))
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.FAILED
    assert repo.failed == [PID]


async def test_oversize_object_is_rejected_terminally(processed):
    repo, store = FakeRepo(), FakeStore(error=ObjectTooLargeError(KEY))
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.FAILED
    assert repo.failed == [PID]


async def test_spoofed_upload_flips_to_failed(monkeypatch):
    def _reject(raw, **kwargs):
        raise UnsupportedImageError("nope")

    monkeypatch.setattr(ingest_mod, "process_image", _reject)
    repo, store = FakeRepo(), FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.FAILED
    assert repo.failed == [PID] and not store.objects


async def test_duplicate_event_short_circuits(processed):
    dedup = FakeDedup()
    dedup.seen.add(f"image:{KEY}:{ETAG}")
    repo, store = FakeRepo(), FakeStore()
    assert await build(repo, store, dedup).ingest(KEY, ETAG) is IngestOutcome.DUPLICATE
    assert not store.download_calls


async def test_dedup_outage_does_not_fail_the_upload(processed):
    repo, store = FakeRepo(), FakeStore()
    assert await build(repo, store, BrokenDedup()).ingest(KEY, ETAG) is IngestOutcome.READY


async def test_non_product_key_is_skipped(processed):
    store = FakeStore()
    assert await build(FakeRepo(), store).ingest("something/else.txt", ETAG) is IngestOutcome.SKIPPED
    assert not store.download_calls


async def test_stale_flip_queues_its_own_unreferenced_renditions(processed):
    main = public_main_key(PID, TOKEN, VERSION)
    repo = FakeRepo(applied=False, live_key=public_main_key(PID, TOKEN, "other"))
    store = FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.STALE
    assert [t.object_key for t in repo.reclaims] == [main]


async def test_redelivery_of_the_live_object_keeps_it(processed):
    main = public_main_key(PID, TOKEN, VERSION)
    repo, store = FakeRepo(applied=False, live_key=main), FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.STALE
    assert repo.reclaims == []


async def test_soft_deleted_product_renditions_are_queued(processed):
    repo, store = FakeRepo(applied=False, live_key=None), FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.STALE
    assert repo.reclaims, "a gone product's renditions must not be left behind"


async def test_replaced_image_is_queued_for_reclaim(processed):
    previous = public_main_key(PID, "oldtok", "oldver")
    repo, store = FakeRepo(previous_key=previous), FakeStore()
    assert await build(repo, store).ingest(KEY, ETAG) is IngestOutcome.READY
    assert [t.object_key for t in repo.reclaims] == [previous]


async def test_scheduling_a_reclaim_twice_is_idempotent(processed):
    repo = FakeRepo(applied=False, live_key="something/else.webp")
    await build(repo, FakeStore()).ingest(KEY, ETAG)
    await build(repo, FakeStore()).ingest(KEY, ETAG)
    assert len(repo.reclaims) == 1


async def test_a_failed_cleanup_enqueue_is_not_hidden_by_dedup(processed):
    """The dedup marker is what makes a redelivery a no-op, so it must be written
    only *after* the cleanup is durably queued — otherwise a failed enqueue leaks
    the public objects forever: SQS redelivers, dedup acks instantly, done."""

    class Unschedulable(FakeRepo):
        async def schedule_image_reclaim(self, product_id, object_key: str) -> None:
            raise RuntimeError("db down")

    dedup = FakeDedup()
    repo = Unschedulable(applied=False, live_key="something/else.webp")
    with pytest.raises(RuntimeError):
        await build(repo, FakeStore(), dedup).ingest(KEY, ETAG)
    assert not dedup.seen, "a redelivery must still reprocess and retry the cleanup"


# --- abandoned-presign reaper -----------------------------------------------


async def test_reaper_sweeps_with_the_configured_grace():
    repo, store = FakeRepo(), FakeStore()
    await build(repo, store).reap_abandoned_uploads()
    assert repo.reaper_calls == [(900, 100)]


async def test_reaper_restores_a_product_whose_raw_upload_never_arrived():
    repo, store = FakeRepo(), FakeStore()
    repo.due_uploads = [PendingUpload(PID, TOKEN)]
    assert await build(repo, store).reap_abandoned_uploads() == 1
    assert store.probed == [upload_key(PID, TOKEN)]
    assert repo.expired == [(PID, TOKEN)]
    assert not repo.deferred_uploads


async def test_reaper_defers_instead_of_discarding_an_upload_whose_event_is_stuck():
    """The bytes are there — the event is merely backlogged/DLQ'd. Reaping would
    clear the token its eventual replay is guarded on."""
    repo, store = FakeRepo(), FakeStore()
    repo.due_uploads = [PendingUpload(PID, TOKEN)]
    store.objects[upload_key(PID, TOKEN)] = RAW
    assert await build(repo, store).reap_abandoned_uploads(recheck_seconds=60) == 0
    assert repo.deferred_uploads == [(PID, TOKEN, 60)]
    assert not repo.expired


async def test_reaper_reaps_nothing_when_the_probe_fails():
    repo, store = FakeRepo(), FakeStore()
    repo.due_uploads = [PendingUpload(PID, TOKEN)]
    store.probe_error = RuntimeError("s3 down")
    assert await build(repo, store).reap_abandoned_uploads() == 0
    assert not repo.expired and not repo.deferred_uploads


# --- durable reclaim drain --------------------------------------------------


async def test_drain_deletes_every_rendition_and_clears_the_row():
    previous = public_main_key(PID, "oldtok", "oldver")
    repo, store = FakeRepo(), FakeStore()
    await repo.schedule_image_reclaim(PID, previous)
    assert await build(repo, store).drain_reclaims() == 1
    assert sorted(store.deleted) == sorted(public_rendition_keys(previous))
    assert repo.reclaims == []


async def test_drain_keeps_the_row_when_s3_fails():
    class Exploding(FakeStore):
        async def delete(self, key: str) -> None:
            raise RuntimeError("s3 down")

    repo = FakeRepo()
    await repo.schedule_image_reclaim(PID, public_main_key(PID, "oldtok", "oldver"))
    assert await build(repo, Exploding()).drain_reclaims() == 0
    assert repo.reclaims, "a transient S3 fault must not drop the cleanup"
    assert repo.deferred and repo.deferred[0][2].startswith("RuntimeError")


async def test_drain_skips_a_key_that_became_live_again():
    live = public_main_key(PID, TOKEN, VERSION)
    repo, store = FakeRepo(live_key=live), FakeStore()
    await repo.schedule_image_reclaim(PID, live)
    assert await build(repo, store).drain_reclaims() == 1
    assert store.deleted == [], "the product serves those objects"
    assert repo.reclaims == []


async def test_drain_is_a_no_op_on_an_empty_queue():
    repo, store = FakeRepo(), FakeStore()
    assert await build(repo, store).drain_reclaims() == 0
    assert store.deleted == []


# --- worker transport -------------------------------------------------------


class FakeSqs:
    def __init__(self, messages: list[dict]) -> None:
        self._messages = messages
        self.deleted: list[str] = []

    async def receive_message(self, **kwargs) -> dict:
        return {"Messages": self._messages}

    async def delete_message(self, QueueUrl: str, ReceiptHandle: str) -> None:  # noqa: N803 (botocore kwargs)
        self.deleted.append(ReceiptHandle)


class FakeSessionmaker:
    def __call__(self):
        return self

    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc) -> None:
        return None


def _s3_message(handle: str, *, key: str = KEY, etag: str = ETAG) -> dict:
    body = {"Records": [{"s3": {"object": {"key": key, "eTag": etag}}}]}
    return {"Body": json.dumps(body), "ReceiptHandle": handle}


def _worker(monkeypatch, repo: FakeRepo, store: FakeStore, messages: list[dict]) -> tuple[ImageWorker, FakeSqs]:
    monkeypatch.setattr(worker_mod, "CatalogRepository", lambda session: repo)
    sqs = FakeSqs(messages)
    return (
        ImageWorker(
            sqs,
            store,
            FakeDedup(),
            FakeSessionmaker(),
            "http://queue",
            max_dimension=2048,
            max_bytes=5_000_000,
            max_pixels=40_000_000,
            dedup_ttl_seconds=60,
        ),
        sqs,
    )


async def test_worker_acks_a_handled_message(monkeypatch, processed):
    repo, store = FakeRepo(), FakeStore()
    worker, sqs = _worker(monkeypatch, repo, store, [_s3_message("h1")])
    assert await worker.poll_once() == 1
    assert sqs.deleted == ["h1"] and repo.ready_calls


async def test_worker_leaves_a_poison_message_for_redrive(monkeypatch, processed):
    repo = FakeRepo()
    store = FakeStore(error=RuntimeError("transient s3 fault"))
    worker, sqs = _worker(monkeypatch, repo, store, [_s3_message("h1")])
    assert await worker.poll_once() == 0
    assert sqs.deleted == [], "a failed message must stay visible for SQS redrive → DLQ"


async def test_worker_ignores_non_s3_records(monkeypatch, processed):
    repo, store = FakeRepo(), FakeStore()
    message = {"Body": json.dumps({"Records": [{"eventName": "s3:TestEvent"}]}), "ReceiptHandle": "h1"}
    worker, sqs = _worker(monkeypatch, repo, store, [message])
    assert await worker.poll_once() == 1
    assert sqs.deleted == ["h1"] and not store.download_calls


async def test_worker_drains_the_reclaim_queue_each_pass(monkeypatch, processed):
    previous = public_main_key(PID, "oldtok", "oldver")
    repo, store = FakeRepo(), FakeStore()
    await repo.schedule_image_reclaim(PID, previous)
    worker, _ = _worker(monkeypatch, repo, store, [])
    assert await worker.poll_once() == 0
    assert sorted(store.deleted) == sorted(public_rendition_keys(previous))
    assert repo.reaper_calls, "the abandoned-upload reaper runs on the same pass"


async def test_worker_survives_a_failing_reclaim_sweep(monkeypatch, processed):
    class BrokenRepo(FakeRepo):
        async def claim_image_reclaims(self, *, batch_size: int):
            raise RuntimeError("db down")

    repo, store = BrokenRepo(), FakeStore()
    worker, sqs = _worker(monkeypatch, repo, store, [_s3_message("h1")])
    assert await worker.poll_once() == 1, "cleanup trouble must not stall message handling"
    assert sqs.deleted == ["h1"]
