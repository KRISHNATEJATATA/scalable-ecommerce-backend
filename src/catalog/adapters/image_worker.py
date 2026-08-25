"""Image worker — the `service`-role S3-event consumer for the upload pipeline.

Thin SQS transport shell: it long-polls the ``image-uploads`` queue that S3
ObjectCreated notifications land in (LocalStack locally; a real S3→SQS
notification in the cloud), builds the concrete repository/store adapters per
message, and delegates every uploaded object to the application-layer
:class:`~src.catalog.application.image_ingest.ImageIngestService` (which owns the
sniff/re-encode/mark-usable logic over the ports). Keeping the business logic in
the application layer means this adapter never touches the DB directly.

A handler that raises leaves the message for SQS redrive → DLQ (replay per
``docs/RUNBOOK.md``); a clean pass (including a rejected-but-handled upload)
deletes the message. Run: ``python -m src.catalog.adapters.image_worker``.

**Rollout phase: producer, not consumer.** What it consumes is a raw S3
notification, which carries no ``schema_version`` — but marking an image usable
writes a ``ProductUpdatedV2`` outbox row, so this worker *emits* domain events. On
an event-version bump it deploys in the **producer** phase, alongside the API and
after the bus consumers (the cache worker) are stable — see ``docs/DEPLOYMENT.md``
§ "Rolling out a new event version".
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any
from urllib.parse import unquote_plus

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.catalog.adapters.db.repository import CatalogRepository
from src.catalog.adapters.s3_images import ImageStore
from src.catalog.application.image_ingest import ImageIngestService
from src.shared.bus.client import sqs_client
from src.shared.bus.polling import poll_forever
from src.shared.clients.s3_client import s3_client
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class ImageWorker:
    """SQS transport for the image pipeline; each object is handed to the ingest service."""

    def __init__(
        self,
        sqs: Any,
        store: ImageStore,
        valkey: Any,
        sessionmaker: async_sessionmaker,
        queue_url: str,
        *,
        max_dimension: int,
        max_bytes: int,
        max_pixels: int,
        dedup_ttl_seconds: int,
        upload_reaper_grace_seconds: int | None = None,
        max_messages: int = 1,
        wait_time_seconds: int = 10,
    ) -> None:
        self._sqs = sqs
        self._store = store
        self._valkey = valkey
        self._sessionmaker = sessionmaker
        self._queue_url = queue_url
        self._max_dimension = max_dimension
        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._ttl = dedup_ttl_seconds
        self._reaper_grace = upload_reaper_grace_seconds
        self._max_messages = max_messages
        self._wait = wait_time_seconds

    def _service(self, session: Any) -> ImageIngestService:
        """Build the application service over per-session adapters."""
        return ImageIngestService(
            CatalogRepository(session),
            self._store,
            self._valkey,
            max_dimension=self._max_dimension,
            max_bytes=self._max_bytes,
            max_pixels=self._max_pixels,
            dedup_ttl_seconds=self._ttl,
            upload_reaper_grace_seconds=self._reaper_grace,
        )

    async def _process_record(self, record: dict) -> None:
        """Ingest one S3 record via a per-message session + application service."""
        s3 = record.get("s3")
        if not s3:  # not an S3 record (e.g. s3:TestEvent) → nothing to do
            return
        key = unquote_plus(s3["object"]["key"])
        etag = s3["object"].get("eTag", "")
        async with self._sessionmaker() as session:
            await self._service(session).ingest(key, etag)

    async def _process_message(self, message: dict) -> None:
        body = json.loads(message["Body"])
        for record in body.get("Records", []):
            await self._process_record(record)

    async def poll_once(self) -> int:
        """Receive one batch; process + delete each. Returns messages handled.

        Also runs the pipeline's two housekeeping sweeps each pass: the durable
        rendition-cleanup queue (``catalog.image_reclaim``) and the reaper for
        presigned uploads that expired with no bytes ever arriving. Piggybacking on
        this loop keeps both retryable without a second service; each claims rows
        with ``FOR UPDATE SKIP LOCKED``, so replicas split the work.

        Images are decoded/re-encoded serially and CPU-heavy, so a batch of ten
        would routinely outrun even the queue's generous visibility timeout
        (``IMAGE_VISIBILITY_TIMEOUT_SECONDS``, 300s — sized for *one* worst-case
        image): messages would reappear mid-processing and burn redrive attempts
        until they hit the DLQ despite succeeding. One message per receive keeps the
        in-flight work inside that ceiling. Throughput is unchanged (processing was
        already serial) — scale by running more worker tasks, not bigger batches.
        """
        resp = await self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_messages,
            WaitTimeSeconds=self._wait,
        )
        handled = 0
        for message in resp.get("Messages", []):
            try:
                await self._process_message(message)
            except Exception:  # boundary: poison message stays for SQS redrive → DLQ
                log.exception("image processing failed; leaving message for redrive")
                continue
            await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
            handled += 1
        await self._housekeeping()
        return handled

    async def _housekeeping(self) -> None:
        """Run the cleanup + reaper sweeps; failures must never affect message handling."""
        try:
            async with self._sessionmaker() as session:
                service = self._service(session)
                await service.drain_reclaims()
                await service.reap_abandoned_uploads()
        except Exception:  # boundary: both sweeps are durable and simply retry next pass
            log.warning("image housekeeping sweep failed; work stays queued for retry", exc_info=True)

    async def run(self, stop: asyncio.Event) -> None:
        """Long-poll loop until ``stop`` is set.

        Transient receive/delete failures are retried with backoff rather than
        killing the worker — see :mod:`src.shared.bus.polling`.
        """
        await poll_forever(self.poll_once, stop, log)


async def run_worker(settings: AppSettings, sessionmaker: async_sessionmaker, valkey: Any, stop: asyncio.Event) -> None:
    """Build a real S3/SQS-backed worker from settings and run its loop."""
    if not settings.image_queue_url or not settings.s3_bucket:
        raise RuntimeError("IMAGE_QUEUE_URL and S3_BUCKET must be configured for the image worker")
    async with s3_client(settings) as s3, sqs_client(settings) as sqs:
        worker = ImageWorker(
            sqs,
            ImageStore(s3, settings.s3_bucket),
            valkey,
            sessionmaker,
            settings.image_queue_url,
            max_dimension=settings.image_max_dimension,
            max_bytes=settings.image_max_upload_bytes,
            max_pixels=settings.image_max_source_pixels,
            dedup_ttl_seconds=settings.consumer_dedup_ttl_seconds,
            upload_reaper_grace_seconds=settings.image_upload_reaper_grace_seconds,
            wait_time_seconds=settings.consumer_wait_time_seconds,
        )
        await worker.run(stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.catalog.adapters.image_worker` — the `service`-role image worker."""
    from src.shared.clients import valkey_client
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import serve_worker_metrics

    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="image-worker")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    valkey = valkey_client.create_client(settings)
    log.info("image worker starting (queue=%s)", settings.image_queue_url)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_worker(settings, sessionmaker, valkey, stop)
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
