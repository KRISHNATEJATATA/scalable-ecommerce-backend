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
        max_messages: int = 10,
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
        self._max_messages = max_messages
        self._wait = wait_time_seconds

    async def _process_record(self, record: dict) -> None:
        """Ingest one S3 record via a per-message session + application service."""
        s3 = record.get("s3")
        if not s3:  # not an S3 record (e.g. s3:TestEvent) → nothing to do
            return
        key = unquote_plus(s3["object"]["key"])
        etag = s3["object"].get("eTag", "")
        async with self._sessionmaker() as session:
            service = ImageIngestService(
                CatalogRepository(session),
                self._store,
                self._valkey,
                max_dimension=self._max_dimension,
                max_bytes=self._max_bytes,
                max_pixels=self._max_pixels,
                dedup_ttl_seconds=self._ttl,
            )
            await service.ingest(key, etag)

    async def _process_message(self, message: dict) -> None:
        body = json.loads(message["Body"])
        for record in body.get("Records", []):
            await self._process_record(record)

    async def poll_once(self) -> int:
        """Receive one batch; process + delete each. Returns messages handled."""
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
        return handled

    async def run(self, stop: asyncio.Event) -> None:
        """Long-poll loop until ``stop`` is set."""
        while not stop.is_set():
            await self.poll_once()


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
            max_messages=settings.consumer_max_messages,
            wait_time_seconds=settings.consumer_wait_time_seconds,
        )
        await worker.run(stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.catalog.adapters.image_worker` — the `service`-role image worker."""
    from src.shared.clients import valkey_client
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging

    settings = get_settings()
    setup_logging(settings.log_level)
    engine = create_engine(settings)
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
