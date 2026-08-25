"""Port (Protocol) for catalog image object storage.

Implemented by ``adapters/s3_images.ImageStore`` (aioboto3 over S3/LocalStack)
and wired in ``src/shared/container.py``. Typed as a structural contract so the
presign use-case and the image worker depend on the abstraction, not aioboto3 —
and tests can inject an in-memory fake.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple, Protocol, TypedDict


class PresignedUpload(TypedDict):
    """The S3 presigned-POST envelope plus the upload's key and token.

    ``token`` is the opaque per-attempt id embedded in ``key``; the service
    persists it so the worker can reject a stale event for a superseded upload.
    """

    url: str
    fields: dict[str, str]
    key: str
    token: str


class DownloadedObject(NamedTuple):
    """Bytes actually fetched, plus the type claimed at upload and their ETag.

    ``etag`` is the ETag of *these* bytes (not of whatever the queued event
    described), so the caller can content-address what it just read.
    """

    data: bytes
    content_type: str | None
    etag: str


class ObjectNotFoundError(Exception):
    """The requested object is gone (expired by lifecycle, or already deleted).

    A *terminal* condition, unlike a transient S3 fault: retrying can never make
    the bytes reappear, so the ingest use-case marks the image ``failed`` instead
    of letting the message bounce through redrive into the DLQ forever.
    """


class ObjectChangedError(Exception):
    """The object no longer holds the bytes the event described (ETag mismatch).

    A presigned POST is replayable, so the raw upload key is mutable: by the time
    a queued event is processed the key may hold *newer* bytes. Processing those
    under the older event would attach content the event never described. The
    newer bytes have their own ``ObjectCreated`` event, so this is a no-op, not a
    retry.
    """


class ObjectTooLargeError(Exception):
    """The object is larger than the caller's byte cap (never fully read)."""


class ImageStorePort(Protocol):
    async def presign_upload(
        self, product_id: uuid.UUID, *, content_type: str, max_bytes: int, ttl_seconds: int
    ) -> PresignedUpload: ...

    async def download(self, key: str, *, max_bytes: int, expected_etag: str | None = None) -> DownloadedObject: ...

    async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None: ...

    async def exists(self, key: str) -> bool: ...

    async def delete(self, key: str) -> None: ...
