"""Port (Protocol) for catalog image object storage.

Implemented by ``adapters/s3_images.ImageStore`` (aioboto3 over S3/LocalStack)
and wired in ``src/shared/container.py``. Typed as a structural contract so the
presign use-case and the image worker depend on the abstraction, not aioboto3 —
and tests can inject an in-memory fake.
"""

from __future__ import annotations

import uuid
from typing import Protocol, TypedDict


class PresignedUpload(TypedDict):
    """The S3 presigned-POST envelope plus the upload's key and token.

    ``token`` is the opaque per-attempt id embedded in ``key``; the service
    persists it so the worker can reject a stale event for a superseded upload.
    """

    url: str
    fields: dict[str, str]
    key: str
    token: str


class ImageStorePort(Protocol):
    async def presign_upload(
        self, product_id: uuid.UUID, *, content_type: str, max_bytes: int, ttl_seconds: int
    ) -> PresignedUpload: ...

    async def download(self, key: str) -> tuple[bytes, str | None]: ...

    async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None: ...
