"""S3 (aioboto3) image object store — presign + worker download/upload.

The object-key layout lives in ``catalog.domain.image_keys`` (shared with the
application ingest use-case); this adapter only speaks aioboto3. The upload
prefix is what the S3→SQS ObjectCreated notification is scoped to, so only raw
uploads (never the worker's own ``public/`` writes) re-trigger the worker.
"""

from __future__ import annotations

import inspect
import uuid
from typing import Any

from src.catalog.domain.image_keys import new_upload_token, upload_key
from src.catalog.ports.storage import ImageStorePort, PresignedUpload

# Long-lived, immutable cache: the object key is unique per upload (token), so a
# new image is a new key — the CDN never needs to invalidate.
CACHE_CONTROL = "public, max-age=31536000, immutable"


async def _maybe_await(value: Any) -> Any:
    """aiobotocore exposes presign helpers as sync in some versions, coroutines in others."""
    return await value if inspect.isawaitable(value) else value


class ImageStore(ImageStorePort):
    """aioboto3 S3 adapter. ``client`` is an entered aioboto3 S3 client."""

    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    async def presign_upload(
        self, product_id: uuid.UUID, *, content_type: str, max_bytes: int, ttl_seconds: int
    ) -> PresignedUpload:
        """Presigned POST whose policy pins the content-type and caps the size.

        Generates the private upload key + token itself
        (``uploads/{product_id}/{token}.bin``) and pins ``Content-Type`` so the
        merchant's *claimed* type is persisted on the object — the worker later
        re-sniffs the bytes and rejects a mismatch. S3 rejects an upload that
        violates the ``content-length-range`` or ``Content-Type`` condition, so
        the endpoint is never an open uploader.
        """
        token = new_upload_token()
        key = upload_key(product_id, token)
        post = await _maybe_await(
            self._client.generate_presigned_post(
                Bucket=self._bucket,
                Key=key,
                Fields={"Content-Type": content_type},
                Conditions=[
                    {"Content-Type": content_type},
                    ["content-length-range", 1, max_bytes],
                ],
                ExpiresIn=ttl_seconds,
            )
        )
        return {"url": post["url"], "fields": post["fields"], "key": key, "token": token}

    async def download(self, key: str) -> tuple[bytes, str | None]:
        """Fetch an object's raw bytes and its stored (claimed) ``Content-Type``."""
        resp = await self._client.get_object(Bucket=self._bucket, Key=key)
        content_type = resp.get("ContentType")
        async with resp["Body"] as stream:
            return await stream.read(), content_type

    async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None:
        await self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl=CACHE_CONTROL,
        )
