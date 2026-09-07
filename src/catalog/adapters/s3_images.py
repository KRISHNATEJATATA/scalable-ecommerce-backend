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

from botocore.exceptions import ClientError

from src.catalog.domain.image_keys import new_upload_token, upload_key
from src.catalog.ports.storage import (
    DownloadedObject,
    ImageStorePort,
    ObjectChangedError,
    ObjectNotFoundError,
    ObjectTooLargeError,
    PresignedUpload,
)

# Long-lived, immutable cache: the object key is unique per upload (token), so a
# new image is a new key — the CDN never needs to invalidate.
CACHE_CONTROL = "public, max-age=31536000, immutable"

# Drain size for downloads: big enough that a max-size image is a handful of reads,
# small enough that one short-read round-trip never holds the whole cap in memory.
_DOWNLOAD_CHUNK = 64 * 1024


def _clean_etag(etag: str) -> str:
    """S3 quotes ETags in headers; the S3→SQS event record does not. Normalise."""
    return etag.strip().strip('"')


def _quote_etag(etag: str) -> str:
    """``If-Match`` takes a quoted entity tag, whatever form the caller has."""
    return f'"{_clean_etag(etag)}"'


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

    async def download(self, key: str, *, max_bytes: int, expected_etag: str | None = None) -> DownloadedObject:
        """Fetch an object's bytes (bounded), its claimed ``Content-Type`` and ETag.

        **Pinned to ``expected_etag`` when given.** The raw upload key is *mutable*
        — a presigned POST stays replayable for its whole TTL — so by the time a
        queued event is processed the key can already hold different bytes. Reading
        unconditionally would process content the event never described (and
        content-address it under the older event's version). ``IfMatch`` makes S3
        refuse that read: a 412 becomes :class:`ObjectChangedError`, and the newer
        bytes are handled by their own ``ObjectCreated`` event.

        **Bounded.** ``ContentLength`` is checked before reading, and the read
        drains to EOF in bounded chunks (asking one byte past the cap at the end),
        so an oversized (or lying) object can never be pulled into worker memory
        in full.

        A missing object is raised as the port's :class:`ObjectNotFoundError`, not a
        botocore error: it is terminal (the raw upload was lifecycle-expired or
        deleted), and the application layer must not depend on aioboto3 to say so.

        This relies on S3 answering a missing key with ``NoSuchKey``, which it only
        does when the caller holds ``s3:ListBucket`` on the bucket — without it S3
        returns ``AccessDenied`` (403) instead, which stays a retry and would redrive
        to the DLQ. The task role must grant `s3:ListBucket` (see ``docs/DEPLOYMENT.md``).
        403 is deliberately *not* mapped here: a genuine permissions fault must stay
        loud and retryable rather than silently marking images `failed`.
        """
        params: dict[str, Any] = {"Bucket": self._bucket, "Key": key}
        if expected_etag:
            params["IfMatch"] = _quote_etag(expected_etag)
        try:
            resp = await self._client.get_object(**params)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"NoSuchKey", "404", "NotFound"}:
                raise ObjectNotFoundError(key) from exc
            if code in {"PreconditionFailed", "412"}:
                raise ObjectChangedError(key) from exc
            raise
        # Every exit past this point must release the response, or a stream of
        # oversized objects would leak one pooled connection each until the client
        # pool is exhausted — so the body context wraps the size checks too.
        async with resp["Body"]:  # the context manager only releases the connection
            declared = resp.get("ContentLength")
            if declared is not None and declared > max_bytes:
                raise ObjectTooLargeError(f"{key} is {declared} bytes (cap {max_bytes})")
            # **Read to EOF in bounded chunks.** aiobotocore's ``StreamingBody.read(amt)``
            # delegates to aiohttp's ``StreamReader.read``, whose contract is "at most
            # amt bytes" — it returns whatever is currently buffered, not exactly amt
            # (botocore's sync reader blocks until amt or EOF; the async one does not).
            # One big ``read(max_bytes + 1)`` therefore truncates any body arriving
            # across more than one TCP fragment, and PIL then fails to decode the
            # partial image. Each read stays at most one byte past the remaining cap:
            # enough to *detect* an object bigger than a truthful-looking
            # ContentLength, never enough to blow up. Read via the StreamingBody
            # itself, never the context manager's value: aiobotocore's ``__aenter__``
            # returns the *wrapped* aiohttp response, whose ``read()`` takes no byte
            # limit — the bound would silently vanish.
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = await resp["Body"].read(min(_DOWNLOAD_CHUNK, max_bytes - total + 1))
                if not chunk:
                    break  # empty read = EOF (aiohttp contract)
                total += len(chunk)
                if total > max_bytes:
                    raise ObjectTooLargeError(f"{key} exceeds {max_bytes} bytes")
                chunks.append(chunk)
            data = b"".join(chunks)
        return DownloadedObject(data, resp.get("ContentType"), _clean_etag(resp.get("ETag", "")))

    async def put_bytes(self, key: str, data: bytes, *, content_type: str) -> None:
        await self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl=CACHE_CONTROL,
        )

    async def exists(self, key: str) -> bool:
        """Is there an object under ``key``? (``HEAD``, so no bytes are transferred.)

        Used by the abandoned-upload reaper to tell "the client never uploaded" from
        "the bytes are there but their event is delayed, backed up, or sitting in the
        DLQ" — reaping the latter would discard a perfectly valid image.

        Same ``s3:ListBucket`` requirement as :meth:`download`: without it S3 answers
        a missing key with 403, and 403 is deliberately not swallowed here — a
        permissions fault must fail loudly rather than being read as "no upload
        arrived", which is exactly the answer that destroys the merchant's image.
        """
        try:
            await self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
                return False
            raise
        return True

    async def delete(self, key: str) -> None:
        """Remove one object (used to reclaim renditions written for a stale event)."""
        await self._client.delete_object(Bucket=self._bucket, Key=key)
