"""S3 image-store adapter tests — the download contract the ingest depends on.

A fake aioboto3 client (no network, no LocalStack): what matters here is that the
adapter pins the read to the event's ETag, bounds it, and translates botocore
errors into the port's terminal/no-op exceptions.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from src.catalog.adapters.s3_images import ImageStore
from src.catalog.ports.storage import ObjectChangedError, ObjectNotFoundError, ObjectTooLargeError

KEY = "uploads/p/t.bin"


class FakeBody:
    """Mimics aiobotocore's ``StreamingBody``: ``__aenter__`` yields the *wrapped*
    aiohttp response, whose ``read()`` accepts no byte limit. Reading through the
    context manager's value would therefore silently drop the bound."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.requested: int | None = None
        self.exited = False

    async def read(self, amt: int | None = None) -> bytes:
        self.requested = amt
        return self._data if amt is None else self._data[:amt]

    async def __aenter__(self):
        return _UnboundedWrapped()

    async def __aexit__(self, *exc) -> None:
        self.exited = True


class _UnboundedWrapped:
    async def read(self) -> bytes:  # note: no ``amt`` — exactly like aiohttp's
        raise AssertionError("must not read through the context manager's value")


class FakeS3:
    def __init__(self, *, data: bytes = b"bytes", content_length: int | None = None, error: str | None = None) -> None:
        self._data = data
        self._content_length = len(data) if content_length is None else content_length
        self._error = error
        self.calls: list[dict] = []
        self.body = FakeBody(data)

    async def get_object(self, **params):
        self.calls.append(params)
        if self._error:
            raise ClientError({"Error": {"Code": self._error}}, "GetObject")
        return {
            "Body": self.body,
            "ContentType": "image/jpeg",
            "ContentLength": self._content_length,
            "ETag": '"abc123"',
        }

    async def head_object(self, **params):
        self.calls.append(params)
        if self._error:
            raise ClientError({"Error": {"Code": self._error}}, "HeadObject")
        return {"ContentLength": self._content_length, "ETag": '"abc123"'}


async def test_download_pins_ifmatch_and_normalises_the_etag():
    s3 = FakeS3()
    store = ImageStore(s3, "bucket")
    obj = await store.download(KEY, max_bytes=100, expected_etag="abc123")
    assert s3.calls[0]["IfMatch"] == '"abc123"'  # quoted for the header
    assert obj.etag == "abc123"  # unquoted back out, matching the S3 event record


async def test_download_without_an_expected_etag_sends_no_condition():
    s3 = FakeS3()
    await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert "IfMatch" not in s3.calls[0]


async def test_download_read_is_bounded():
    s3 = FakeS3(data=b"x" * 50)
    await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.requested == 101, "must ask for one byte past the cap, never unbounded"
    assert s3.body.exited, "the response context must still be released"


async def test_declared_oversize_is_rejected_before_reading():
    s3 = FakeS3(data=b"x" * 10, content_length=10_000)
    with pytest.raises(ObjectTooLargeError):
        await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.requested is None, "an oversize object must never be read"
    assert s3.body.exited, "rejecting must not leak the pooled connection"


async def test_understated_content_length_is_still_caught():
    s3 = FakeS3(data=b"x" * 500, content_length=10)  # object lies about its size
    with pytest.raises(ObjectTooLargeError):
        await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.exited, "rejecting must not leak the pooled connection"


@pytest.mark.parametrize("code", ["NoSuchKey", "404", "NotFound"])
async def test_missing_object_maps_to_terminal_not_found(code):
    with pytest.raises(ObjectNotFoundError):
        await ImageStore(FakeS3(error=code), "bucket").download(KEY, max_bytes=100)


@pytest.mark.parametrize("code", ["PreconditionFailed", "412"])
async def test_etag_mismatch_maps_to_object_changed(code):
    with pytest.raises(ObjectChangedError):
        await ImageStore(FakeS3(error=code), "bucket").download(KEY, max_bytes=100, expected_etag="abc")


async def test_permission_error_stays_loud_and_retryable():
    # Deliberately NOT mapped to not-found: a missing s3:ListBucket grant must
    # surface as a fault, not silently mark every image failed.
    with pytest.raises(ClientError):
        await ImageStore(FakeS3(error="AccessDenied"), "bucket").download(KEY, max_bytes=100)


async def test_exists_is_true_when_the_raw_upload_is_there():
    assert await ImageStore(FakeS3(), "bucket").exists(KEY) is True


@pytest.mark.parametrize("code", ["NoSuchKey", "404", "NotFound"])
async def test_exists_is_false_only_for_a_genuinely_missing_object(code):
    assert await ImageStore(FakeS3(error=code), "bucket").exists(KEY) is False


async def test_exists_reraises_permission_errors():
    # A 403 (no s3:ListBucket) must never be read as "no upload arrived" — the
    # reaper would then discard a valid image it could not see.
    with pytest.raises(ClientError):
        await ImageStore(FakeS3(error="AccessDenied"), "bucket").exists(KEY)
