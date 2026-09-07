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
    context manager's value would therefore silently drop the bound. Like the real
    aiohttp-backed reader, ``read(amt)`` returns *at most* ``amt`` bytes — whatever
    is currently buffered — never blocking to fill the caller's request."""

    def __init__(self, data: bytes, *, fragment_size: int | None = None) -> None:
        self._data = data
        self._pos = 0
        # aiohttp buffering model: each read sees at most ``fragment_size`` bytes.
        # None = the whole body is buffered at once.
        self._fragment_size = fragment_size
        self.requested: list[int | None] = []
        self.exited = False

    async def read(self, amt: int | None = None) -> bytes:
        self.requested.append(amt)
        if amt is None:  # unbounded drain
            chunk, self._pos = self._data[self._pos :], len(self._data)
            return chunk
        buffered = self._fragment_size if self._fragment_size is not None else len(self._data)
        chunk = self._data[self._pos : self._pos + min(amt, buffered)]
        self._pos += len(chunk)
        return chunk

    async def __aenter__(self):
        return _UnboundedWrapped()

    async def __aexit__(self, *exc) -> None:
        self.exited = True


class _UnboundedWrapped:
    async def read(self) -> bytes:  # note: no ``amt`` — exactly like aiohttp's
        raise AssertionError("must not read through the context manager's value")


class FakeS3:
    def __init__(
        self,
        *,
        data: bytes = b"bytes",
        content_length: int | None = None,
        error: str | None = None,
        fragment_size: int | None = None,
    ) -> None:
        self._data = data
        self._content_length = len(data) if content_length is None else content_length
        self._error = error
        self.calls: list[dict] = []
        self.body = FakeBody(data, fragment_size=fragment_size)

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


async def test_download_reads_are_bounded():
    s3 = FakeS3(data=b"x" * 50)
    await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.requested[0] == 101, "must ask one byte past the cap to detect a lying ContentLength"
    assert all(a is not None and a <= 101 for a in s3.body.requested), "never read unbounded or past the cap"
    assert s3.body.exited, "the response context must still be released"


async def test_fragmented_body_is_read_to_eof_not_truncated():
    """Regression for the short-read bug: aiobotocore's ``read(amt)`` returns *at
    most* amt — whatever aiohttp has buffered — so a body arriving across multiple
    TCP fragments truncated every large upload into a PIL decode failure. The
    adapter must drain to EOF and reassemble the object whole."""
    data = bytes(range(256)) * 343  # 87_808 bytes, far more than one fragment
    s3 = FakeS3(data=data, fragment_size=4096)
    obj = await ImageStore(s3, "bucket").download(KEY, max_bytes=1_000_000)
    assert obj.data == data, "a short read must never truncate a multi-fragment body"
    assert len(s3.body.requested) > 2, "the fragmenting fake must actually force multiple reads"


async def test_object_exactly_at_the_cap_is_accepted():
    s3 = FakeS3(data=b"x" * 100)
    obj = await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert obj.data == b"x" * 100, "cap-sized objects must not trip the oversized guard"


async def test_declared_oversize_is_rejected_before_reading():
    s3 = FakeS3(data=b"x" * 10, content_length=10_000)
    with pytest.raises(ObjectTooLargeError):
        await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.requested == [], "an oversize object must never be read"
    assert s3.body.exited, "rejecting must not leak the pooled connection"


async def test_understated_content_length_is_still_caught():
    s3 = FakeS3(data=b"x" * 500, content_length=10)  # object lies about its size
    with pytest.raises(ObjectTooLargeError):
        await ImageStore(s3, "bucket").download(KEY, max_bytes=100)
    assert s3.body.requested[0] == 101, "the lie is caught by reading one byte past the cap"
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
