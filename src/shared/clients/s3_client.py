"""aioboto3 S3 client factory.

Async-native to keep the presign (request path) and the image worker's
download/upload loops off the blocking path. ``s3_endpoint_url`` points at
LocalStack locally and is ``None`` in the cloud, where the ECS task role
supplies credentials (no keys in code).

Returns an async context manager: ``async with s3_client(settings) as s3``.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, cast

import aioboto3

from src.shared.config.setting import AppSettings

_session = aioboto3.Session()


def s3_client(settings: AppSettings) -> AbstractAsyncContextManager[Any]:
    """Async S3 client context manager (presign + worker download/upload).

    Return-typed (aioboto3 ships no stubs, so the bare factory reads as
    unknown and every ``async with`` on it warns) — the object already *is*
    an async CM at runtime; this only writes down what the workers rely on.
    """
    return cast(
        "AbstractAsyncContextManager[Any]",
        _session.client("s3", endpoint_url=settings.s3_endpoint_url, region_name=settings.s3_region),
    )
