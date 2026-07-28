"""aioboto3 S3 client factory.

Async-native to keep the presign (request path) and the image worker's
download/upload loops off the blocking path. ``s3_endpoint_url`` points at
LocalStack locally and is ``None`` in the cloud, where the ECS task role
supplies credentials (no keys in code).

Returns an async context manager: ``async with s3_client(settings) as s3``.
"""

from __future__ import annotations

import aioboto3

from src.shared.config.setting import AppSettings

_session = aioboto3.Session()


def s3_client(settings: AppSettings):
    """Async S3 client context manager (presign + worker download/upload)."""
    return _session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
    )
