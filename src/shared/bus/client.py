"""aioboto3 client factories for the SNS/SQS bus.

Async-native (aioboto3) to match the S3 path and keep the relay/consumer loops
non-blocking. ``bus_endpoint_url`` points at LocalStack locally and is ``None`` in
the cloud, where the ECS task role supplies credentials (no keys in code).

Each factory returns an async context manager — ``async with sns_client(settings)``.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, cast

import aioboto3

from src.shared.config.setting import AppSettings

_session = aioboto3.Session()


def sns_client(settings: AppSettings) -> AbstractAsyncContextManager[Any]:
    """Async SNS client context manager (publisher side).

    Return-typed (aioboto3 ships no stubs, so the bare factory reads as
    unknown and every ``async with`` on it warns) — the object already *is*
    an async CM at runtime; this only writes down what the workers rely on.
    """
    return cast(
        "AbstractAsyncContextManager[Any]",
        _session.client("sns", endpoint_url=settings.bus_endpoint_url, region_name=settings.bus_region),
    )


def sqs_client(settings: AppSettings) -> AbstractAsyncContextManager[Any]:
    """Async SQS client context manager (consumer side)."""
    return cast(
        "AbstractAsyncContextManager[Any]",
        _session.client("sqs", endpoint_url=settings.bus_endpoint_url, region_name=settings.bus_region),
    )
