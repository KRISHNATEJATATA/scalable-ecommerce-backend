"""aioboto3 client factories for the SNS/SQS bus.

Async-native (aioboto3) to match the S3 path and keep the relay/consumer loops
non-blocking. ``bus_endpoint_url`` points at LocalStack locally and is ``None`` in
the cloud, where the ECS task role supplies credentials (no keys in code).

Each factory returns an async context manager — ``async with sns_client(settings)``.
"""

from __future__ import annotations

import aioboto3

from src.shared.config.setting import AppSettings

_session = aioboto3.Session()


def sns_client(settings: AppSettings):
    """Async SNS client context manager (publisher side)."""
    return _session.client("sns", endpoint_url=settings.bus_endpoint_url, region_name=settings.bus_region)


def sqs_client(settings: AppSettings):
    """Async SQS client context manager (consumer side)."""
    return _session.client("sqs", endpoint_url=settings.bus_endpoint_url, region_name=settings.bus_region)
