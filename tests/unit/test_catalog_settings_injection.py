"""Catalog uses the settings injected by the container, not the global singleton.

``create_app(custom_settings)`` must be authoritative: the CDN base behind
``image_url`` and the presign upload ceiling both come from the injected config.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.catalog.application.service import CatalogService
from src.catalog.ports.repository import ProductRecord
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import InvalidUploadError

_NOW = datetime.now(UTC)
_DSN = "postgresql+asyncpg://u:p@localhost:5432/test"


def _row(image_key: str | None = "products/x.jpg", image_status: str = "ready"):
    return SimpleNamespace(
        id=uuid.uuid4(),
        merchant_id=uuid.uuid4(),
        name="widget",
        description=None,
        category=None,
        price=Decimal("9.99"),
        image_key=image_key,
        image_status=image_status,
        version_id=1,  # aggregate counter the emitted ProductUpdated is ordered by
        created_at=_NOW,
        updated_at=_NOW,
    )


class _Repo:
    def __init__(self, row):
        self.row = row

    async def get_product(self, product_id):
        return self.row

    async def set_image_pending(self, product, token, *, expires_at, outbox):
        self.expires_at = expires_at
        return self.row


class _ImageStore:
    def __init__(self):
        self.calls: list[dict] = []

    async def presign_upload(self, product_id, *, content_type, max_bytes, ttl_seconds):
        self.calls.append({"max_bytes": max_bytes, "ttl_seconds": ttl_seconds})
        return {"url": "http://s3/local", "fields": {}, "key": "k", "token": "t"}


def test_stand_in_row_satisfies_the_repository_port_contract():
    """The double must carry every field the use-cases read off a real row.

    ``ProductRecord`` is ``@runtime_checkable`` precisely so this is checkable: the
    repo runs no type checker, so without this assert a double that drops (say)
    ``version_id`` fails as an ``AttributeError`` deep inside a use-case — which is
    exactly how it failed before the port declared the shape.
    """
    assert isinstance(_row(), ProductRecord)


async def test_image_url_uses_injected_base_not_global_settings():
    row = _row()
    svc = CatalogService(_Repo(row), image_base_url="https://cdn.injected.test/")
    result = await svc.get_product(row.id)
    assert result is not None
    assert result.image_url == "https://cdn.injected.test/products/x.jpg"


async def test_image_url_none_when_image_not_ready():
    row = _row(image_key=None, image_status="none")
    svc = CatalogService(_Repo(row), image_base_url="https://cdn.injected.test")
    result = await svc.get_product(row.id)
    assert result is not None and result.image_url is None


async def test_presign_enforces_injected_limit_and_ttl():
    row = _row()
    store = _ImageStore()
    svc = CatalogService(
        _Repo(row),
        store,
        image_max_upload_bytes=100,
        image_upload_ttl_seconds=42,
    )
    args = dict(product_id=row.id, merchant_id=row.merchant_id, is_admin=False, content_type="image/jpeg")

    with pytest.raises(InvalidUploadError):
        await svc.presign_image_upload(**args, content_length=101)

    ticket = await svc.presign_image_upload(**args, content_length=100)
    assert ticket is not None and ticket.expires_in == 42
    assert store.calls == [{"max_bytes": 100, "ttl_seconds": 42}]


def test_settings_public_image_base_falls_back_to_endpoint_and_bucket():
    # _env_file=None + an explicit DSN: never read the developer's .env, so CI
    # (no .env) and a local run agree.
    def settings(**overrides) -> AppSettings:
        s3 = {"s3_public_base_url": None, "s3_endpoint_url": None, "s3_bucket": None}
        return AppSettings(_env_file=None, database_url=_DSN, **(s3 | overrides))

    assert settings(s3_public_base_url="https://cdn.test").image_public_base_url == "https://cdn.test"
    assert (
        settings(s3_endpoint_url="http://localhost:4566", s3_bucket="imgs").image_public_base_url
        == "http://localhost:4566/imgs"
    )
    assert settings().image_public_base_url is None
    # An endpoint without a bucket must not build "<endpoint>/None/<key>".
    assert settings(s3_endpoint_url="http://localhost:4566").image_public_base_url is None


def test_settings_presign_public_base_url_validation():
    """The re-host origin is spliced into presigned URLs verbatim — a value
    without scheme/netloc would mint unPOSTable URLs, and a path/query-bearing
    value would be silently mangled by the splice (extra components dropped);
    both are rejected at startup."""

    def settings(**overrides) -> AppSettings:
        return AppSettings(_env_file=None, database_url=_DSN, **overrides)

    assert settings(s3_presign_public_base_url=None).s3_presign_public_base_url is None
    assert (
        settings(s3_presign_public_base_url="http://localhost:4566").s3_presign_public_base_url
        == "http://localhost:4566"
    )
    for bad in (
        "localhost:4566",
        "ftp://x",
        "http://",
        "https://uploads.example.com/s3",  # path prefix would be silently dropped
        "http://host?x=1",
    ):
        with pytest.raises(ValueError):
            settings(s3_presign_public_base_url=bad)
