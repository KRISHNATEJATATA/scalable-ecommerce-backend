"""Catalog application DTOs — the service layer's input/output shapes.

Live in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports these for route type hints / OpenAPI.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator

from src.catalog.domain.image_status import ImageStatus
from src.shared.config.setting import get_settings


def _public_image_url(image_key: str | None, image_status: ImageStatus) -> str | None:
    """Unsigned CDN URL for a READY public product image (``None`` otherwise)."""
    if image_status != ImageStatus.READY or not image_key:
        return None
    settings = get_settings()
    base = settings.s3_public_base_url or (
        f"{settings.s3_endpoint_url}/{settings.s3_bucket}" if settings.s3_endpoint_url else None
    )
    if not base:
        # No public base configured → refuse to emit a broken ``None/<bucket>/<key>``
        # URL. Startup validation (AppSettings) fails-fast in prod; this guards any
        # remaining misconfiguration rather than serving a malformed link.
        raise RuntimeError("s3_public_base_url (or s3_endpoint_url) must be configured to serve product image URLs")
    return f"{base.rstrip('/')}/{image_key}"


class ProductResponse(BaseModel):
    """The public HTTP response shape for a product."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    description: str | None
    category: str | None
    price: Decimal
    image_key: str | None
    image_status: ImageStatus
    created_at: datetime
    updated_at: datetime

    @computed_field  # unsigned CDN URL; presigned URLs stay reserved for private assets
    @property
    def image_url(self) -> str | None:
        return _public_image_url(self.image_key, self.image_status)


class ProductCreate(BaseModel):
    """Merchant create payload. ``merchant_id`` is never accepted from input —
    it is bound from the authenticated caller (ownership can't be spoofed).
    ``image_key`` is never accepted either: images are set only by the image
    worker after the upload passes sniff + re-encode (see the presign endpoint).
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    category: str | None = Field(default=None, max_length=255)
    price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class ProductUpdate(BaseModel):
    """Partial merchant update — every field optional; unset fields are untouched.
    ``image_key`` is not updatable here (worker-owned; use the presign endpoint).
    """

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    category: str | None = Field(default=None, max_length=255)
    price: Decimal | None = Field(default=None, gt=0, max_digits=12, decimal_places=2)

    @model_validator(mode="after")
    def _reject_explicit_null(self) -> ProductUpdate:
        """A NOT-NULL column may be omitted (untouched) but never set to ``null``.

        ``name``/``price`` back NOT-NULL columns: an explicit ``null`` in the body
        would otherwise pass through ``exclude_unset`` and hit the DB as a NOT-NULL
        violation (500). Reject it at the trust boundary as a 422 instead.
        """
        for field in ("name", "price"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} may be omitted but not null")
        return self
