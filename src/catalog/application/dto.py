"""Catalog application DTOs — the service layer's input/output shapes.

Live in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports these for route type hints / OpenAPI.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.catalog.domain.image_status import ImageStatus


def public_image_url(image_key: str | None, image_status: ImageStatus, base: str | None) -> str | None:
    """Unsigned CDN URL for a READY public product image (``None`` otherwise).

    ``base`` is injected (from the app's settings, via the container) rather than
    read from the global settings, so an app built with ``create_app(settings)``
    serves URLs from *that* config.
    """
    if image_status != ImageStatus.READY or not image_key:
        return None
    if not base:
        # No public base configured → refuse to emit a broken ``None/<bucket>/<key>``
        # URL. Startup validation (AppSettings) fails-fast in prod; this guards any
        # remaining misconfiguration rather than serving a malformed link.
        raise RuntimeError("s3_public_base_url (or s3_endpoint_url) must be configured to serve product image URLs")
    return f"{base.rstrip('/')}/{image_key}"


class ProductResponse(BaseModel):
    """The public HTTP response shape for a product."""

    # ``json_schema_serialization_defaults_required``: ``image_url`` has a default
    # (the service fills it) but is always present on the wire — keep the generated
    # response schema matching the hand-authored contract, which requires it.
    model_config = ConfigDict(from_attributes=True, json_schema_serialization_defaults_required=True)

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
    # Unsigned CDN URL; presigned URLs stay reserved for private assets. Set by the
    # service from the injected public base (see :func:`public_image_url`) — not a
    # computed field, so it can't reach for the global settings at serialization time.
    image_url: str | None = None


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
