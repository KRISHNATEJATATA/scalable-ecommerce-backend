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

from src.catalog.domain.image_keys import PUBLIC_IMAGE_EXT, THUMBNAIL_SIZES
from src.catalog.domain.image_status import ImageStatus

# Fields that are optional-to-send but never nullable: they back NOT-NULL columns,
# so ``null`` is rejected at runtime (see ``ProductUpdate._reject_explicit_null``).
_NOT_NULLABLE_PATCH_FIELDS = ("name", "price")


def _patch_schema(schema: dict) -> None:
    """Make the generated ``ProductUpdate`` schema match what the API accepts.

    Pydantic renders ``str | None = None`` — the idiomatic "may be omitted" shape —
    as ``anyOf: [string, null]``, which advertises ``{"name": null}`` as valid when
    the runtime answers 422. Collapse those unions to the non-null branch and drop
    the ``null`` default, so the live ``/openapi.json`` and the hand-authored
    contract tell clients the same thing. ``minProperties`` rejects the no-op ``{}``
    patch that would otherwise emit a false ``ProductUpdated`` event.
    """
    schema["minProperties"] = 1
    for field in _NOT_NULLABLE_PATCH_FIELDS:
        prop = schema.get("properties", {}).get(field)
        if not prop:  # pragma: no cover - only reachable if a field is renamed
            continue
        variants = [v for v in prop.pop("anyOf", []) if v.get("type") != "null"]
        if len(variants) == 1:
            prop.update(variants[0])
        elif variants:
            prop["anyOf"] = variants
        if prop.get("default", ...) is None:
            del prop["default"]


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


def public_thumbnail_urls(image_key: str | None, image_status: ImageStatus, base: str | None) -> dict[str, str] | None:
    """Unsigned CDN URLs for a READY image's thumbnails, keyed by rendition name.

    The worker writes ``{token}_{name}.webp`` next to the main object for every
    entry in :data:`THUMBNAIL_SIZES`; without this the renditions exist but no
    client can discover them (the key convention is not part of the contract).
    Derived from the main key so there is still exactly one key layout.

    ``None`` unless the key actually carries the worker's ``.webp`` extension:
    ``READY`` alone doesn't structurally guarantee a worker-written key, and
    appending ``_thumb_256.webp`` to anything else would advertise URLs that 404.
    """
    if not image_key or not image_key.endswith(f".{PUBLIC_IMAGE_EXT}"):
        return None
    main = public_image_url(image_key, image_status, base)
    if main is None:
        return None
    stem = main.removesuffix(f".{PUBLIC_IMAGE_EXT}")
    return {name: f"{stem}_{name}.{PUBLIC_IMAGE_EXT}" for name in THUMBNAIL_SIZES}


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
    # Same-shape map of the worker's thumbnail renditions (``thumb_256``/``thumb_64``
    # → unsigned CDN URL), or ``null`` when there is no ready image. Advertised so
    # clients don't have to reconstruct the ``_{name}.webp`` key convention.
    image_thumbnail_urls: dict[str, str] | None = None


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

    An **empty** patch is rejected: it changes nothing but would still persist a
    ``ProductUpdated`` outbox row, so consumers would see a domain event for a
    state transition that never happened. ``name``/``price`` may be omitted but
    never sent as ``null`` — both the validator and the generated schema say so.
    """

    # ``json_schema_extra`` is a mutator (see :func:`_patch_schema`) so the live
    # ``/openapi.json`` matches the hand-authored contract: no empty patch, and no
    # ``null`` advertised for the two NOT-NULL-backed fields.
    model_config = ConfigDict(extra="forbid", json_schema_extra=_patch_schema)

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    category: str | None = Field(default=None, max_length=255)
    price: Decimal | None = Field(default=None, gt=0, max_digits=12, decimal_places=2)

    @model_validator(mode="after")
    def _reject_empty_patch(self) -> ProductUpdate:
        """An empty body is a no-op update — reject it rather than emit a false event.

        ``update_product`` always writes a ``ProductUpdated`` outbox row, so ``{}``
        would publish an event asserting a change that did not occur. Guard it at
        the trust boundary (422) instead of teaching every consumer to ignore it.
        """
        if not self.model_fields_set:
            raise ValueError("patch must set at least one field")
        return self

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
