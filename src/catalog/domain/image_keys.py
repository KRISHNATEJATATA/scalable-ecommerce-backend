"""Object-key layout for the image pipeline — pure domain knowledge.

The key scheme (not S3-specific) both the storage adapter and the application
ingest use-case share, so neither has to import the other. The ``product_id``
prefix lets the worker map an S3 event back to a product with no lookup table.

    uploads/{product_id}/{token}.bin        ← raw merchant upload (private)
    public/{product_id}/{token}.webp        ← re-encoded main image (CDN)
    public/{product_id}/{token}_{name}.webp ← thumbnails
"""

from __future__ import annotations

import uuid

UPLOAD_PREFIX = "uploads"
PUBLIC_PREFIX = "public"
# Processed public images are always re-encoded to this format/extension.
PUBLIC_IMAGE_EXT = "webp"


def new_upload_token() -> str:
    """Fresh opaque per-attempt upload token."""
    return uuid.uuid4().hex


def upload_key(product_id: uuid.UUID, token: str) -> str:
    """Private raw-upload key for a product + attempt token."""
    return f"{UPLOAD_PREFIX}/{product_id}/{token}.bin"


def public_main_key(product_id: uuid.UUID, token: str) -> str:
    """Public re-encoded main-image key for a product + token."""
    return f"{PUBLIC_PREFIX}/{product_id}/{token}.{PUBLIC_IMAGE_EXT}"


def public_thumb_key(product_id: uuid.UUID, token: str, name: str) -> str:
    """Public thumbnail key (``name`` is e.g. ``thumb_256``)."""
    return f"{PUBLIC_PREFIX}/{product_id}/{token}_{name}.{PUBLIC_IMAGE_EXT}"


def parse_upload_key(key: str) -> tuple[uuid.UUID, str] | None:
    """Extract ``(product_id, token)`` from an ``uploads/{id}/{token}.bin`` key.

    Returns ``None`` for anything not matching (e.g. a stray key or the worker's
    own ``public/`` write) so the worker can skip it instead of crashing.
    """
    parts = key.split("/")
    if len(parts) != 3 or parts[0] != UPLOAD_PREFIX:
        return None
    try:
        product_id = uuid.UUID(parts[1])
    except ValueError:
        return None
    token = parts[2].rsplit(".", 1)[0]
    return product_id, token
