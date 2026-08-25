"""Object-key layout for the image pipeline — pure domain knowledge.

The key scheme (not S3-specific) both the storage adapter and the application
ingest use-case share, so neither has to import the other. The ``product_id``
prefix lets the worker map an S3 event back to a product with no lookup table.

    uploads/{product_id}/{token}.bin                ← raw merchant upload (private)
    public/{product_id}/{token}_{ver}.webp          ← re-encoded main image (CDN)
    public/{product_id}/{token}_{ver}_{name}.webp   ← thumbnails

``ver`` is a **content** version (see :func:`content_version`), not just the upload
token: a presigned POST is replayable for its whole TTL, so the same token can carry
different bytes. Keying public objects by content makes those writes land on a new
key instead of overwriting a live, ``immutable``-cached image.
"""

from __future__ import annotations

import hashlib
import uuid

UPLOAD_PREFIX = "uploads"
PUBLIC_PREFIX = "public"
# Processed public images are always re-encoded to this format/extension.
PUBLIC_IMAGE_EXT = "webp"
# Fixed thumbnail names → longest-side size (px). Domain knowledge because it is
# part of the public key layout: the worker writes exactly these renditions and the
# product response advertises their URLs under the same names.
THUMBNAIL_SIZES: dict[str, int] = {"thumb_256": 256, "thumb_64": 64}


def new_upload_token() -> str:
    """Fresh opaque per-attempt upload token."""
    return uuid.uuid4().hex


def upload_key(product_id: uuid.UUID, token: str) -> str:
    """Private raw-upload key for a product + attempt token."""
    return f"{UPLOAD_PREFIX}/{product_id}/{token}.bin"


def content_version(raw: bytes) -> str:
    """Key-safe content version: a truncated SHA-256 of the bytes themselves.

    Deterministic, so a redelivery of the same object rebuilds the same public
    keys, while different bytes always get their own — that is what stops a
    replayed presigned POST from overwriting a live, ``immutable``-cached image.

    Hashed here rather than reusing S3's **ETag**, which for a single-part upload
    is an MD5: MD5 collisions are cheap to craft, so two deliberately-chosen files
    could share one public key and one could overwrite the other's renditions.
    Truncation to 128 bits keeps keys short while staying far out of reach of a
    (second-)preimage attack, which is the property that matters here — a
    *collision* between two attacker-chosen files is useless because the key is
    also scoped by ``product_id`` and the server-issued upload token.

    CPU-bound (a few ms per MiB): call it off the event loop.
    """
    return hashlib.sha256(raw).hexdigest()[:32]


def public_main_key(product_id: uuid.UUID, token: str, version: str) -> str:
    """Public re-encoded main-image key for a product + token + content version."""
    return f"{PUBLIC_PREFIX}/{product_id}/{token}_{version}.{PUBLIC_IMAGE_EXT}"


def public_thumb_key(product_id: uuid.UUID, token: str, version: str, name: str) -> str:
    """Public thumbnail key (``name`` is e.g. ``thumb_256``)."""
    return f"{PUBLIC_PREFIX}/{product_id}/{token}_{version}_{name}.{PUBLIC_IMAGE_EXT}"


def public_rendition_keys(main_key: str) -> list[str]:
    """Every public object written for one main key (the main image + its thumbnails).

    Derived from the main key rather than from a product id + token + version, so
    a key read back from the database (a *previous*, now-replaced image) can be
    reclaimed without re-deriving how it was built. ``[]`` for a key that isn't a
    worker-written ``.webp``, so a hand-set key can never drive blind deletes.
    """
    if not main_key.endswith(f".{PUBLIC_IMAGE_EXT}"):
        return []
    stem = main_key.removesuffix(f".{PUBLIC_IMAGE_EXT}")
    return [main_key, *(f"{stem}_{name}.{PUBLIC_IMAGE_EXT}" for name in THUMBNAIL_SIZES)]


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
