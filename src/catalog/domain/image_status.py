"""Image pipeline lifecycle — one domain enum, not scattered string literals.

The single source of truth for the ``catalog.products.image_status`` values.
Subclasses ``str`` so it serializes as its plain value (``"ready"``) and compares
equal to the DB/wire string, while giving every layer a typed, greppable name
instead of a bare literal.
"""

from __future__ import annotations

from enum import StrEnum


class ImageStatus(StrEnum):
    """Lifecycle of a product's image, owned by the image worker.

    ``none`` (no image) → ``pending`` (presigned upload minted, bytes awaited)
    → ``ready`` (sniffed, re-encoded, thumbnails written, public key attached)
    or ``failed`` (bytes weren't a real allowed image, spoofed, or oversized).
    """

    NONE = "none"
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"


# Canonical allowed-value tuple for the DB CHECK constraint (single source).
IMAGE_STATUS_VALUES: tuple[str, ...] = tuple(s.value for s in ImageStatus)
