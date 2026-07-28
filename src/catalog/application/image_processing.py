"""Pure, CPU-bound image processing for the upload pipeline.

No I/O, no SQLAlchemy, no FastAPI — just bytes → bytes so the image worker can
run it off the event loop with ``asyncio.to_thread`` (Pillow re-encode +
``python-magic`` sniff are both blocking/CPU-bound).

Two security jobs live here, and neither may be simplified away:
1. **Sniff the real bytes** (``python-magic``) and reject anything whose actual
   magic bytes aren't an allowed image type — the claimed content-type/extension
   from the client is never trusted (spoof defense).
2. **Re-encode** every image through Pillow to a fixed format, which strips EXIF
   and any trailing payload, and clamp oversized dimensions.

Run the self-check: ``python -m src.catalog.application.image_processing``.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image

from src.catalog.domain.image_keys import PUBLIC_IMAGE_EXT

# Real-bytes allow-list (what magic must report), independent of the claimed type.
ALLOWED_MIME: frozenset[str] = frozenset({"image/jpeg", "image/png", "image/webp"})
# Fixed output: WebP re-encode strips EXIF/trailing bytes and is CDN-friendly.
OUTPUT_FORMAT = "WEBP"
OUTPUT_EXT = PUBLIC_IMAGE_EXT
# Fixed thumbnail longest-side sizes (px).
THUMBNAIL_SIZES: dict[str, int] = {"thumb_256": 256, "thumb_64": 64}


class UnsupportedImageError(Exception):
    """Uploaded bytes are not an allowed image (spoofed type, corrupt, or too big)."""


@dataclass(frozen=True, slots=True)
class ProcessedImage:
    """Re-encoded main image + thumbnails, all WebP bytes. ``mime`` is the sniffed type."""

    mime: str
    width: int
    height: int
    main: bytes
    thumbnails: dict[str, bytes]


def sniff_mime(raw: bytes) -> str:
    """Return the MIME type of ``raw`` from its actual bytes (never the claimed type)."""
    import magic  # imported lazily so the module stays importable without libmagic

    return magic.from_buffer(raw, mime=True)


def _encode_webp(img: Image.Image, max_dimension: int) -> tuple[bytes, int, int]:
    """Clamp to ``max_dimension`` (longest side) and encode to WebP; returns (bytes, w, h)."""
    clamped = img.copy()
    clamped.thumbnail((max_dimension, max_dimension))  # in-place, preserves aspect ratio
    buf = io.BytesIO()
    # A fresh save with no exif/icc argument drops EXIF + trailing payloads.
    clamped.save(buf, format=OUTPUT_FORMAT, method=4)
    return buf.getvalue(), clamped.width, clamped.height


def process_image(raw: bytes, *, max_dimension: int, max_bytes: int, claimed_mime: str | None = None) -> ProcessedImage:
    """Sniff + re-encode + thumbnail. Raises ``UnsupportedImageError`` on any reject.

    The image is marked usable by the caller ONLY if this returns — a spoofed or
    oversized upload raises instead. When ``claimed_mime`` is given (the
    merchant-declared content-type persisted on the S3 object), the real sniffed
    type must match it: bytes whose actual type differs from what was claimed are
    rejected even if they are themselves a valid image (spoof defense — AC "real
    bytes don't match its claimed type").
    """
    if len(raw) > max_bytes:  # defense in depth; the S3 presign policy is the first gate
        raise UnsupportedImageError(f"upload exceeds {max_bytes} bytes")

    mime = sniff_mime(raw)
    if mime not in ALLOWED_MIME:
        raise UnsupportedImageError(f"sniffed type {mime!r} is not an allowed image")
    if claimed_mime is not None and mime != claimed_mime:
        raise UnsupportedImageError(f"sniffed type {mime!r} does not match claimed type {claimed_mime!r}")

    try:
        with Image.open(io.BytesIO(raw)) as opened:
            opened.load()
            rgb = opened.convert("RGB")
    except Exception as exc:  # boundary: any decode failure is a rejected upload
        raise UnsupportedImageError("could not decode image") from exc

    main, width, height = _encode_webp(rgb, max_dimension)
    thumbnails = {name: _encode_webp(rgb, size)[0] for name, size in THUMBNAIL_SIZES.items()}
    return ProcessedImage(mime=mime, width=width, height=height, main=main, thumbnails=thumbnails)


def _self_check() -> None:  # pragma: no cover - runnable smoke test
    """Assert EXIF strip, thumbnail emission, oversize + spoof rejection."""
    # A JPEG carrying EXIF → re-encoded WebP must have no EXIF and yield thumbnails.
    src = Image.new("RGB", (4000, 3000), (10, 20, 30))
    exif = Image.Exif()
    exif[0x010E] = "secret-camera-note"  # ImageDescription
    buf = io.BytesIO()
    src.save(buf, format="JPEG", exif=exif)
    jpeg_bytes = buf.getvalue()

    try:
        out = process_image(jpeg_bytes, max_dimension=2048, max_bytes=5_000_000)
    except ImportError:
        # No libmagic on this host (e.g. Windows dev box): skip magic-dependent asserts.
        print("SKIP magic asserts (libmagic unavailable); run inside the Docker image to exercise sniffing")
        return
    except UnsupportedImageError as exc:  # pragma: no cover - defensive
        raise AssertionError(f"a valid JPEG was rejected: {exc}") from exc

    assert out.mime == "image/jpeg", out.mime
    assert max(out.width, out.height) == 2048, (out.width, out.height)  # clamped
    assert set(out.thumbnails) == {"thumb_256", "thumb_64"}
    reencoded = Image.open(io.BytesIO(out.main))
    assert not dict(reencoded.getexif()), "EXIF was not stripped"

    # Spoofed / non-image bytes are rejected.
    try:
        process_image(b"not an image at all" * 10, max_dimension=2048, max_bytes=5_000_000)
    except UnsupportedImageError:
        pass
    else:
        raise AssertionError("non-image bytes were not rejected")

    # Real JPEG bytes but a mismatched CLAIMED type → rejected (spoof defense).
    try:
        process_image(jpeg_bytes, max_dimension=2048, max_bytes=5_000_000, claimed_mime="image/png")
    except UnsupportedImageError:
        pass
    else:
        raise AssertionError("claimed/sniffed mismatch was not rejected")

    print("OK image_processing self-check passed")


if __name__ == "__main__":  # pragma: no cover
    _self_check()
