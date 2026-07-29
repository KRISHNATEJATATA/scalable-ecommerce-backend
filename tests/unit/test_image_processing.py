"""Image-processing unit tests.

Exercise the CPU-bound sniff + re-encode path directly (no S3/DB). Skipped where
``libmagic`` isn't installed (e.g. a Windows dev box); it IS present in the
Docker image the worker runs in and in CI Linux with libmagic1.
"""

from __future__ import annotations

import io

import pytest

try:
    import magic  # noqa: F401  (probe for libmagic; the module imports it lazily)
except ImportError:
    pytest.skip("libmagic not installed on this host", allow_module_level=True)

from PIL import Image  # noqa: E402

from src.catalog.application.image_processing import (  # noqa: E402
    THUMBNAIL_SIZES,
    UnsupportedImageError,
    process_image,
    sniff_mime,
)

_LIMITS = {"max_dimension": 2048, "max_bytes": 5_000_000, "max_pixels": 40_000_000}


def _jpeg_with_exif() -> bytes:
    img = Image.new("RGB", (4000, 3000), (12, 34, 56))
    exif = Image.Exif()
    exif[0x010E] = "secret-note"  # ImageDescription
    buf = io.BytesIO()
    img.save(buf, format="JPEG", exif=exif)
    return buf.getvalue()


def test_reencode_strips_exif_and_clamps_and_thumbnails():
    out = process_image(_jpeg_with_exif(), **_LIMITS)
    assert out.mime == "image/jpeg"
    assert max(out.width, out.height) == 2048  # clamped to max_dimension
    assert set(out.thumbnails) == set(THUMBNAIL_SIZES)
    reencoded = Image.open(io.BytesIO(out.main))
    assert reencoded.format == "WEBP"
    assert not dict(reencoded.getexif()), "EXIF must be stripped"
    for name, size in THUMBNAIL_SIZES.items():
        thumb = Image.open(io.BytesIO(out.thumbnails[name]))
        assert max(thumb.size) <= size


def test_spoofed_non_image_is_rejected():
    # Bytes whose real type isn't an allowed image → rejected regardless of any
    # claimed content-type/extension (spoof defense).
    with pytest.raises(UnsupportedImageError):
        process_image(b"%PDF-1.7 not really an image" * 20, **_LIMITS)


def test_claimed_type_mismatch_is_rejected():
    # Real JPEG bytes but the merchant claimed PNG at presign → rejected even
    # though the bytes are themselves a valid image (AC: real bytes must match
    # the claimed type).
    with pytest.raises(UnsupportedImageError):
        process_image(_jpeg_with_exif(), claimed_mime="image/png", **_LIMITS)


def test_claimed_type_match_is_accepted():
    out = process_image(_jpeg_with_exif(), claimed_mime="image/jpeg", **_LIMITS)
    assert out.mime == "image/jpeg"


def test_oversize_rejected_before_decode():
    with pytest.raises(UnsupportedImageError):
        process_image(b"x" * 10, max_dimension=2048, max_bytes=5, max_pixels=40_000_000)


def test_decompression_bomb_rejected_before_decode():
    # A valid image whose source dimensions exceed the pixel cap is rejected from
    # the header, before load() allocates the full raster (memory-exhaustion guard).
    with pytest.raises(UnsupportedImageError):
        process_image(_jpeg_with_exif(), max_dimension=2048, max_bytes=5_000_000, max_pixels=1000)


def test_sniff_reads_real_bytes():
    png = io.BytesIO()
    Image.new("RGB", (8, 8)).save(png, format="PNG")
    assert sniff_mime(png.getvalue()) == "image/png"
