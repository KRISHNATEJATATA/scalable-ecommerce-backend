"""Product name blank-guard : whitespace-only names are rejected, not normalized.

``min_length=1`` alone accepts ``"   "`` — a name made only of whitespace is not
a name. The guard rejects at the trust boundary (422); it never strips, so
``"  ok  "`` is accepted (and stored) verbatim.
"""

import pytest
from pydantic import ValidationError

from src.catalog.application.dto import ProductCreate, ProductUpdate

_BLANKS = ("   ", "\t\n")  # pass min_length=1; only the blank-guard can catch them


def test_create_rejects_whitespace_only_names():
    for blank in _BLANKS:
        with pytest.raises(ValidationError, match="name must not be blank"):
            ProductCreate(name=blank, price="1.00")


def test_create_empty_name_still_a_validation_error():
    # "" is caught earlier by min_length=1 — same 422 boundary, different message.
    with pytest.raises(ValidationError):
        ProductCreate(name="", price="1.00")


def test_create_accepts_padded_name_verbatim():
    assert ProductCreate(name="  ok  ", price="1.00").name == "  ok  "


def test_update_rejects_whitespace_only_names_but_allows_omitted():
    for blank in _BLANKS:
        with pytest.raises(ValidationError, match="name must not be blank"):
            ProductUpdate(name=blank)
    # Omitted stays legal — that is the patch contract (untouched field). The
    # patch must set something (an empty one is separately rejected), so assert
    # omission via a price-only patch: name stays None, no blank-guard raise.
    assert ProductUpdate(price="1.00").name is None
