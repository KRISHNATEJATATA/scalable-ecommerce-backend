"""Query-param guard: the 400 must name EVERY unknown param, not just the first."""

from typing import cast

import pytest
from starlette.datastructures import QueryParams
from starlette.requests import Request

from src.shared.api.query import reject_unknown_query_params
from src.shared.errors.exceptions import InvalidQueryParamError


class _Req:
    """Stand-in for a starlette Request (only ``query_params`` is read)."""

    def __init__(self, qs: str) -> None:
        self.query_params = QueryParams(qs)


def _request(qs: str) -> Request:
    """The guard only reads ``query_params``; the cast keeps both type checkers quiet."""
    return cast(Request, _Req(qs))


_ALLOWED = frozenset({"limit", "cursor"})


def test_single_unknown_param_keeps_kind_value_contract():
    with pytest.raises(InvalidQueryParamError) as exc:
        reject_unknown_query_params(_request("price=9"), _ALLOWED)
    assert (exc.value.kind, exc.value.value) == ("query", "price")


def test_all_unknown_params_are_named_in_the_400_detail():
    with pytest.raises(InvalidQueryParamError) as exc:
        reject_unknown_query_params(_request("a=1&b=2&c=3"), _ALLOWED)
    assert (exc.value.kind, exc.value.value) == ("query", "a, b, c")
    # _bad_request_handler renders str(exc) as the Problem detail: all three names.
    assert all(name in str(exc.value) for name in ("a", "b", "c"))
