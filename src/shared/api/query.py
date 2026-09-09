"""Shared HTTP query-param guard.

FastAPI silently **ignores** a query param a route doesn't declare, so an
unsupported filter (``?price=5``) would return an unfiltered page — answering a
different question than the one asked. Every list route runs its incoming keys
through :func:`reject_unknown_query_params` so the rejection lives in one place
as the next list route (orders, payments) arrives.
"""

from __future__ import annotations

from starlette.requests import Request

from src.shared.errors.exceptions import InvalidQueryParamError


def reject_unknown_query_params(request: Request, allowed: frozenset[str]) -> None:
    """Raise :class:`InvalidQueryParamError` (→ 400) naming *every* undeclared query param.

    The exception's ``value`` carries the sorted list joined on commas, so the
    rendered 400 detail answers once for the whole request — reporting only the
    first unknown param would mean the caller re-discovers the rest one 400 at a
    time. One rep per param per request is the contract: callers that split on
    ``", "`` see the exact keys to drop or whitelist.
    """
    unknown = sorted(set(request.query_params) - allowed)
    if unknown:
        raise InvalidQueryParamError("query", ", ".join(unknown))


if __name__ == "__main__":
    # DB-free self-check: known params pass; one unknown keeps the single-value
    # kind/value shape, and several unknowns arrive as one sorted comma list.
    from typing import cast

    from starlette.datastructures import QueryParams

    class _Req:
        def __init__(self, qs: str) -> None:
            self.query_params = QueryParams(qs)

    allowed = frozenset({"limit", "cursor"})
    reject_unknown_query_params(_Req("limit=5&cursor=abc"), allowed)  # type: ignore[arg-type]
    try:
        reject_unknown_query_params(_Req("limit=5&price=9"), allowed)  # type: ignore[arg-type]
    except InvalidQueryParamError as exc:
        assert (exc.kind, exc.value) == ("query", "price")
    else:
        raise AssertionError("expected InvalidQueryParamError for an undeclared param")

    try:
        reject_unknown_query_params(cast("Request", _Req("a=1&b=2&c=3")), allowed)
    except InvalidQueryParamError as exc:
        assert exc.kind == "query" and exc.value == "a, b, c"
    else:
        raise AssertionError("expected InvalidQueryParamError naming every unknown param")

    print("query-param guard self-check ok")
