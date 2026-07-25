"""Keyset/cursor pagination machinery shared across module repositories.

See ``docs/adr/0003-keyset-cursor-pagination.md``. Holds the opaque cursor
codec, the ORM keyset helper, the whitelist gate, the ``Page[T]`` envelope, and
the ``PageParams`` query DTO — tightly coupled, so one module.

Two query builders consume this: ORM ``select()`` repos call
:func:`apply_keyset` + :func:`build_page`; the catalog raw-SQL hot path reuses
the cursor codec + :func:`build_page` but hand-writes its ``WHERE``/``ORDER BY``.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import cast, literal, tuple_
from sqlalchemy.sql import ColumnElement, Select

from src.shared.errors.exceptions import InvalidCursorError, InvalidQueryParamError

DEFAULT_LIMIT = 20
MAX_LIMIT = 100


class PageParams(BaseModel):
    """Query DTO for a keyset page.

    ``sort`` is a field name with an optional leading ``-`` for descending
    (default ``-created_at`` → newest first). The sign is stripped here; the
    field itself is validated against the repo's whitelist at query build.
    ``limit`` is hard-capped (rejected, not clamped) at :data:`MAX_LIMIT`.
    """

    limit: int = Field(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)
    sort: str = "-created_at"
    cursor: str | None = None

    @property
    def descending(self) -> bool:
        return self.sort.startswith("-")

    @property
    def sort_field(self) -> str:
        return self.sort[1:] if self.sort.startswith("-") else self.sort


@dataclass(slots=True)
class Page[T]:
    """One keyset page. ``next_cursor is None`` ⇒ last page (no total; keyset can't count cheaply)."""

    items: list[T]
    next_cursor: str | None


class PageResponse[T](BaseModel):
    """Wire envelope for a keyset page. Services build it from the internal
    :class:`Page` by mapping each item to its response schema and passing
    ``next_cursor`` through unchanged.
    """

    items: list[T]
    next_cursor: str | None = None


def encode_cursor(sort_value: Any, id_value: Any) -> str:
    """Opaque, unsigned base64url JSON of the last row's ``(sort_value, id)`` keyset tuple."""
    raw = json.dumps([str(sort_value), str(id_value)]).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    """Decode a cursor to ``(sort_value, id)`` strings; Postgres casts them back to column types.

    Any malformed/tampered value → :class:`InvalidCursorError`. Values stay
    strings on purpose: they're re-cast to the sort/id column types in SQL, so
    no client-supplied text ever reaches a column position untyped.
    """
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        parsed = json.loads(raw)
        # Reject anything but a 2-item list: `a, b = <dict>` iterates keys and
        # `a, b = "xy"` iterates chars, both silently "succeeding" on the wrong shape.
        if not isinstance(parsed, list) or len(parsed) != 2:
            raise ValueError("cursor must decode to a 2-item [sort_value, id] array")
        sort_value, id_value = parsed
    except Exception as exc:  # malformed base64 / JSON / wrong shape
        raise InvalidCursorError(cursor) from exc
    return str(sort_value), str(id_value)


def resolve_sort_column(sort_field: str, sort_map: Mapping[str, ColumnElement]) -> ColumnElement:
    """Map a sort field name to its column via the repo whitelist, or raise.

    The whitelist is the authoritative gate (not just an edge Pydantic
    ``Literal``): only mapped names reach the SQL, so an unlisted column can't
    be injected via the ``sort`` param.
    """
    try:
        return sort_map[sort_field]
    except KeyError:
        raise InvalidQueryParamError("sort", sort_field) from None


def check_filters(filters: Mapping[str, Any], allowed: frozenset[str]) -> None:
    """Reject any filter key not on the repo whitelist (column-name-injection guard)."""
    for key in filters:
        if key not in allowed:
            raise InvalidQueryParamError("filter", key)


def apply_keyset(
    stmt: Select,
    sort_col: ColumnElement,
    id_col: ColumnElement,
    params: PageParams,
    cursor: tuple[str, str] | None,
) -> Select:
    """Add the keyset ``WHERE`` (if a cursor), the ``(sort, id)`` ORDER BY, and ``LIMIT n+1``.

    Uses a row-value comparison ``(sort, id) < (v, i)`` so the ``id`` tiebreaker
    follows the sort direction — total ordering even when the sort field ties
    (the classic keyset dup/skip bug). The ``+1`` row is the has-next probe.
    """
    if cursor is not None:
        sort_val, id_val = cursor
        left = tuple_(sort_col, id_col)
        right = tuple_(cast(literal(sort_val), sort_col.type), cast(literal(id_val), id_col.type))
        stmt = stmt.where(left < right if params.descending else left > right)
    if params.descending:
        ordering = (sort_col.desc(), id_col.desc())
    else:
        ordering = (sort_col.asc(), id_col.asc())
    return stmt.order_by(*ordering).limit(params.limit + 1)


def build_page[T](items: list[T], params: PageParams, key_of: Callable[[T], tuple[Any, Any]]) -> Page[T]:
    """Trim the +1 probe row and, if more remain, encode ``next_cursor`` from the last kept item."""
    has_more = len(items) > params.limit
    kept = items[: params.limit]
    next_cursor: str | None = None
    if has_more and kept:
        sort_value, id_value = key_of(kept[-1])
        next_cursor = encode_cursor(sort_value, id_value)
    return Page(items=kept, next_cursor=next_cursor)


if __name__ == "__main__":
    # DB-free self-check of the codec + envelope + whitelist gates.
    import uuid
    from datetime import datetime

    pid = uuid.uuid4()
    ts = datetime.now()
    enc = encode_cursor(ts, pid)
    assert decode_cursor(enc) == (str(ts), str(pid))

    _dict_shaped = base64.urlsafe_b64encode(json.dumps({"a": 1, "b": 2}).encode()).decode()
    _two_char_str = base64.urlsafe_b64encode(json.dumps("xy").encode()).decode()
    for bad in (
        "!!!not-base64!!!",
        encode_cursor(ts, pid)[:-4],
        base64.urlsafe_b64encode(b'"x"').decode(),
        _dict_shaped,
        _two_char_str,
    ):
        try:
            decode_cursor(bad)
        except InvalidCursorError:
            pass
        else:
            raise AssertionError(f"expected InvalidCursorError for {bad!r}")

    sort_map: dict[str, ColumnElement] = {"created_at": literal(1)}
    assert resolve_sort_column("created_at", sort_map) is sort_map["created_at"]
    for kind, call in (
        ("sort", lambda: resolve_sort_column("price", sort_map)),
        ("filter", lambda: check_filters({"evil": 1}, frozenset({"status"}))),
    ):
        try:
            call()
        except InvalidQueryParamError as exc:
            assert exc.kind == kind
        else:
            raise AssertionError(f"expected InvalidQueryParamError({kind})")

    p = PageParams(limit=2)
    rows = [("a", 1), ("b", 2), ("c", 3)]  # 3 > limit 2 ⇒ has next
    page = build_page(rows, p, key_of=lambda r: (r[0], r[1]))
    assert page.items == [("a", 1), ("b", 2)]
    assert page.next_cursor == encode_cursor("b", 2)
    last = build_page([("a", 1)], p, key_of=lambda r: (r[0], r[1]))
    assert last.next_cursor is None

    try:
        PageParams(limit=101)
    except Exception:
        pass
    else:
        raise AssertionError("limit > 100 must be rejected, not clamped")

    print("pagination self-check ok")
