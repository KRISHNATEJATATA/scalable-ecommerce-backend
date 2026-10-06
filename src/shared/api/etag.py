"""Shared ETag / If-Match helpers (optimistic concurrency preconditions).

Moved verbatim from the catalog routes (the precedent) so inventory PUT — and
any future conditional write — reuses the same grammar: ``ETag: "<version>"``
responses, ``If-Match: "*"`` (any) or one quoted integer (``"<n>"``). Weak
(``W/``) prefixes and list values are rejected as malformed (400) rather than
guessed. Absent or ``*`` means no precondition (``None``).
"""

from __future__ import annotations

import re

from fastapi import HTTPException, Response, status

# ``If-Match`` grammar this API accepts: ``*`` (any) or one quoted integer
# (``"<version>"`` — W/ weak prefixes and list values are not produced by any
# of this API's ETags, so they are rejected as malformed rather than guessed).
_IF_MATCH_RE = re.compile(r'^"(\d+)"$')


def etag_of(version: int) -> str:
    """The entity tag for a version: the quoted integer (RFC 9110 form)."""
    return f'"{version}"'


def parse_if_match(header: str | None) -> int | None:
    """Parse ``If-Match`` into a version to compare, or ``None`` when no precondition.

    Compares the quoted integer numerically (so a zero-padded echo of a real
    version still matches — the API never issues leading zeros, and a padded
    tag can only ever name a version that exists). Repeated ``If-Match``
    header lines are not supported: Starlette serves only the first.

    ``None`` (absent), ``*`` (any version), or a matching ``"<n>"`` all pass;
    a malformed header is a 400 (the client is broken, not stale); anything
    else is the exact version the client read.
    """
    if header is None:
        return None
    if header.strip() == "*":
        return None
    match = _IF_MATCH_RE.match(header.strip())
    if match is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='If-Match must be a quoted integer (ETag) or "*"',
        )
    return int(match.group(1))


def set_etag(response: Response, version: int) -> None:
    """Stamp the response's ``ETag`` from the aggregate version."""
    response.headers["ETag"] = etag_of(version)
