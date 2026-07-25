"""Purpose-named domain exceptions raised below the HTTP boundary.

Repositories never import FastAPI; they raise these plain exceptions and a
later RFC 9457 handler (ticket 14) maps them to ``400`` Problem Details. Until
then, callers/tests assert the exception type directly.
"""


class InvalidCursorError(ValueError):
    """A pagination cursor could not be decoded (malformed/tampered base64url JSON)."""

    def __init__(self, cursor: str) -> None:
        super().__init__(f"Invalid pagination cursor: {cursor!r}")
        self.cursor = cursor


class InvalidQueryParamError(ValueError):
    """A sort/filter field is not on the repository's whitelist (column-name-injection guard)."""

    def __init__(self, kind: str, value: str) -> None:
        super().__init__(f"Unknown {kind} field: {value!r}")
        self.kind = kind
        self.value = value
