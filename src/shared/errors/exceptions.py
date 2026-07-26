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


class AuthenticationError(Exception):
    """The caller could not be authenticated (missing/invalid/expired token) → 401."""

    def __init__(self, detail: str = "authentication required") -> None:
        super().__init__(detail)
        self.detail = detail


class AuthorizationError(Exception):
    """The caller is authenticated but not permitted (role/ownership) → 403."""

    def __init__(self, detail: str = "not permitted") -> None:
        super().__init__(detail)
        self.detail = detail


class DependencyUnavailableError(Exception):
    """An upstream dependency (e.g. Keycloak JWKS) is unreachable → 503, our failure."""

    def __init__(self, detail: str = "a required dependency is unavailable") -> None:
        super().__init__(detail)
        self.detail = detail
