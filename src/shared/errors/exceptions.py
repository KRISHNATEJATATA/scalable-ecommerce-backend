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


class InvalidUploadError(ValueError):
    """A requested upload fails server-side validation before a presigned URL is issued
    (claimed content-type not allowed, or declared size over the policy cap) → 400."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class InvalidReservationError(ValueError):
    """A reservation request is malformed (e.g. a non-positive quantity) → 400.

    Distinct from :class:`InsufficientStockError` (409): nothing about the stock
    level would make this request valid. The DB's ``ck_reservations_qty_positive``
    is the backstop; this is the caller-facing rejection.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


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


class InsufficientStockError(Exception):
    """A reservation was rejected because free stock did not cover the request → 409.

    Raised by the losing side of a race for the last unit as well as by a plain
    out-of-stock request — from the caller's point of view they're the same
    outcome, and the atomic conditional decrement is what makes them identical.
    """

    def __init__(self, sku: str, qty: int) -> None:
        super().__init__(f"insufficient stock for {sku!r} (requested {qty})")
        self.sku = sku
        self.qty = qty
        self.detail = f"insufficient stock for {sku!r} (requested {qty})"


class ReservationConflictError(Exception):
    """An order line already holds a *different* quantity of this SKU → 409.

    Distinct from :class:`InsufficientStockError` on purpose. Both are 409s, but
    they mean opposite things and want opposite responses: insufficient stock is
    the inventory invariant working (retry later, or don't), while this is the
    caller contradicting itself — retrying the same line with a changed quantity.
    Conflating them would file caller bugs under the oversell counter and hide
    genuine contention behind noise.
    """

    def __init__(self, sku: str, held_qty: int, requested_qty: int) -> None:
        detail = f"reservation for {sku!r} already holds {held_qty}, cannot re-reserve {requested_qty}"
        super().__init__(detail)
        self.sku = sku
        self.held_qty = held_qty
        self.requested_qty = requested_qty
        self.detail = detail


class StockMutationError(Exception):
    """A guarded stock UPDATE matched an unexpected number of rows → 500, our bug.

    The stock counters contradict the reservation being transitioned (e.g. a
    release whose ``reserved -= qty`` matched nothing). Not a caller error and not
    recoverable by retrying — it means the invariant is already broken, so the
    transaction is rolled back and this surfaces loudly rather than committing a
    status flip and an event for stock that never moved.
    """


class DependencyUnavailableError(Exception):
    """An upstream dependency (e.g. Keycloak JWKS) is unreachable → 503, our failure."""

    def __init__(self, detail: str = "a required dependency is unavailable") -> None:
        super().__init__(detail)
        self.detail = detail
