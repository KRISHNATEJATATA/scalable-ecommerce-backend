"""Purpose-named domain exceptions raised below the HTTP boundary.

Repositories never import FastAPI; they raise these plain exceptions and a
later RFC 9457 handler maps them to ``400`` Problem Details. Until
then, callers/tests assert the exception type directly.
"""


class InvalidCursorError(ValueError):
    """A pagination cursor could not be used (malformed/tampered base64url JSON, or stale for
    the filter it is replayed under) → 400."""

    def __init__(self, cursor: str, *, detail: str | None = None) -> None:
        message = detail if detail is not None else f"Invalid pagination cursor: {cursor!r}"
        super().__init__(message)
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


class InvalidCartOperationError(ValueError):
    """A cart mutation fails server-side boundary limits → 400.

    Non-positive or over-cap quantities, or a new line past the max-items cap —
    the cart is client-input-shaped state and must not become a Valkey
    memory-amplification vector. Distinct from 404 (unknown product / absent
    line): nothing about the catalog would make *this* request valid.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class InvalidPaymentMethodError(ValueError):
    """The payment-method token is unusable → 400.

    Today's one producer: a token shaped like **raw card data** (a 12-21 digit
    PAN, optionally spaced/dashed). Card data must never reach the gateway
    adapter, let alone be stored (PCI SAQ-A) — a hosted-checkout token is the only
    accepted shape, so this rejects the misuse at the boundary.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class PaymentIdempotencyConflictError(Exception):
    """A charge replayed under an existing idempotency key with different order/amount → 409.

    Real gateways reject this exact mismatch (the key pins the first request's
    parameters); surfacing it keeps a buggy retry from silently paying the wrong
    amount for the wrong order while believing it resumed the original."""

    def __init__(self) -> None:
        detail = "this idempotency key was already used for a different order/amount"
        super().__init__(detail)
        self.detail = detail


class CheckoutIdempotencyConflictError(Exception):
    """A checkout replayed under an existing key with a different body → 409.

    The key pins the first request's body (payment token + cart snapshot hash):
    replaying it with the same body returns the stored response, replaying it
    with a different body is a caller bug, not a new order."""

    def __init__(self) -> None:
        detail = "this Idempotency-Key was already used with a different request body"
        super().__init__(detail)
        self.detail = detail


class OrderStateConflictError(Exception):
    """The order's state refuses the transition → 409.

    Cancelling a `paid`/`shipped` order, checking out an empty cart, or any
    other well-formed request the lifecycle rejects. A retry after fixing the
    caller-side state can legitimately succeed."""

    def __init__(self, detail: str = "the order's state does not allow this operation") -> None:
        super().__init__(detail)
        self.detail = detail


class UnknownPaymentRefError(Exception):
    """A webhook references no payment we issued → 404.

    Deliberately NOT swallowed as a 202-ack: a webhook for an unknown ref is a
    misconfiguration (wrong gateway environment, forged payload) that operators
    must see. The gateway will re-deliver; once the row exists it resolves."""

    def __init__(self, detail: str = "no payment matches this reference") -> None:
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


class StockBelowReservedError(Exception):
    """A stock upsert tried to set ``on_hand`` below the units already held → 409.

    The DB's ``ck_inventory_reserved_lte_on_hand`` is the backstop; the guarded
    upsert refuses first so the caller gets a fixable conflict (raise ``on_hand``
    or wait for holds to release) instead of a CHECK-violation 500. Reserved
    units belong to live checkouts — they may not be silently erased.
    """

    def __init__(self, sku: str, requested_on_hand: int, reserved: int) -> None:
        detail = f"on_hand {requested_on_hand} for {sku!r} is below the {reserved} unit(s) currently reserved"
        super().__init__(detail)
        self.sku = sku
        self.requested_on_hand = requested_on_hand
        self.reserved = reserved
        self.detail = detail


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


class ReservationContendedError(Exception):
    """The reservation lost its uniqueness race repeatedly — churn on that line, not
    stock pressure → 409, retry shortly.

    Distinct from :class:`InsufficientStockError` so the oversell counter stays an
    honest *stock* signal: exhausting the reserve retry loop means concurrent
    holds kept colliding on the same order line, which says nothing about whether
    free stock covered the request. Counting it as an oversell block would inflate
    exactly the metric the atomic decrement exists to keep meaningful.
    """

    def __init__(self, sku: str) -> None:
        detail = f"reservation for {sku!r} is under heavy contention; retry shortly"
        super().__init__(detail)
        self.sku = sku
        self.detail = detail


class ConcurrentUpdateError(Exception):
    """An optimistic-lock (``version_id``) conflict lost the race → 409, retryable.

    Two writers loaded the same aggregate and both tried to commit: SQLAlchemy's
    ``version_id_col`` guard makes the loser's ``UPDATE`` match zero rows and raise
    ``StaleDataError``. That is the oversell-style guard working, not a server
    fault, so it must not fall through to the 500 boundary handler — the caller can
    simply re-read and re-apply its patch. Adapters translate the SQLAlchemy error
    into this one so the ORM exception never leaks past the repository.

    **Scope:** this guards the read→write window *inside one request* — the
    server-side backstop. Across *sequential* requests, clients detect the
    stale read themselves via ``ETag``/``If-Match`` (412,
    :class:`PreconditionFailedError`), which exposes the same ``version_id``
    counter the ORM lock guards.
    """

    def __init__(self, resource: str = "resource") -> None:
        detail = f"{resource} was modified concurrently; re-read it and retry"
        super().__init__(detail)
        self.resource = resource
        self.detail = detail


class PreconditionFailedError(Exception):
    """A client ``If-Match`` precondition failed against the loaded aggregate — 412.

    The client based its write on a version that no longer matches the row:
    the client-side sibling of :class:`ConcurrentUpdateError` (that one catches
    the race *inside* the request via the ORM optimistic lock; this one catches
    a stale read *across* requests). The remedy is the same — re-read and
    re-apply.
    """

    def __init__(self) -> None:
        self.detail = "the product changed since it was read (If-Match mismatch); re-read it and re-apply"
        super().__init__(self.detail)


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


class KeycloakEntityNotFoundError(Exception):
    """The referenced Keycloak entity (user ``sub``, realm role) does not exist → 404.

    Raised instead of letting ``python-keycloak``'s raw error fall through to the
    500 boundary: an admin acting on an unknown/deleted account is a caller-fixable
    outcome, not a server fault.
    """

    def __init__(self, detail: str = "the referenced Keycloak entity does not exist") -> None:
        super().__init__(detail)
        self.detail = detail


class KeycloakConflictError(Exception):
    """Keycloak rejected the write because it contradicts existing state → 409.

    Today the one producer is account creation against an email/username that
    already exists — a retryable-by-correction outcome that must not surface as
    a permanent-looking 500.
    """

    def __init__(self, detail: str = "Keycloak state conflicts with this request") -> None:
        super().__init__(detail)
        self.detail = detail


class KeycloakInvalidRequestError(Exception):
    """Keycloak refused the payload itself (HTTP 400) → 400.

    Today the one producer is account creation against a malformed email the
    admin can fix (e.g. ``error-invalid-email``) — provider-refused input is
    caller-fixable and must not surface as a permanent-looking 500.
    """

    def __init__(self, detail: str = "Keycloak rejected this request") -> None:
        super().__init__(detail)
        self.detail = detail
