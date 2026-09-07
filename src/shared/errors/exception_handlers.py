"""
Global exception handlers for FastAPI.

Registers handlers that convert exceptions to the flat RFC 9457 Problem
Details shape defined in the API contract.
"""

import logging
import uuid
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from sqlalchemy.orm.exc import StaleDataError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from src.shared.config.logging import request_id_ctx
from src.shared.errors.error_builder import PROBLEM_CONTENT_TYPE, build_problem
from src.shared.errors.exceptions import (
    AuthenticationError,
    AuthorizationError,
    CheckoutIdempotencyConflictError,
    ConcurrentUpdateError,
    DependencyUnavailableError,
    InsufficientStockError,
    InvalidCartOperationError,
    InvalidCursorError,
    InvalidPaymentMethodError,
    InvalidQueryParamError,
    InvalidReservationError,
    InvalidUploadError,
    KeycloakConflictError,
    KeycloakEntityNotFoundError,
    OrderStateConflictError,
    PaymentIdempotencyConflictError,
    ReservationConflictError,
    ReservationContendedError,
    StockBelowReservedError,
    UnknownPaymentRefError,
)
from src.shared.middleware.security import REQUEST_ID_HEADER

logger = logging.getLogger(__name__)


def _problem_response(status: int, title: str, **kwargs) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content=build_problem(status, title, **kwargs),
        media_type=PROBLEM_CONTENT_TYPE,
    )


async def _authentication_error_handler(_: Request, exc: AuthenticationError) -> JSONResponse:
    # RFC 9457 401 with the Bearer challenge so clients know how to authenticate.
    response = _problem_response(401, title="Unauthorized", detail=exc.detail)
    response.headers["WWW-Authenticate"] = "Bearer"
    return response


async def _authorization_error_handler(_: Request, exc: AuthorizationError) -> JSONResponse:
    return _problem_response(403, title="Forbidden", detail=exc.detail)


async def _bad_request_handler(_: Request, exc: InvalidQueryParamError | InvalidCursorError) -> JSONResponse:
    # Client-supplied sort/filter/cursor that fails the repo whitelist/codec is a
    # 400 (bad input), never a 500 — these surface via the list routes' query params.
    return _problem_response(400, title="Bad Request", detail=str(exc))


async def _detail_bad_request_handler(
    _: Request,
    exc: InvalidUploadError | InvalidReservationError | InvalidPaymentMethodError | InvalidCartOperationError,
) -> JSONResponse:
    # Requests rejected by server-side validation before they reach durable state:
    # an upload whose declared type/size fails policy, a reservation whose quantity
    # is non-positive, a payment token shaped like raw card data, a cart mutation
    # past its boundary limits. 400 with the exception's own detail — never a 500.
    return _problem_response(400, title="Bad Request", detail=exc.detail)


async def _insufficient_stock_handler(_: Request, exc: InsufficientStockError) -> JSONResponse:
    # 409, not 400: the request was well-formed, the *state* refused it. A retry
    # after the reaper frees an expired hold can legitimately succeed.
    return _problem_response(409, title="Conflict", detail=exc.detail)


async def _reservation_conflict_handler(_: Request, exc: ReservationConflictError) -> JSONResponse:
    # Also 409, but a different `title`: the line already holds a different qty,
    # so the caller must release the stale hold rather than wait for stock.
    return _problem_response(409, title="Reservation Conflict", detail=exc.detail)


async def _reservation_contended_handler(_: Request, exc: ReservationContendedError) -> JSONResponse:
    # 409, and deliberately NOT counted as an oversell block: exhausting the
    # reserve retries means concurrent holds kept colliding on this line —
    # transient pressure, not a stock answer.
    return _problem_response(409, title="Reservation Contention", detail=exc.detail)


async def _stock_below_reserved_handler(_: Request, exc: StockBelowReservedError) -> JSONResponse:
    # 409: the upsert was well-formed but the state refused it — units are still
    # held by live checkouts. Raise `on_hand` (or wait for the holds to release)
    # and retry; never a CHECK-violation 500.
    return _problem_response(409, title="Conflict", detail=exc.detail)


async def _payment_idempotency_conflict_handler(_: Request, exc: PaymentIdempotencyConflictError) -> JSONResponse:
    # 409: the key pins whatever it first charged — replaying it with a different
    # order/amount is a caller bug a real gateway would also refuse.
    return _problem_response(409, title="Idempotency Conflict", detail=exc.detail)


async def _checkout_idempotency_conflict_handler(_: Request, exc: CheckoutIdempotencyConflictError) -> JSONResponse:
    # 409: the Idempotency-Key pins the first checkout body — same key with a
    # different body is a caller bug, never a second order.
    return _problem_response(409, title="Idempotency Conflict", detail=exc.detail)


async def _order_state_conflict_handler(_: Request, exc: OrderStateConflictError) -> JSONResponse:
    # 409: the request was well-formed, the order lifecycle refused it (cancel a
    # paid order, checkout an empty cart). Caller-fixable, retryable after repair.
    return _problem_response(409, title="Conflict", detail=exc.detail)


async def _unknown_payment_ref_handler(_: Request, exc: UnknownPaymentRefError) -> JSONResponse:
    # 404, not a silent 202: a webhook for an unknown ref is a misconfiguration
    # (wrong gateway environment, forged body) that must be visible. The provider
    # re-delivers, so once the row exists it resolves.
    return _problem_response(404, title="Not Found", detail=exc.detail)


async def _concurrent_update_handler(_: Request, exc: ConcurrentUpdateError) -> JSONResponse:
    # 409: the optimistic lock (`version_id`) rejected a lost-update, which is the
    # guard working — a retryable client outcome, not the 500 boundary.
    return _problem_response(409, title="Conflict", detail=exc.detail)


async def _stale_data_handler(_: Request, exc: StaleDataError) -> JSONResponse:
    # Backstop: any module whose adapter forgets to translate SQLAlchemy's
    # optimistic-lock error still answers 409 rather than an opaque 500.
    #
    # WARNING, not INFO, and deliberately loud: reaching here is a defect either
    # way. Every adapter is supposed to translate its own conflicts, and
    # SQLAlchemy raises StaleDataError for a *second* reason — an ORM UPDATE/DELETE
    # that matched an unexpected row count with no versioning involved, which is
    # our bug and morally a 500. Answering 409 keeps a real lock conflict
    # retryable; this log line is what stops the other case hiding inside normal
    # contention. Alert on it.
    logger.warning("optimistic lock conflict reached the boundary untranslated (adapter bug?): %s", exc)
    return _problem_response(409, title="Conflict", detail=ConcurrentUpdateError().detail)


async def _dependency_unavailable_handler(_: Request, exc: DependencyUnavailableError) -> JSONResponse:
    logger.warning("Dependency unavailable: %s", exc.detail)
    return _problem_response(503, title="Service Unavailable", detail=exc.detail)


async def _keycloak_not_found_handler(_: Request, exc: KeycloakEntityNotFoundError) -> JSONResponse:
    # 404, not the 500 boundary: an admin acting on a sub/role Keycloak doesn't
    # have is a caller-fixable outcome (stale list, typo), not a server fault.
    return _problem_response(404, title="Not Found", detail=exc.detail)


async def _keycloak_conflict_handler(_: Request, exc: KeycloakConflictError) -> JSONResponse:
    # 409: e.g. account creation against an email Keycloak already has. The
    # caller picks a different address; a 500 here read as "broken server" and
    # paged on-call for what is user input.
    return _problem_response(409, title="Conflict", detail=exc.detail)


async def _http_exception_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    # RFC 9457: `title` is the status reason ("Not Found"), `detail` carries the
    # occurrence-specific text ("product not found"). Framework-raised headers
    # (e.g. Starlette's 401 challenge) ride along.
    title = _status_reason(exc.status_code)
    response = _problem_response(exc.status_code, title=title, detail=str(exc.detail))
    for name, value in (exc.headers or {}).items():
        response.headers[name] = value
    return response


def _status_reason(status: int) -> str:
    """The HTTP reason phrase as the Problem ``title`` (unknown codes → generic)."""
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Error"


async def _validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return _problem_response(
        422,
        title="Request validation failed",
        detail="One or more fields are invalid.",
        details=[{"loc": e.get("loc"), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()],
    )


async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    # Boundary catch: 5xx bodies are sanitized — a generic message, never the raw
    # exception text, SQL statement, or driver error; internals stay in the logs
    # (the RedactFilter scrubs secrets/PII there), tied to the trace_id. Dev may
    # opt into raw detail via the prod-refused `verbose_error_details` setting.
    #
    # This handler runs under Starlette's ServerErrorMiddleware, OUTSIDE
    # RequestIDMiddleware: by the time we get here, its `finally` has already
    # reset `request_id_ctx`, so the log line and `build_problem()` would stamp
    # an empty trace_id. Re-establish the context from the id the middleware
    # stashed on request.state (a fresh one for a request that died before the
    # middleware ran), and mirror it back on the response — a client-reported
    # 500 must be correlatable to its log record.
    request_id = getattr(request.state, "request_id", None) or uuid.uuid4().hex
    token = request_id_ctx.set(request_id)
    try:
        logger.exception("Unhandled exception: %s", type(exc).__name__)
        settings = request.app.state.settings
        detail = f"{type(exc).__name__}: {exc}" if settings.verbose_error_details else None
        response = _problem_response(500, title="Internal Server Error", detail=detail)
    finally:
        request_id_ctx.reset(token)
    response.headers[REQUEST_ID_HEADER] = request_id
    return response


def register_exception_handlers(app: FastAPI) -> None:
    """Wire the RFC 9457 handlers onto the app (called from the app factory)."""
    app.add_exception_handler(AuthenticationError, _authentication_error_handler)
    app.add_exception_handler(AuthorizationError, _authorization_error_handler)
    app.add_exception_handler(InvalidQueryParamError, _bad_request_handler)
    app.add_exception_handler(InvalidCursorError, _bad_request_handler)
    app.add_exception_handler(InvalidUploadError, _detail_bad_request_handler)
    app.add_exception_handler(InvalidReservationError, _detail_bad_request_handler)
    app.add_exception_handler(InvalidCartOperationError, _detail_bad_request_handler)
    app.add_exception_handler(InsufficientStockError, _insufficient_stock_handler)
    app.add_exception_handler(ReservationConflictError, _reservation_conflict_handler)
    app.add_exception_handler(ReservationContendedError, _reservation_contended_handler)
    app.add_exception_handler(StockBelowReservedError, _stock_below_reserved_handler)
    app.add_exception_handler(PaymentIdempotencyConflictError, _payment_idempotency_conflict_handler)
    app.add_exception_handler(CheckoutIdempotencyConflictError, _checkout_idempotency_conflict_handler)
    app.add_exception_handler(OrderStateConflictError, _order_state_conflict_handler)
    app.add_exception_handler(UnknownPaymentRefError, _unknown_payment_ref_handler)
    app.add_exception_handler(ConcurrentUpdateError, _concurrent_update_handler)
    app.add_exception_handler(StaleDataError, _stale_data_handler)
    app.add_exception_handler(DependencyUnavailableError, _dependency_unavailable_handler)
    app.add_exception_handler(KeycloakEntityNotFoundError, _keycloak_not_found_handler)
    app.add_exception_handler(KeycloakConflictError, _keycloak_conflict_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
