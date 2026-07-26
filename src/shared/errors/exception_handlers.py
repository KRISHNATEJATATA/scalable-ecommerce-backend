"""
Global exception handlers for FastAPI.

Registers handlers that convert exceptions to the flat RFC 9457 Problem
Details shape defined in the API contract.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from src.shared.errors.error_builder import PROBLEM_CONTENT_TYPE, build_problem
from src.shared.errors.exceptions import (
    AuthenticationError,
    AuthorizationError,
    DependencyUnavailableError,
)

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


async def _dependency_unavailable_handler(_: Request, exc: DependencyUnavailableError) -> JSONResponse:
    logger.warning("Dependency unavailable: %s", exc.detail)
    return _problem_response(503, title="Service Unavailable", detail=exc.detail)


async def _http_exception_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    return _problem_response(exc.status_code, title=str(exc.detail))


async def _validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return _problem_response(
        422,
        title="Request validation failed",
        detail="One or more fields are invalid.",
        details=[{"loc": e.get("loc"), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()],
    )


async def _unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    # Boundary catch: never leak internals; the trace_id ties the log to the response.
    logger.exception("Unhandled exception: %s", type(exc).__name__)
    return _problem_response(500, title="Internal Server Error")


def register_exception_handlers(app: FastAPI) -> None:
    """Wire the RFC 9457 handlers onto the app (called from the app factory)."""
    app.add_exception_handler(AuthenticationError, _authentication_error_handler)
    app.add_exception_handler(AuthorizationError, _authorization_error_handler)
    app.add_exception_handler(DependencyUnavailableError, _dependency_unavailable_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
