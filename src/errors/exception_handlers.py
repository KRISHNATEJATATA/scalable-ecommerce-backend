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

from src.errors.error_builder import PROBLEM_CONTENT_TYPE, build_problem

logger = logging.getLogger(__name__)


def _problem_response(status: int, title: str, **kwargs) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content=build_problem(status, title, **kwargs),
        media_type=PROBLEM_CONTENT_TYPE,
    )


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
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
