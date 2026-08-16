"""Make the generated OpenAPI describe the RFC 9457 errors the app actually returns.

FastAPI documents its own validation failure as ``422 application/json`` with an
``HTTPValidationError`` body, but every error here goes through
:mod:`src.shared.errors.exception_handlers` and comes back as
``application/problem+json`` with the flat Problem Details shape. A client
generated from ``/openapi.json`` would otherwise parse the wrong content type and
look for ``detail[]`` fields that are never sent.

Rewriting the generated document (rather than annotating every route with
``responses=``) keeps the guarantee global: a new route cannot forget to declare
it, because the error shape is a property of the app's handlers, not of the route.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from src.shared.errors.error_builder import PROBLEM_CONTENT_TYPE

# Mirrors the hand-authored contract's ``Problem`` schema and ``build_problem``.
PROBLEM_SCHEMA: dict[str, Any] = {
    "description": "RFC 9457 Problem Details (the one flat error shape).",
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "status": {"type": "integer"},
        "title": {"type": "string"},
        "detail": {"type": "string"},
        "trace_id": {"type": "string"},
        "details": {"type": "array", "items": {"type": "object"}},
    },
    "required": ["type", "status", "title"],
}
_PROBLEM_REF = {"$ref": "#/components/schemas/Problem"}
# FastAPI's stock validation-error models, unreachable once every 4xx/5xx is a Problem.
_UNUSED_SCHEMAS = ("HTTPValidationError", "ValidationError")


def use_problem_details_openapi(app: FastAPI) -> None:
    """Point ``app.openapi`` at a document whose error responses are Problems."""

    def openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema

        # Generate through FastAPI's own method rather than calling ``get_openapi``
        # with a hand-copied argument list: that list grows (``servers``,
        # ``webhooks``, ``openapi_tags``, ``separate_input_output_schemas``…) and
        # anything not copied is silently dropped from the published document the
        # day someone sets it on ``create_app``. Delegating keeps this module
        # responsible for exactly one thing — the error shape.
        schema = FastAPI.openapi(app)

        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["Problem"] = PROBLEM_SCHEMA

        for operations in schema.get("paths", {}).values():
            for operation in operations.values():
                if not isinstance(operation, dict):  # path-level "parameters", etc.
                    continue
                for status, response in operation.get("responses", {}).items():
                    if status.isdigit() and int(status) >= 400:
                        response["content"] = {PROBLEM_CONTENT_TYPE: {"schema": _PROBLEM_REF}}

        for name in _UNUSED_SCHEMAS:
            components.pop(name, None)

        app.openapi_schema = schema  # mutated in place; cache the finished document
        return schema

    app.openapi = openapi
