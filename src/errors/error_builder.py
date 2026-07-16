"""
RFC 9457 Problem Details builder.

Constructs standardized error responses following the API contract's flat
Problem Details shape:

    {"type", "status", "title", "detail", "trace_id", "details"}
"""

from typing import Any

from src.config.logging import request_id_ctx

PROBLEM_CONTENT_TYPE = "application/problem+json"


def build_problem(
    status: int,
    title: str,
    detail: str | None = None,
    type_: str = "about:blank",
    details: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return the one flat RFC 9457 Problem Details body used everywhere."""
    problem: dict[str, Any] = {
        "type": type_,
        "status": status,
        "title": title,
        "trace_id": request_id_ctx.get(),
    }
    if detail is not None:
        problem["detail"] = detail
    if details:
        problem["details"] = details
    return problem
