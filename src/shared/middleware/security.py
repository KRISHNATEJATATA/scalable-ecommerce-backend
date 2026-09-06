"""Security-related ASGI middleware.

Security headers (HSTS, CSP, X-Content-Type-Options, X-Frame-Options),
request-id propagation, and proxy (X-Forwarded-*) handling. Wired in Phase 1/9.
"""

import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from src.shared.config.logging import request_id_ctx

REQUEST_ID_HEADER = "X-Request-ID"

# static header set is enough for now.
_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
    "Referrer-Policy": "no-referrer",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
}

# Relaxed CSP for the intentionally enabled non-production docs pages ONLY
# (Swagger UI / ReDoc load their bundles from the jsdelivr CDN and initialize
# with inline scripts). The API itself keeps the strict global policy — this is
# never applied in prod, where docs are disabled entirely.
_DOCS_CSP = (
    "default-src 'none'; "
    "script-src https://cdn.jsdelivr.net 'unsafe-inline'; "
    "style-src https://cdn.jsdelivr.net 'unsafe-inline'; "
    "img-src 'self' https://fastapi.tiangolo.com data:; "
    "font-src https://cdn.jsdelivr.net data:; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Assign/propagate a request id and expose it via contextvar + response header.

    The id is also stashed on ``request.state``: the 500 boundary handler runs
    under ``ServerErrorMiddleware``, *outside* this middleware, after the
    contextvar has been reset — it recovers the id from there so the sanitized
    500 body, its log line, and the response header all quote the same id.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex
        request.state.request_id = request_id
        token = request_id_ctx.set(request_id)
        try:
            response = await call_next(request)
        finally:
            request_id_ctx.reset(token)
        response.headers[REQUEST_ID_HEADER] = request_id
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach a fixed set of security headers to every response.

    ``docs_csp_paths`` lists the exact docs paths (``/docs``, ``/redoc``, …)
    that may receive the relaxed :data:`_DOCS_CSP` — pass them only when docs
    are actually served (non-prod). Every other response, and every response in
    prod, keeps the strict ``default-src 'none'`` policy.
    """

    def __init__(self, app, dispatch=None, *, docs_csp_paths: frozenset[str] = frozenset()) -> None:
        super().__init__(app, dispatch=dispatch)
        self._docs_csp_paths = docs_csp_paths

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        if request.url.path in self._docs_csp_paths:
            response.headers["Content-Security-Policy"] = _DOCS_CSP
        return response
