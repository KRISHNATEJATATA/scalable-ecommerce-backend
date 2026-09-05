"""Payments HTTP routes — the async-confirmation webhook.

Routes stay thin: read the raw body (the signature is computed over exactly the
bytes the gateway sent — re-serializing would break verification), hand both to
the service, and map outcomes. The endpoint is deliberately **not** behind the
bearer-token dependency: its authentication *is* the HMAC signature over the
shared secret, verified in the service. A 204 answers applied *and*
idempotent-no-op deliveries alike (the provider treats both as delivered); an
unknown reference raises 404 so a misconfigured gateway environment is visible
instead of silently swallowed.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from src.payments.application.service import PaymentsService
from src.shared.container import get_payments_service

router = APIRouter(prefix="/payments", tags=["payments"])

PaymentsServiceDep = Annotated[PaymentsService, Depends(get_payments_service)]

#: Real webhook deliveries are a few hundred bytes of JSON; anything past this is
#: a misbehaving sender (or a memory-exhaustion probe). Checked on the declared
#: length *and* the bytes actually read, before HMAC, answering 413 either way —
#: the provider treats 4xx as delivered, so a stuck giant is dropped, not retried.
MAX_WEBHOOK_BODY_BYTES = 64 * 1024


@router.post("/webhook", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def payment_webhook(request: Request, service: PaymentsServiceDep) -> None:
    """Receive one gateway confirmation (``payment.succeeded`` / ``payment.failed``).

    Signed with ``X-Payment-Signature: sha256=<HMAC-SHA256(raw_body, secret)>``.
    Duplicate and out-of-order notifications answer 204 without changing state.
    Bodies over 64 KiB answer 413 before verification."""
    declared = request.headers.get("content-length")
    try:
        declared_too_big = declared is not None and int(declared) > MAX_WEBHOOK_BODY_BYTES
    except ValueError:
        declared_too_big = False  # garbage header: the actual-body check below still applies
    if declared_too_big:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"webhook body exceeds {MAX_WEBHOOK_BODY_BYTES} bytes",
        )
    body = await request.body()
    if len(body) > MAX_WEBHOOK_BODY_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"webhook body exceeds {MAX_WEBHOOK_BODY_BYTES} bytes",
        )
    await service.handle_webhook(body, request.headers.get("x-payment-signature"))
