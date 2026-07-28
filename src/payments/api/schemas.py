"""Payments wire schema — the response shape for a payment row.

``PaymentResponse`` lives in ``application.dto`` (layers contract:
application must not depend on api) and is re-exported here for route type
hints / OpenAPI.
"""

from __future__ import annotations

from src.payments.application.dto import PaymentResponse as PaymentResponse
