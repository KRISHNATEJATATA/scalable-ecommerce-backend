"""Orders wire schemas — checkout input plus the public order shape.

``OrderResponse`` / ``OrderItemResponse`` live in ``application.dto`` (layers
contract: application must not depend on api) and are re-exported here for
route type hints / OpenAPI.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from src.orders.application.dto import OrderItemResponse as OrderItemResponse
from src.orders.application.dto import OrderResponse as OrderResponse


class CheckoutRequest(BaseModel):
    """Checkout payload: the cart is server-side, so only the gateway token travels.

    A token/ref from hosted checkout — never card data (a PAN-shaped value is
    rejected with 400, per the payments boundary). The max length keeps a
    hostile body from shipping megabytes toward the gateway and matches what a
    hosted-checkout token plausibly is.
    """

    model_config = ConfigDict(extra="forbid")

    payment_token: str = Field(min_length=1, max_length=512)
