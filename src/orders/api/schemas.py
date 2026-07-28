"""Orders wire schemas — the public HTTP response shape for an order + its lines.

``OrderResponse`` / ``OrderItemResponse`` live in ``application.dto`` (layers
contract: application must not depend on api) and are re-exported here for
route type hints / OpenAPI.
"""

from __future__ import annotations

from src.orders.application.dto import OrderItemResponse as OrderItemResponse
from src.orders.application.dto import OrderResponse as OrderResponse
