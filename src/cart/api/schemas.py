"""Cart wire schemas — the public HTTP shapes for the basket.

The request/response DTOs shared with the service layer live in
``application.dto`` (layers contract: application must not depend on api) and
are re-exported here for route type hints / OpenAPI.
"""

from __future__ import annotations

from src.cart.application.dto import CartAddItem as CartAddItem
from src.cart.application.dto import CartItemResponse as CartItemResponse
from src.cart.application.dto import CartResponse as CartResponse
from src.cart.application.dto import CartUpdateItem as CartUpdateItem
