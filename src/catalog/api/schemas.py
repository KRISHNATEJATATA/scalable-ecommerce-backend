"""Catalog wire schemas — the public HTTP response shape for a product.

The request/response DTOs shared with the service layer (``ProductResponse``,
``ProductCreate``, ``ProductUpdate``) live in ``application.dto`` (layers
contract: application must not depend on api) and are re-exported here for
route type hints / OpenAPI.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from src.catalog.application.dto import ProductCreate as ProductCreate
from src.catalog.application.dto import ProductResponse as ProductResponse
from src.catalog.application.dto import ProductUpdate as ProductUpdate


class ImagePresignResponse(BaseModel):
    """Presigned-POST envelope a merchant uses to upload one product image directly
    to S3. ``fields`` already carry the content-type + ``content-length-range``
    policy conditions, so an upload violating them is rejected by S3 itself.
    """

    url: str
    fields: dict[str, str]
    key: str
    expires_in: int


class ImagePresignRequest(BaseModel):
    """Merchant declares the intended upload; validated BEFORE a URL is issued."""

    model_config = ConfigDict(extra="forbid")

    content_type: str = Field(description="claimed image MIME type (jpeg/png/webp)")
    content_length: int = Field(gt=0, description="declared byte size; capped by policy")
