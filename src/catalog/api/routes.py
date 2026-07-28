"""Catalog HTTP routes — product CRUD + the shopper listing.

Routes stay thin: authenticate/authorize via dependencies, call the service, map
``None`` → 404. Writes are gated on the ``merchant`` realm role (``admin`` also
passes the gate); per-row ownership is enforced in the service, not here. The
caller's local ``users.id`` (``CurrentUserDep``) is bound as ``merchant_id`` — a
merchant can never spoof ownership by sending someone else's id. Reads require a
valid token but no particular role (any shopper may browse).
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.catalog.api.schemas import (
    ImagePresignRequest,
    ImagePresignResponse,
    ProductCreate,
    ProductResponse,
    ProductUpdate,
)
from src.catalog.application.service import CatalogService
from src.shared.auth.dependencies import PrincipalDep, require_role
from src.shared.auth.principal import Principal
from src.shared.container import CurrentUserDep, get_catalog_service
from src.shared.db.pagination import DEFAULT_LIMIT, MAX_LIMIT, PageParams, PageResponse

router = APIRouter(prefix="/products", tags=["catalog"])

CatalogServiceDep = Annotated[CatalogService, Depends(get_catalog_service)]
# One shared merchant-gate dependency for every write route. As a route-level
# ``dependencies=[...]`` entry it only gates (create); as a parameter annotation it
# also injects the ``Principal`` (update/delete need ``is_admin``).
_merchant_principal = Depends(require_role("merchant", "admin"))
MerchantPrincipalDep = Annotated[Principal, _merchant_principal]

_NOT_FOUND = HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product not found")


@router.get("", response_model=PageResponse[ProductResponse])
async def list_products(
    service: CatalogServiceDep,
    _principal: PrincipalDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    sort: Annotated[str, Query(description="field name, optional leading '-' for descending")] = "-created_at",
    cursor: str | None = None,
    category: str | None = None,
    merchant_id: uuid.UUID | None = None,
) -> PageResponse[ProductResponse]:
    """Keyset-paginated, filterable listing of live products."""
    filters: dict[str, object] = {}
    if category is not None:
        filters["category"] = category
    if merchant_id is not None:
        filters["merchant_id"] = merchant_id
    return await service.list_products(PageParams(limit=limit, sort=sort, cursor=cursor), filters or None)


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(product_id: uuid.UUID, service: CatalogServiceDep, _principal: PrincipalDep) -> ProductResponse:
    """Fetch one live product, or 404."""
    product = await service.get_product(product_id)
    if product is None:
        raise _NOT_FOUND
    return product


@router.post(
    "", response_model=ProductResponse, status_code=status.HTTP_201_CREATED, dependencies=[_merchant_principal]
)
async def create_product(body: ProductCreate, service: CatalogServiceDep, caller: CurrentUserDep) -> ProductResponse:
    """Create a product owned by the authenticated merchant (emits ``ProductCreated``)."""
    return await service.create_product(merchant_id=caller.id, data=body)


@router.patch("/{product_id}", response_model=ProductResponse)
async def update_product(
    product_id: uuid.UUID,
    body: ProductUpdate,
    service: CatalogServiceDep,
    caller: CurrentUserDep,
    principal: MerchantPrincipalDep,
) -> ProductResponse:
    """Update an owned product (emits ``ProductUpdated``); cross-merchant → 403, missing → 404."""
    product = await service.update_product(
        product_id=product_id, merchant_id=caller.id, is_admin=principal.is_admin, patch=body
    )
    if product is None:
        raise _NOT_FOUND
    return product


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    product_id: uuid.UUID,
    service: CatalogServiceDep,
    caller: CurrentUserDep,
    principal: MerchantPrincipalDep,
) -> None:
    """Soft-delete an owned product (emits ``ProductDeleted``); cross-merchant → 403, missing → 404."""
    deleted = await service.delete_product(product_id=product_id, merchant_id=caller.id, is_admin=principal.is_admin)
    if not deleted:
        raise _NOT_FOUND


@router.post("/{product_id}/image:presign", response_model=ImagePresignResponse)
async def presign_product_image(
    product_id: uuid.UUID,
    body: ImagePresignRequest,
    service: CatalogServiceDep,
    caller: CurrentUserDep,
    principal: MerchantPrincipalDep,
) -> ImagePresignResponse:
    """Issue a short-TTL presigned upload for an owned product image.

    Validates ownership + content-type + size before minting the URL (not an open
    uploader); the image worker marks the product image usable only after the
    uploaded bytes pass sniff + re-encode. Cross-merchant → 403, missing → 404.
    """
    presigned = await service.presign_image_upload(
        product_id=product_id,
        merchant_id=caller.id,
        is_admin=principal.is_admin,
        content_type=body.content_type,
        content_length=body.content_length,
    )
    if presigned is None:
        raise _NOT_FOUND
    return ImagePresignResponse(
        url=presigned.url, fields=presigned.fields, key=presigned.key, expires_in=presigned.expires_in
    )
