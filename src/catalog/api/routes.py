"""Catalog HTTP routes — product CRUD + the shopper listing.

Routes stay thin: authenticate/authorize via dependencies, call the service, map
``None`` → 404. Writes are gated on the ``merchant`` realm role (``admin`` also
passes the gate); per-row ownership is enforced in the service, not here. The
caller's local ``users.id`` (``CurrentUserDep``) is bound as ``merchant_id`` — a
merchant can never spoof ownership by sending someone else's id. Reads require a
valid token but no particular role (any shopper may browse).

Conditional writes: product responses carry ``ETag: "<version>"``
and writes may opt in to ``If-Match`` — a mismatch answers 412 before any
state changes. Opt-in, never required: script consumers are not forced to
track the header (the optimistic-lock 409 remains the server-side backstop).
"""

from __future__ import annotations

import re
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status

from src.catalog.api.schemas import (
    ImagePresignRequest,
    ImagePresignResponse,
    ProductCreate,
    ProductResponse,
    ProductUpdate,
)
from src.catalog.application.service import CacheRead, CatalogService
from src.shared.api.query import reject_unknown_query_params
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

# Cache-outcome header on the single-product GET (see ``application.service.CacheOutcome``).
# Listed in the app's CORS ``expose_headers`` (src/app.py) — browsers hide custom
# response headers from page JS otherwise, which would defeat the whole header.
PRODUCT_CACHE_HEADER = "X-Cache"

# Every query param the listing understands; anything else is a 400 (see
# ``shared/api/query.py``) rather than a silently unfiltered page.
_LIST_QUERY_PARAMS = frozenset({"limit", "sort", "cursor", "category", "merchant_id", "search"})

# ``If-Match`` grammar this API accepts: ``*`` (any) or one quoted integer
# (``"<version>"`` — W/ weak prefixes and list values are not produced by any
# of this API's ETags, so they are rejected as malformed rather than guessed).
_IF_MATCH_RE = re.compile(r"^\"(\d+)\"$")


def etag_of(version: int) -> str:
    """The entity tag for a product version: the quoted integer (RFC 9110 form)."""
    return f'"{version}"'


def parse_if_match(header: str | None) -> int | None:
    """Parse ``If-Match`` into a version to compare, or ``None`` when no precondition.

    Compares the quoted integer numerically (so a zero-padded echo of a real
    version still matches — the API never issues leading zeros, and a padded
    tag can only ever name a version that exists). Repeated ``If-Match``
    header lines are not supported: Starlette serves only the first.

    ``None`` (absent), ``*`` (any version), or a matching ``"<n>"`` all pass;
    a malformed header is a 400 (the client is broken, not stale); anything
    else is the exact version the client read.
    """
    if header is None:
        return None
    if header.strip() == "*":
        return None
    match = _IF_MATCH_RE.match(header.strip())
    if match is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='If-Match must be a quoted integer (ETag) or "*"',
        )
    return int(match.group(1))


def set_etag(response: Response, product: ProductResponse) -> None:
    """Stamp the response's ``ETag`` from the product's aggregate version."""
    response.headers["ETag"] = etag_of(product.version)


@router.get("", response_model=PageResponse[ProductResponse])
async def list_products(
    request: Request,
    service: CatalogServiceDep,
    _principal: PrincipalDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    sort: Annotated[str, Query(description="field name, optional leading '-' for descending")] = "-created_at",
    cursor: str | None = None,
    category: str | None = None,
    merchant_id: uuid.UUID | None = None,
    search: Annotated[
        str | None,
        Query(max_length=200, description="Case-insensitive substring match over product name and description."),
    ] = None,
) -> PageResponse[ProductResponse]:
    """Keyset-paginated, filterable listing of live products."""
    reject_unknown_query_params(request, _LIST_QUERY_PARAMS)
    filters: dict[str, object] = {}
    if category is not None:
        filters["category"] = category
    if merchant_id is not None:
        filters["merchant_id"] = merchant_id
    return await service.list_products(
        PageParams(limit=limit, sort=sort, cursor=cursor), filters or None, search=search
    )


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: uuid.UUID, service: CatalogServiceDep, _principal: PrincipalDep, response: Response
) -> ProductResponse:
    """Fetch one live product, or 404. The response carries ``ETag: "<version>"``.

    Also stamps ``X-Cache: hit | miss | bypass`` — whether the read was served
    from the cache, drove the DB fill that (re)populated it, or the cache was
    out of the loop (fault/degraded). Absent when caching is disabled. The 404
    carries it too: the read that confirms an absent id is a miss, its
    negative-cached repeat a hit.
    """
    cache_read = CacheRead()
    product = await service.get_product(product_id, cache_read=cache_read)
    cache_header = {PRODUCT_CACHE_HEADER: cache_read.outcome.value} if cache_read.outcome is not None else None
    if product is None:
        # Same 404 as the other routes, but the cache outcome travels on it —
        # HTTPException headers survive the Problem-Details mapping.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product not found", headers=cache_header)
    set_etag(response, product)
    if cache_header is not None:
        response.headers.update(cache_header)
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
    response: Response,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> ProductResponse:
    """Update an owned product (emits ``ProductUpdated``); cross-merchant → 403, missing → 404.

    An ``If-Match: "<version>"`` header makes the write conditional: a stale
    version answers 412 (re-read and re-apply) before any state changes.
    """
    product = await service.update_product(
        product_id=product_id,
        merchant_id=caller.id,
        is_admin=principal.is_admin,
        patch=body,
        if_match=parse_if_match(if_match),
    )
    if product is None:
        raise _NOT_FOUND
    set_etag(response, product)
    return product


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def delete_product(
    product_id: uuid.UUID,
    service: CatalogServiceDep,
    caller: CurrentUserDep,
    principal: MerchantPrincipalDep,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> None:
    """Soft-delete an owned product (emits ``ProductDeleted``); cross-merchant → 403, missing → 404.

    Honours ``If-Match`` like the update: a stale version answers 412 and
    nothing is deleted.
    """
    deleted = await service.delete_product(
        product_id=product_id, merchant_id=caller.id, is_admin=principal.is_admin, if_match=parse_if_match(if_match)
    )
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
