"""Inventory HTTP routes — the merchant/admin stock upsert.

The module's only external write surface: declare how many units of a SKU are
on hand. Routes stay thin — gate on the ``merchant`` role (``admin`` passes any
of), call the service, let the shared Problem-Details handlers map the service
errors. The SKU is a path string (max 64 — the column's length); the caller
JIT-provisions through ``CurrentUserDep`` like every other write route.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path

from src.inventory.api.schemas import InventoryResponse, StockUpsertRequest
from src.inventory.application.service import InventoryService
from src.shared.auth.dependencies import require_role
from src.shared.auth.principal import Principal
from src.shared.container import CurrentUserDep, get_inventory_service

router = APIRouter(prefix="/admin/inventory", tags=["inventory"])

InventoryServiceDep = Annotated[InventoryService, Depends(get_inventory_service)]
# Same merchant-gate shape as catalog's write routes; ``admin`` passes the any-of
# gate. Injected as a parameter (mirroring catalog) so the gate both gates and
# binds the verified principal.
_merchant_principal = Depends(require_role("merchant", "admin"))
MerchantPrincipalDep = Annotated[Principal, _merchant_principal]


@router.put("/{sku}", response_model=InventoryResponse)
async def upsert_stock(
    # The column is VARCHAR(64); the character class keeps typos/URL mischief
    # (spaces, slashes, control chars) at the 422 boundary instead of the DB.
    sku: Annotated[str, Path(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._:-]+$")],
    body: StockUpsertRequest,
    service: InventoryServiceDep,
    _caller: CurrentUserDep,
    _principal: MerchantPrincipalDep,
) -> InventoryResponse:
    """Declare the SKU's on-hand units (creates the row or re-points ``on_hand``).

    Idempotent per value. ``409`` when the row's live holds would exceed the
    new ``on_hand`` — reserved units belong to checkouts in flight and may not
    be erased.
    """
    return await service.upsert_stock(sku, body.on_hand)
