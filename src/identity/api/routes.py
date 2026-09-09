"""Identity HTTP routes.

``GET /v1/me`` returns the caller's JIT-provisioned local mirror. The admin
endpoints manage Keycloak realm roles / enablement and are gated on the ``admin``
realm role. Admin routes also depend on ``CurrentUserDep`` (not just the role
gate) so the *acting* admin is itself JIT-provisioned/active-checked — an admin
token alone must not bypass local provisioning. Routes stay thin: verification
and provisioning live in the auth dependencies and services; roles are *never*
bound from request input.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request, status

from src.identity.api.schemas import AdminUserResponse, CreateUserRequest, CreateUserResponse, UserResponse
from src.identity.application.service import IdentityAdminService
from src.shared.api.query import reject_unknown_query_params
from src.shared.auth.dependencies import require_role
from src.shared.auth.principal import Principal
from src.shared.container import CurrentUserDep, get_identity_admin_service
from src.shared.db.pagination import DEFAULT_LIMIT, MAX_LIMIT, PageResponse

router = APIRouter(tags=["identity"])

# Keycloak subs are UUIDs; the tightest URL-safe bound before the value reaches
# python-keycloak's URL formatting (`..` traversal / `?` query injection).
_SUB_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
SubPath = Annotated[str, Path(min_length=1, max_length=64, pattern=_SUB_PATTERN)]

AdminServiceDep = Annotated[IdentityAdminService, Depends(get_identity_admin_service)]
_require_admin = Depends(require_role("admin"))


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: CurrentUserDep) -> UserResponse:
    """Return the authenticated caller's local user mirror (JIT-provisioned)."""
    return current_user


@router.post(
    "/admin/users",
    response_model=CreateUserResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[_require_admin],
)
async def create_user(
    body: CreateUserRequest, service: AdminServiceDep, _admin_user: CurrentUserDep
) -> CreateUserResponse:
    """Create a new Keycloak account (admin only); Keycloak defaults the role to ``consumer``."""
    sub = await service.create_user(body.email)
    return CreateUserResponse(sub=sub)


@router.get("/admin/users", response_model=PageResponse[AdminUserResponse], dependencies=[_require_admin])
async def list_admin_users(
    request: Request,
    service: AdminServiceDep,
    _admin_user: CurrentUserDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
    search: Annotated[
        str | None,
        Query(max_length=200, description="Case-insensitive substring match over username/email."),
    ] = None,
) -> PageResponse[AdminUserResponse]:
    """List the Keycloak directory (admin only) as a ``{items, next_cursor}`` page.

    Items carry ``{sub, email, merchant_role, disabled}`` resolved live from the
    Keycloak Admin API; ``search`` matches any part of username/email; unknown
    query params are a 400. No ``sort`` param — Keycloak's users endpoint has
    none (deviation documented in frontend-handoff).
    """
    reject_unknown_query_params(request, frozenset({"limit", "cursor", "search"}))
    return await service.list_users(limit=limit, cursor=cursor, search=search)


@router.post(
    "/admin/users/{sub}/roles/merchant",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def grant_merchant(sub: SubPath, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Grant the ``merchant`` realm role to a Keycloak user (admin only)."""
    await service.grant_merchant(sub)


@router.delete(
    "/admin/users/{sub}/roles/merchant",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def revoke_merchant(sub: SubPath, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Revoke the ``merchant`` realm role from a Keycloak user (admin only)."""
    await service.revoke_merchant(sub)


@router.post(
    "/admin/users/{sub}/disable",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def disable_user(sub: SubPath, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Disable a user in Keycloak and mirror ``is_active=false`` locally (admin only)."""
    await service.disable_user(sub)


@router.get("/internal/whoami", status_code=status.HTTP_200_OK)
async def internal_whoami(principal: Annotated[Principal, Depends(require_role("service"))]) -> dict[str, str]:
    """Machine-to-machine health/identity check for the ``service`` role.

    Exercised by relay/worker clients (Phase 6+) authenticating with a Keycloak
    ``service`` machine-account token; confirms ``require_role`` resolves the
    role end-to-end with no DB hit. No ownership semantics here: ``service`` is a
    plain role gate like any other — it grants no ownership bypass (only ``admin``
    bypasses ownership, and that bypass never extends to role gates).
    """
    return {"sub": principal.sub, "role": "service"}
