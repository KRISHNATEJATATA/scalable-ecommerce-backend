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

from fastapi import APIRouter, Depends, status

from src.identity.api.schemas import CreateUserRequest, CreateUserResponse, UserResponse
from src.identity.application.service import IdentityAdminService
from src.shared.auth.dependencies import require_role
from src.shared.auth.principal import Principal
from src.shared.container import CurrentUserDep, get_identity_admin_service

router = APIRouter(tags=["identity"])

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


@router.post(
    "/admin/users/{sub}/roles/merchant",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def grant_merchant(sub: str, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Grant the ``merchant`` realm role to a Keycloak user (admin only)."""
    await service.grant_merchant(sub)


@router.delete(
    "/admin/users/{sub}/roles/merchant",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def revoke_merchant(sub: str, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Revoke the ``merchant`` realm role from a Keycloak user (admin only)."""
    await service.revoke_merchant(sub)


@router.post(
    "/admin/users/{sub}/disable",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    dependencies=[_require_admin],
)
async def disable_user(sub: str, service: AdminServiceDep, _admin_user: CurrentUserDep) -> None:
    """Disable a user in Keycloak and mirror ``is_active=false`` locally (admin only)."""
    await service.disable_user(sub)


@router.get("/internal/whoami", status_code=status.HTTP_200_OK)
async def internal_whoami(principal: Annotated[Principal, Depends(require_role("service"))]) -> dict[str, str]:
    """Machine-to-machine health/identity check for the ``service`` role.

    Exercised by relay/worker clients (Phase 6+) authenticating with a Keycloak
    ``service`` machine-account token; confirms ``require_role`` resolves the
    role end-to-end with no DB hit. No ownership semantics — ``service`` bypasses
    per-resource ownership the same way ``admin`` bypasses role gates.
    """
    return {"sub": principal.sub, "role": "service"}
