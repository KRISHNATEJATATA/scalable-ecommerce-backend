"""Identity application DTO — the service layer's output shape.

Lives in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports this for route type hints / OpenAPI.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class UserResponse(BaseModel):
    """The local ``identity.users`` mirror as returned by the service layer."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    oidc_sub: str
    email: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class AdminUserResponse(BaseModel):
    """One Keycloak directory entry in the admin listing (service-built from live Admin-API data).

    Not the local mirror: ``sub`` is the Keycloak id, ``email`` may be ``None``
    (an account without an address), ``merchant_role`` is resolved live from
    Keycloak role mappings, and ``disabled`` mirrors Keycloak ``enabled=false``.
    """

    sub: str
    email: str | None
    merchant_role: bool
    disabled: bool
