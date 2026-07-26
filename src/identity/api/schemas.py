"""Identity wire schema — the response shape for the local user mirror.

``from_attributes`` lets the service build this straight off the domain
``User`` dataclass. No ``role`` field: roles are Keycloak realm claims, never a
local column (the local row only anchors FKs + the ``is_active`` mirror).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class UserResponse(BaseModel):
    """The local ``identity.users`` mirror as returned by the service layer."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    oidc_sub: str
    email: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class CreateUserRequest(BaseModel):
    """Admin request to provision a brand-new Keycloak account.

    No ``role`` field: Keycloak defaults new users to ``consumer`` — the app
    never accepts a role from request input (privilege-escalation guard).
    """

    email: str = Field(min_length=3, max_length=320)
    temporary_password: str = Field(min_length=8, max_length=128)


class CreateUserResponse(BaseModel):
    """The Keycloak ``sub`` of the newly created account."""

    sub: str
