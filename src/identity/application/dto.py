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
