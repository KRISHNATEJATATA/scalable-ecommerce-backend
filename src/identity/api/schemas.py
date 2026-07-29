"""Identity wire schema — the response shape for the local user mirror.

``UserResponse`` lives in ``application.dto`` (layers contract: application
must not depend on api) and is re-exported here for route type hints /
OpenAPI. No ``role`` field: roles are Keycloak realm claims, never a local
column (the local row only anchors FKs + the ``is_active`` mirror).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from src.identity.application.dto import UserResponse as UserResponse


class CreateUserRequest(BaseModel):
    """Admin request to provision a brand-new Keycloak account.

    No ``role`` field: Keycloak defaults new users to ``consumer`` — the app
    never accepts a role from request input (privilege-escalation guard). No
    password field either: the spec forbids credentials passing through this API;
    Keycloak drives password setup (``UPDATE_PASSWORD`` required action).
    """

    email: str = Field(min_length=3, max_length=320)


class CreateUserResponse(BaseModel):
    """The Keycloak ``sub`` of the newly created account."""

    sub: str
