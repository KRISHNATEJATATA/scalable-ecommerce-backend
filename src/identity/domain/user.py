"""Identity domain entity — pure Python, imports nothing outward.

Frozen slotted dataclass mirroring the ``identity.users`` row (the JIT-provisioned
local mirror of a Keycloak account). No ``role`` field: roles are Keycloak realm
claims, not a local column — the local row only anchors FKs + the ``is_active`` mirror.
Also holds ``DirectoryUser``, the read-side projection of a Keycloak directory entry.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class User:
    """The local ``identity.users`` mirror keyed by OIDC ``sub`` (immutable)."""

    id: uuid.UUID
    oidc_sub: str
    email: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DirectoryUser:
    """A Keycloak account as seen in the admin directory listing (immutable).

    Read live from the Admin API — not a local mirror row: a user who
    self-registered in Keycloak but was never JIT-provisioned still appears.
    """

    sub: str
    email: str | None
    enabled: bool
