"""Identity mapper: ORM ``User`` row → domain ``User``.

Typed ``Any`` to avoid an application → adapters import (the row is duck-typed).
"""

from __future__ import annotations

from typing import Any

from src.identity.domain.user import User


def to_domain(row: Any) -> User:
    """Map an ORM ``User`` row to a domain ``User``."""
    return User(
        id=row.id,
        oidc_sub=row.oidc_sub,
        email=row.email,
        is_active=row.is_active,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
