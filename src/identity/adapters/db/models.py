"""SQLAlchemy models for the ``identity`` schema.

Own declarative ``Base`` (own metadata) so this module's Alembic chain only
ever sees its own tables.
"""

import uuid

from sqlalchemy import Boolean, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.shared.db.mixins import OutboxMixin, TimestampMixin, outbox_unpublished_index

SCHEMA = "identity"


class Base(DeclarativeBase):
    pass


class User(Base, TimestampMixin):
    """JIT-provisioned local mirror of a Keycloak account.

    Keyed by the OIDC ``sub`` claim. Anchors FKs from other modules
    (``catalog.merchant_id``, ``orders.user_id``) as an id value only —
    Keycloak remains the identity/role authority.
    """

    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("oidc_sub"), {"schema": SCHEMA})

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    oidc_sub: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Outbox(Base, OutboxMixin):
    """Transactional outbox for identity-originated events (``UserCreated``, ``UserDeleted``, ...)."""

    __tablename__ = "outbox"
    __table_args__ = (outbox_unpublished_index("identity"), {"schema": SCHEMA})
