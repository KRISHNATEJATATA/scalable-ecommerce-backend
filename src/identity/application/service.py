"""Identity use-cases.

``IdentityService`` covers reads + JIT provisioning over the local user mirror
(maps repo rows through the domain to the ``UserResponse`` schema so the ORM row
never crosses the service boundary). ``IdentityAdminService`` wraps the Keycloak
Admin API (role grants + enable/disable), also mirroring ``is_active`` locally on
disable. No soft-delete filter: disabled users still resolve so FKs anchor and
JIT lookups find an existing row.
"""

from __future__ import annotations

import asyncio
import base64
import json
import uuid

from src.identity.application.dto import AdminUserResponse, UserResponse
from src.identity.application.mappers import to_domain
from src.identity.application.outbox import user_created_outbox
from src.identity.domain.user import DirectoryUser
from src.identity.ports.admin import IdentityAdminPort
from src.identity.ports.repository import IdentityRepositoryPort
from src.shared.db.pagination import PageResponse
from src.shared.errors.exceptions import InvalidCursorError


class IdentityService:
    """Read-side use-cases + JIT provisioning over the local user mirror."""

    def __init__(self, repo: IdentityRepositoryPort) -> None:
        self._repo = repo

    async def get_by_oidc_sub(self, oidc_sub: str) -> UserResponse | None:
        """Resolve a user by their OIDC ``sub``, or ``None`` if absent."""
        row = await self._repo.get_by_oidc_sub(oidc_sub)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))

    async def get_by_id(self, user_id: uuid.UUID) -> UserResponse | None:
        """Resolve a user by primary key, or ``None`` if absent."""
        row = await self._repo.get_by_id(user_id)
        if row is None:
            return None
        return UserResponse.model_validate(to_domain(row))

    async def get_or_create_by_sub(self, oidc_sub: str, email: str) -> UserResponse:
        """JIT-provision (or fetch) the local mirror for a verified caller.

        A genuine insert also writes ``UserCreated`` to the identity outbox in the
        same transaction (the repository emits it only when the row was inserted).
        """
        row = await self._repo.get_or_create(oidc_sub, email, user_created_outbox)
        return UserResponse.model_validate(to_domain(row))


class IdentityAdminService:
    """Admin use-cases: manage Keycloak roles/enablement (admin-gated at the route)."""

    def __init__(self, repo: IdentityRepositoryPort, admin: IdentityAdminPort) -> None:
        self._repo = repo
        self._admin = admin

    async def grant_merchant(self, oidc_sub: str) -> None:
        """Grant the ``merchant`` realm role in Keycloak (the role authority)."""
        await self._admin.grant_realm_role(oidc_sub, "merchant")

    async def revoke_merchant(self, oidc_sub: str) -> None:
        """Revoke the ``merchant`` realm role in Keycloak."""
        await self._admin.revoke_realm_role(oidc_sub, "merchant")

    async def disable_user(self, oidc_sub: str) -> None:
        """Disable in Keycloak and mirror ``is_active=false`` locally.

        Ordered to **fail closed**: read Keycloak first (a failed lookup has mutated
        nothing, so the retry is clean), then write the local mirror, then disable in
        Keycloak. If that last call fails the account is already blocked here — the
        opposite order would leave a Keycloak-disabled account whose still-valid
        access tokens keep working against an active local mirror.

        The mirror write runs under the per-``oidc_sub`` advisory lock that JIT
        provisioning also takes, held from before the Keycloak read until the write
        commits. A concurrent first request therefore blocks *before* it can
        provision and resumes to find the row already disabled, instead of briefly
        being served as an active user. The write itself is one upsert (provisioning
        the row already disabled), so there is no read-then-write window inside the
        lock either; without an email there is nothing to provision, so fall back to
        flipping an existing row.

        ponytail: the lock is held across one Keycloak round-trip — bounded by the
        admin client's timeout, and disable is a rare admin-only call. If that ever
        stalls JIT, cache the email or take the lock only around the mirror write and
        accept that an in-flight request sees the pre-disable state.
        """
        await self._repo.lock(oidc_sub)
        email = await self._admin.get_user_email(oidc_sub)
        if email is None:
            await self._repo.set_active(oidc_sub, False)
        else:
            # No ``UserCreated``: a provisioned row here exists only to carry the disable.
            await self._repo.get_or_create(oidc_sub, email, is_active=False)
        await self._admin.set_enabled(oidc_sub, False)

    async def create_user(self, email: str) -> str:
        """Create a new Keycloak account and return its ``sub`` (admin only).

        No credential passes through this API (spec: the API never sees passwords) —
        Keycloak drives password setup via an ``UPDATE_PASSWORD`` required action.
        """
        return await self._admin.create_user(email)

    async def list_users(
        self, *, limit: int, cursor: str | None, search: str | None
    ) -> PageResponse[AdminUserResponse]:
        """List the Keycloak directory (admin-gated at the route) as a cursor page.

        No ``sort`` param: Keycloak's users endpoint has none — directory order is
        Keycloak's. Roles are read live from Keycloak role mappings, so items are
        always role-current (unlike token claims); ``disabled`` mirrors Keycloak
        ``enabled=false``. ``search`` is substring (contains) over username/email —
        the admin adapter wraps the term in Keycloak's ``*term*`` infix wildcard
        (bare ``search`` is prefix-only).

        Has-more is a ``limit + 1`` probe: one extra row is requested from
        Keycloak and trimmed before role enrichment, so a page that exactly
        fills the limit is terminal — no phantom empty next page. The cursor
        pins the walk: it carries the issuing ``limit`` and ``search``, and a
        resumed page is served at the pinned limit (the request's ``limit``
        only applies when starting a fresh walk, ``cursor=None``); replaying a
        cursor under a different ``search`` re-partitions the walk, so it is a
        stale cursor → :class:`InvalidCursorError` (400). Legacy
        ``{"offset": n}`` cursors (pre-pinning) decode leniently as unpinned —
        the request's limit/search then apply, as before.
        """
        offset = 0
        pinned_limit: int | None = None
        pinned_search: str | None = None
        if cursor is not None:
            offset, pinned_limit, pinned_search = _decode_offset_cursor(cursor)
            if pinned_limit is not None and pinned_search != search:
                # A pinned cursor replayed under a different search re-partitions the
                # walk — the silent re-partition IS the bug; an explicit 400 tells the
                # client to restart the walk for the new filter.
                raise InvalidCursorError(
                    cursor, detail="cursor was issued for a different search filter; restart the walk"
                )
        # The cursor pins the walk: a resumed page is served at the pinned limit
        # (the request's limit only applies to fresh walks and legacy cursors).
        page_limit = pinned_limit if pinned_limit is not None else limit
        # limit+1 probe: the extra row only signals has-more — trim before the
        # role-enrichment gather so the probe row costs no Admin-API role lookups.
        users = await self._admin.list_users(search, offset, page_limit + 1)
        has_more = len(users) > page_limit
        users = users[:page_limit]
        sem = asyncio.Semaphore(_ROLE_GATHER_CONCURRENCY)

        async def _is_merchant(user: DirectoryUser) -> bool:
            async with sem:
                return await self._admin.has_realm_role(user.sub, "merchant")

        flags = await asyncio.gather(*(_is_merchant(u) for u in users))
        items = [
            AdminUserResponse(sub=u.sub, email=u.email, merchant_role=flag, disabled=not u.enabled)
            for u, flag in zip(users, flags, strict=True)
        ]
        next_cursor = _encode_offset_cursor(offset + page_limit, page_limit, search) if has_more else None
        return PageResponse(items=items, next_cursor=next_cursor)


# one codec, two helpers, module-local — the shared pagination codec
# is DB-UUID keyset-shaped; this endpoint pages a Keycloak offset instead.
# ponytail: bounds the *intra-request* role-gather (thundering herd ≤10); the
# cross-request token-refresh race on the shared client connection is pre-existing
# and self-heals via the client's 401 retry.
_ROLE_GATHER_CONCURRENCY = 10


def _encode_offset_cursor(offset: int, limit: int | None = None, search: str | None = None) -> str:
    """Opaque cursor: base64url JSON of the Keycloak Admin API offset (its ``first``).

    New-format cursors also pin the walk (``limit`` + ``search``); ``limit=None``
    emits the legacy ``{"offset": n}`` shape, still decoded (unpinned).
    """
    payload: dict[str, object] = {"offset": offset}
    if limit is not None:
        payload["limit"] = limit
        payload["search"] = search
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def _decode_offset_cursor(cursor: str) -> tuple[int, int | None, str | None]:
    """Decode an offset cursor → ``(offset, pinned_limit, pinned_search)``.

    New-format cursors pin both; legacy ``{"offset": n}`` ones decode leniently
    as unpinned (``None, None`` — the request's limit/search apply). Any
    malformed/tampered value → :class:`InvalidCursorError` (→ 400).
    """
    try:
        parsed = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        offset = parsed["offset"]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("cursor offset must be a non-negative int")
        pinned_limit = parsed.get("limit")
        if pinned_limit is None:
            return offset, None, None  # legacy shape: unpinned
        if isinstance(pinned_limit, bool) or not isinstance(pinned_limit, int) or pinned_limit < 1:
            raise ValueError("cursor limit must be a positive int")
        pinned_search = parsed.get("search")
        if pinned_search is not None and not isinstance(pinned_search, str):
            raise ValueError("cursor search must be a string or null")
    except Exception as exc:  # malformed base64 / JSON / wrong shape / bad offset
        raise InvalidCursorError(cursor) from exc
    return offset, pinned_limit, pinned_search
