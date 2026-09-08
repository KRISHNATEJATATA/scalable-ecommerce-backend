"""Keycloak Admin API adapter (service-account client).

Implements :class:`src.identity.ports.admin.IdentityAdminPort` via
``python-keycloak``. The library exposes native async methods (``a_*``), so no
``run_in_threadpool`` wrapping is needed. The connection is built lazily on first
use and reused (it refreshes its own service-account token).

In Keycloak the OIDC ``sub`` claim *is* the internal user id, so ``user_sub`` is
passed straight through as the admin ``user_id``.

Every Admin-API call runs through :meth:`KeycloakIdentityAdmin._guarded`:
bounded retry (transient faults only — connection errors, timeouts, 5xx/429)
inside one circuit-breaker decision, with a per-process breaker on the
``keycloak_admin`` dependency. While the breaker is open, calls fail fast with
a 503 :class:`DependencyUnavailableError` without touching the network. Only
**idempotent** operations retry (role grant/revoke, enable/disable, lookups);
``create_user`` is breaker-guarded but never retried — its compensating delete
can itself fail mid-outage, and re-running a create against a half-dead
Keycloak would surface a misleading 409 for an orphaned credentialless account.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from keycloak.exceptions import KeycloakConnectionError, KeycloakError, KeycloakGetError

from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import KeycloakConflictError, KeycloakEntityNotFoundError


def _translate(exc: KeycloakError) -> Exception:
    """Map Keycloak's status-carrying errors onto purpose-named ones.

    404 (unknown user ``sub`` / realm role) and 409 (email/username already
    taken) are caller-fixable outcomes that must reach the admin as 404/409 —
    falling through here meant the 500 boundary answered them. Anything else
    (401 expired token, 403 missing service-account role, 5xx outage) stays as
    raised: a genuine server/dependency fault.
    """
    code = getattr(exc, "response_code", None)
    if code == 404:
        return KeycloakEntityNotFoundError()
    if code == 409:
        return KeycloakConflictError("a Keycloak account with this email already exists")
    return exc


def map_admin_errors[**P, R](fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Translate ``KeycloakError`` on the wrapped Admin-API call (see :func:`_translate`)."""

    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await fn(*args, **kwargs)
        except KeycloakGetError as exc:
            raise _translate(exc) from exc
        except KeycloakError as exc:
            raise _translate(exc) from exc

    wrapper.__name__ = getattr(fn, "__name__", "map_admin_errors")
    wrapper.__doc__ = fn.__doc__
    return wrapper


# Appended below map_admin_errors to keep the import/translation block at the top
# of the file: the basedpyright diagnostic-diff gate keys on (file, line) and the
# baseline diagnostics live above.
from src.identity.domain.user import DirectoryUser  # noqa: E402
from src.shared.errors.exceptions import DependencyUnavailableError  # noqa: E402
from src.shared.resilience import (  # noqa: E402
    CircuitBreaker,
    CircuitOpenError,
    is_transient_exception,
    retry_transient,
)


def _keycloak_transient(exc: BaseException) -> bool:
    """Transient = worth retrying / breaker-counting: the shared classifier plus
    ``KeycloakConnectionError`` (python-keycloak wraps connection faults with no
    status code, so the generic ``response_code``/OSError sniff misses it)."""
    return is_transient_exception(exc) or isinstance(exc, KeycloakConnectionError)


class KeycloakIdentityAdmin:
    """Grant/revoke realm roles and enable/disable users via Keycloak's Admin API."""

    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings
        self._admin: KeycloakAdmin | None = None
        # One breaker per adapter instance (one per process via app.state).
        self._breaker = CircuitBreaker(
            "keycloak_admin",
            failure_threshold=settings.resilience_breaker_failure_threshold,
            reset_timeout_seconds=settings.resilience_breaker_reset_seconds,
        )

    async def _client(self) -> KeycloakAdmin:
        if self._admin is None:
            settings = self._settings
            if not (
                settings.keycloak_issuer and settings.keycloak_admin_client_id and settings.keycloak_admin_client_secret
            ):
                raise RuntimeError("Keycloak admin client is not configured")
            # issuer is ``<server_url>/realms/<realm>`` → strip back to the server root.
            server_url = settings.keycloak_server_url or settings.keycloak_issuer.rsplit("/realms/", 1)[0] + "/"
            connection = KeycloakOpenIDConnection(
                server_url=server_url,
                realm_name=settings.keycloak_realm,
                client_id=settings.keycloak_admin_client_id,
                client_secret_key=settings.keycloak_admin_client_secret,
            )
            self._admin = KeycloakAdmin(connection=connection)
        return self._admin

    async def _guarded[R](self, operation: Callable[[], Awaitable[R]], *, retry: bool = True) -> R:
        """One breaker-bracketed Admin-API call (see :mod:`src.shared.resilience`).

        The breaker sees a *single* outcome per logical call: a definitive
        answer — including handled 4xx — records success (a dependency that
        answers 404 is healthy; the breaker tracks reachability, not business
        outcomes); a transient fault after the bounded attempts records a
        failure and surfaces as :class:`DependencyUnavailableError` (503); a
        cancellation records an **abandoned** call (no health signal, but the
        half-open probe slot must be released or the breaker wedges).

        Retried only when ``retry`` is set — for the idempotent operations.
        While the breaker is open, the call fails fast without touching the
        network.
        """
        if not self._breaker.allow():
            raise CircuitOpenError("Keycloak Admin API is unavailable (circuit open)")
        try:
            result = await retry_transient(
                operation,
                attempts=self._settings.resilience_max_attempts if retry else 1,
                base_delay_seconds=self._settings.resilience_retry_base_delay_seconds,
                max_delay_seconds=self._settings.resilience_retry_max_delay_seconds,
                is_transient=_keycloak_transient,
            )
        except Exception as exc:
            if _keycloak_transient(exc):
                self._breaker.record_failure()
                raise DependencyUnavailableError("Keycloak is unavailable") from exc
            self._breaker.record_success()  # a definitive answer (4xx incl.) proves it is up
            raise
        except BaseException:
            # CancelledError et al.: no outcome, but release any half-open probe
            # slot — a cancelled call must not wedge the breaker in half-open.
            self._breaker.record_abandoned()
            raise
        self._breaker.record_success()
        return result

    @map_admin_errors
    async def grant_realm_role(self, user_sub: str, role: str) -> None:
        async def op() -> None:
            kc = await self._client()
            role_rep = await kc.a_get_realm_role(role)
            await kc.a_assign_realm_roles(user_sub, [role_rep])

        await self._guarded(op)

    @map_admin_errors
    async def revoke_realm_role(self, user_sub: str, role: str) -> None:
        async def op() -> None:
            kc = await self._client()
            role_rep = await kc.a_get_realm_role(role)
            await kc.a_delete_realm_roles_of_user(user_sub, [role_rep])

        await self._guarded(op)

    @map_admin_errors
    async def set_enabled(self, user_sub: str, enabled: bool) -> None:
        async def op() -> None:
            kc = await self._client()
            await kc.a_update_user(user_sub, {"enabled": enabled})

        await self._guarded(op)

    async def get_user_email(self, user_sub: str) -> str | None:
        """Look up a Keycloak user's email by ``sub`` (``None`` only if the user is gone).

        Suppress **404 alone**. Swallowing every ``KeycloakGetError`` would turn an
        expired admin token (401), a missing service-account role (403) or a Keycloak
        outage (5xx) into "this user doesn't exist" — and the disable path would then
        return 204 without writing the inactive local mirror, leaving the account free
        to JIT-provision itself active again on its next request. Remaining
        provider faults are retried and breaker-counted by :meth:`_guarded`
        first, so what escapes here after a *sustained* outage is already the
        503 type.
        """

        async def op() -> str | None:
            kc = await self._client()
            try:
                user: dict[str, object] | None = await kc.a_get_user(user_sub)
            except KeycloakGetError as exc:
                if exc.response_code == 404:
                    return None
                raise
            email = (user or {}).get("email")
            return email if isinstance(email, str) else None

        return await self._guarded(op)

    @map_admin_errors
    async def create_user(self, email: str) -> str:
        """Create a new Keycloak account (``consumer`` default role, per Keycloak realm
        config), email it a set-up link, and return its ``sub``.

        No credential is passed through this API — the spec forbids the app ever
        seeing passwords. The account is created with ``UPDATE_PASSWORD`` (and
        ``VERIFY_EMAIL``, since the address is unverified) required actions, and we
        immediately send Keycloak's **execute-actions email** so the user actually
        receives the link to set their password. Without that email a
        credentialless account can never authenticate. Keycloak (the credential
        authority) owns the password/refresh story from there on.

        Create + email are made effectively atomic: if the email send fails **or
        the call is cancelled**, the just-created (credentialless, unauthenticatable)
        account is deleted, so a caller retry starts clean instead of colliding with
        an orphaned half-provisioned user on Keycloak's own email/username uniqueness.
        The compensating delete is shielded so a cancellation can't abort the cleanup
        itself. A duplicate email/username surfaces as :class:`KeycloakConflictError`
        (409) — the account already exists — instead of a raw 500.

        **Never retried** (``_guarded(retry=False)``): a create is not idempotent,
        and if the compensating delete itself fails mid-outage, retrying the whole
        operation would hit the surviving account's 409 — stranding an orphaned,
        credentialless account behind a misleading "email already exists". The
        breaker still guards it (fail fast while Keycloak is down).
        """

        async def op() -> str:
            kc = await self._client()
            actions = ["UPDATE_PASSWORD", "VERIFY_EMAIL"]
            try:
                user_sub = await kc.a_create_user(
                    {
                        "email": email,
                        "username": email,
                        "enabled": True,
                        "emailVerified": False,
                        "requiredActions": actions,
                    }
                )
            except KeycloakError as exc:
                raise _translate(exc) from exc
            try:
                await kc.a_send_update_account(user_id=user_sub, payload=actions)
            except BaseException:
                # BaseException (not Exception) so a cancellation during the email send
                # also triggers compensation; shield the delete so the cancellation
                # can't abort the rollback and re-orphan the account.
                await asyncio.shield(kc.a_delete_user(user_sub))
                raise
            return user_sub

        return await self._guarded(op, retry=False)

    @map_admin_errors
    async def list_users(self, search: str | None, first: int, max_results: int) -> list[DirectoryUser]:
        """Page through Keycloak's user directory (offset pagination).

        ``search`` is substring (contains) matching over username/email: the term
        is wrapped as ``*term*`` — Keycloak's documented wildcard syntax, where
        the bare term is *prefix* matching (since Keycloak 18) and ``*`` is the
        only wildcard (``%``/``_``/``\\`` are escaped server-side). Characters
        typed inside the term keep their Keycloak-syntax meaning after the wrap:
        a ``*`` acts as a wildcard (``a*b`` = "a then b") and quoting marks
        become literal (``"x"`` no longer means exact). A multi-word term is
        split by Keycloak into ANDed per-token matches (last token as suffix).
        ``None``/empty pages the whole directory untouched (Keycloak trims
        whitespace server-side, so a blank term also lists everything).

        Every ``KeycloakError`` here is a dependency fault: :meth:`_guarded`
        retries transient ones and fails fast while the breaker is open, then
        :func:`map_admin_errors` maps what remains to
        :class:`DependencyUnavailableError` (503) — the same contract as the
        JWKS path and the frontend's documented outage handling.
        ``max_results`` is passed to Keycloak as its ``max`` query param (``max``
        is avoided as a parameter name to not shadow the builtin).

        ponytail: offset pagination drifts under concurrent user creation (rows can
        repeat/skip across pages) — acceptable for an admin directory.
        """

        async def op() -> list[DirectoryUser]:
            kc = await self._client()
            # Keycloak treats an empty ``search`` ("") as "list everything" — same as
            # None — so don't wrap it into a meaningless "*" (matches-everything-but-differently).
            infix = f"*{search}*" if search else None
            users = await kc.a_get_users(
                query={"first": first, "max": max_results, **({"search": infix} if infix else {})}
            )
            return [DirectoryUser(sub=u["id"], email=u.get("email"), enabled=u.get("enabled", True)) for u in users]

        return await self._guarded(op)

    @map_admin_errors
    async def has_realm_role(self, user_sub: str, role: str) -> bool:
        """Check a user's realm-role mappings (read live, more current than token claims).

        Returns ``True``/``False`` per the role list. A 404 (user vanished between the
        directory page and this lookup) returns ``False`` — the account simply doesn't
        exist anymore, so it is shown roleless instead of failing the whole page. Any
        other ``KeycloakError`` is a dependency fault: retried/fail-fast by
        :meth:`_guarded`, then mapped to :class:`DependencyUnavailableError` (503)
        by :func:`map_admin_errors`.
        """

        async def op() -> bool:
            kc = await self._client()
            try:
                roles = await kc.a_get_realm_roles_of_user(user_sub)
            except KeycloakGetError as exc:
                if exc.response_code == 404:
                    return False
                raise
            return any(r.get("name") == role for r in roles)

        return await self._guarded(op)
