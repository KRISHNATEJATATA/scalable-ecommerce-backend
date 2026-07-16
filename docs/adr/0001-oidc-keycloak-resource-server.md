# ADR 0001 — Authentication & authorization via OIDC (Keycloak), app as a resource server

- Status: Accepted
- Date: 2026-07-15
- Supersedes: the original "stateless self-issued RS256 JWT" auth decision in `plans/finalplan.md`

## Context

The original plan had the app mint its own RS256 JWTs (PyJWT + `pwdlib[argon2]`),
manage refresh-token rotation, a Valkey `jti` denylist, and Google OAuth via
Authlib. The product now needs a clean three-role model (consumer / merchant /
admin) with admin-driven user and merchant management. Rolling our own identity
lifecycle (password reset, email verification, MFA, social federation, user
admin) is a large, security-sensitive surface we do not want to own.

Requirement from the user: an IdP that is **free, runs locally for testing,
deploys to any environment, and is easy to apply and manage.**

## Decision

Adopt **OIDC with Keycloak** as the Identity Provider. The FastAPI app is a
**pure OIDC resource server** — it never handles credentials or login flows; it
only validates tokens Keycloak issued.

Connected sub-decisions:

1. **IdP = Keycloak.** Free, open-source, runs as a container in the existing
   compose stack locally and deploys to any env. Satisfies "free + local + any
   env + easy to manage" and honours the plan's rule that every prod component
   has a local equivalent. (Alternatives: Cognito — AWS-only, not local; Auth0/Okta
   — SaaS, limited free tier; social-only — no user admin.)

2. **Resource server, not BFF.** A separate frontend/SPA runs Authorization Code
   + PKCE against Keycloak. The API validates the Keycloak **RS256** access token
   on every request against Keycloak's cached **JWKS** (`iss`/`aud`/`exp` checked,
   algorithm hardcoded, `alg:none` guarded). Token travels in the `Authorization`
   header → **no auth cookie → no CSRF surface**. Deletes the app's login/refresh/
   logout/cookie/CSRF machinery.

3. **Keycloak is the role authority.** `consumer` / `merchant` / `admin` are
   **realm roles** carried in `realm_access.roles`. RBAC is a cheap
   `Depends(require_role(...))` claim check, no DB hit. Admin creates/disables
   users and grants/revokes the `merchant` role through Keycloak's **Admin API**
   (`python-keycloak`). New users get the default `consumer` role from Keycloak,
   so the privilege-escalation-via-input bug is structurally impossible.

4. **Local `users` row keyed by OIDC `sub`.** JIT-provisioned on first
   authenticated request. Exists only to anchor FK ownership
   (`products.merchant_id`, `orders.user_id`) plus an `is_active` mirror — it is
   **not** the identity or role source.

5. **Revocation = short access-token TTL (~5 min).** A disabled user's existing
   token expires fast; no app-side denylist or introspection hop. This removes
   Valkey from the auth path entirely (it still serves rate-limit counters +
   idempotency keys).

6. **Soft-remove, never hard delete.** Removing an item sets `deleted_at` and
   hides it; removing a user disables them in Keycloak (`enabled=false`) and flips
   the local `is_active=false`. Order history, invoices, and audit trails survive.

## Consequences

- Deleted from the app: self-issued tokens, refresh rotation, password hashing
  (`pwdlib`), Authlib OAuth wiring, the Valkey `jti` denylist, login/refresh/
  logout/CSRF/auth-cookie code, MFA, password reset, email verification. Keycloak
  owns all of it.
- Added: JWKS-based token validation (PyJWT `PyJWKClient`), a thin Keycloak
  Admin-API client (`python-keycloak`), a Keycloak container in compose + a
  Keycloak service in prod (RDS-backed), realm/role/client provisioning via
  realm-import.
- New config (settings + `.env.example`): `KEYCLOAK_ISSUER`, `KEYCLOAK_REALM`,
  `KEYCLOAK_AUDIENCE`, JWKS URL, admin service-account client id/secret.
- Trade-off accepted: up to ~5 min between disabling a user and their token
  expiring. Acceptable for this app; revisit with a Valkey denylist only if an
  instant-kill requirement appears.
