# Glossary

Shared vocabulary for the e-commerce backend. Terms marked **(role)** are
Keycloak realm roles carried in the access token.

| Term | Meaning |
|---|---|
| **OIDC** | OpenID Connect. Identity layer on top of OAuth 2.0. The app trusts Keycloak-issued tokens instead of authenticating users itself. |
| **Keycloak** | The Identity Provider (IdP). Free/open-source, runs as a container locally and as a deployed service in any env. Owns login, credentials, refresh, logout, password reset, MFA, social federation, and the realm-role catalogue. |
| **Resource server** | The role the FastAPI app plays: it only *validates* Keycloak access tokens and serves protected resources. It never runs a login redirect or holds a session cookie. |
| **BFF (Backend-for-Frontend)** | The rejected alternative where the backend runs the OIDC login and holds a session cookie. Not used here. |
| **Access token** | Short-lived (~5 min) RS256 JWT issued by Keycloak, sent by the client in the `Authorization: Bearer …` header. Validated against Keycloak's JWKS. |
| **JWKS** | JSON Web Key Set — Keycloak's public keys, fetched (and cached) by the app to verify token signatures. |
| **`sub`** | The OIDC subject claim: Keycloak's stable unique user id. The local `users` row is keyed by it. |
| **Realm role** | A role defined in Keycloak and carried in the token's `realm_access.roles`. The authorization source of truth. |
| **Consumer (role)** | Browses items and places orders. The default role Keycloak assigns to new users. |
| **Merchant (role)** | Owns items. Can add and update/soft-remove **only their own** items (`merchant_id == user.id`). Cannot touch other merchants' items. |
| **Admin (role)** | Manages identities and catalogue: create/disable users, grant/revoke the merchant role (via Keycloak Admin API), and add/soft-remove any item. |
| **Item** | Product-facing name for a `Product` row: `name`, `description`, `price` (cost), `stock` (count), `image_key`, `merchant_id`. |
| **JIT provisioning** | Just-in-time creation of the local `users` row on a user's first authenticated request, keyed by the OIDC `sub`. |
| **Keycloak Admin API** | Keycloak's management REST API used by admin endpoints to create/disable users and assign roles. Accessed via `python-keycloak` with a service-account. |
| **Soft-remove** | Marking a record hidden/inactive (`deleted_at` for items, `enabled=false` in Keycloak + `is_active=false` locally for users) instead of physically deleting it, so history and audit survive. |
| **Valkey** | Redis-compatible store for rate-limit counters and idempotency keys. **No longer** holds a JWT denylist — revocation is handled by short token TTL. |
