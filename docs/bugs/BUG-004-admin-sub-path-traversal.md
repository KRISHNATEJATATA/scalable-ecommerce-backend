# BUG-004: admin `sub` path parameters reach the Keycloak Admin API unvalidated (URL path traversal / query injection)

## Severity
Medium

## Status
Fixed

## Fix Branch
bug/BUG-004-admin-sub-path-traversal (commit 328ca03 — NOT merged to main)

## Fix
All three admin path params (`grant_merchant`, `revoke_merchant`,
`disable_user`) typed as `SubPath = Annotated[str, Path(min_length=1,
max_length=64, pattern=_SUB_PATTERN)]` — anchored 8-4-4-4-12 hex UUID pattern,
mirroring the inventory SKU path-param convention. Non-UUID subs answer 422
before reaching the service/adapter; service signatures and the Keycloak
adapter untouched. 1 regression test (`tests/unit/test_auth.py`) asserting
status 4xx AND zero calls reaching the fake admin port for traversal/junk
payloads across all three endpoints.

## Summary
`POST /v1/admin/users/{sub}/roles/merchant`, `DELETE .../roles/merchant`,
`POST .../disable` declare `sub: str` with no pattern. The value is handed raw
through the service to python-keycloak, which interpolates it into the Admin API
URL; `urllib.parse.urljoin` normalizes `..` segments and `?`/`#` start query
strings — an app-level admin can steer the service account's requests at a
different realm path or endpoint on the same Keycloak server.

## Expected Behavior
A Keycloak `sub` is a UUID. The trust boundary (route schema, per the repo's own
convention — see the inventory SKU path param's regex) must reject anything that
is not a UUID with 422 before the value reaches the adapter.

## Actual Behavior
Reproduction (2026-09-09, library + repo level):
```
urljoin("http://kc:8080/admin/realms/ecommerce/users/",
        "../../other-admin/realms/master/users/x")
 -> http://kc:8080/admin/realms/other-admin/realms/master/users/x
```
`KeycloakIdentityAdmin.grant_realm_role("../../other-admin/realms/master/users",
"merchant")` accepts the value with no validation error and passes it raw to
`a_assign_realm_roles`. `sub="x?foo=bar"` injects query parameters.

## Root Cause
`src/identity/api/routes.py` (path params without `pattern`/UUID type) — the
identity stack never constrains `sub` before `admin_client.py` formats it into
URLs (verified against installed python-keycloak 4.7.3).

## Affected Area
`src/identity/api/routes.py` (grant/revoke/disable), `src/identity/api/schemas.py`,
`src/identity/adapters/keycloak/admin_client.py` (defense-in-depth).

## Impact
Defense-in-depth gap: exploitation requires an admin-gated caller, and the
service account's cross-realm authorization usually 403s — but the crafted URL
also targets *different endpoints* (e.g. the `set_enabled` PUT ends at `{id}`,
giving full control of the path tail) and leaks the injection into Keycloak
access logs. Violates the repo's own "validate at the trust boundary" rule.

## Proposed Fix
Type the path params as `uuid.UUID` (FastAPI 422s non-UUIDs) or add
`pattern=r"^[0-9a-fA-F-]{36}$"`; optionally assert UUID-shape again in
`KeycloakIdentityAdmin` methods.

## Regression Test
HTTP-level: `POST /v1/admin/users/../../x/roles/merchant` and
`POST /v1/admin/users/not-a-uuid/disable` answer 422 (and never reach the admin
port — assert with the existing fake-admin dependency override).

## Verification
PASS (independent verification agent, 2026-09-09). Verifier's own 21-request
repro against the real app + Testcontainers: every evil payload
(encoded-slash traversal, `x?foo=bar`, `not-a-uuid`, 37-hex, `..`) answered
404/422 with the fake admin recording ZERO calls; valid-UUID happy paths
(grant 204, disable 204) intact. Regex checked against 12 shapes. The 404-vs-422
nuance is confirmed framework behavior (ASGI percent-decoding splits the path
segment → route 404s); the security invariant (value never reaches
python-keycloak) holds either way.

## Tests
`pytest tests/unit/test_auth.py -q` → 30 passed.
`pytest tests/unit -q` (full) → **499 passed, 1 skipped** (main 498 + 1 new).

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean; basedpyright/ty
→ no new diagnostics. Commit stat = exactly routes.py + test_auth.py;
pre-existing dirty files untouched.
