# BUG-005: caller-fixable Keycloak 400s surface as HTTP 500 on admin user creation

## Severity
Medium

## Status
Fixed

## Fix Branch
bug/BUG-005-keycloak-400-as-500 (commit e3c8ef3 — NOT merged to main)

## Fix
Two layers. (1) Schema: `CreateUserRequest.email` now a pragmatic
`local@domain.tld` regex (anchored, no spaces/control chars, one @) with
`max_length=255` (Keycloak's column bound; 320 dropped) — regex chosen over
`EmailStr` because email-validator is absent and no new deps were allowed;
OpenAPI contract synced. (2) Adapter backstop: `_translate` gains a 400 arm
raising new `KeycloakInvalidRequestError`, registered on
`_detail_bad_request_handler` → clean 400 Problem Details. 3 regression tests
(2 schema-level HTTP 422s + 1 adapter-level 400 mapping).

## Summary
`CreateUserRequest.email` validates length only (`min_length=3, max_length=320`)
— no email format, no 255-char bound (Keycloak's username/email column).
`_translate` in the Keycloak admin adapter maps only 404 and 409; a Keycloak 400
(e.g. `error-invalid-email`) stays a raw `KeycloakPostError`, which no exception
handler registers — the catch-all boundary answers HTTP 500 for input the admin
can fix.

## Expected Behavior
Caller-fixable input errors are 4xx (per the repo's RFC 9457 status-mapping
convention): a malformed/oversized email must be rejected at the schema
(422) or mapped from the provider's 400 to a purpose-named 4xx — never 500.

## Actual Behavior
Reproduction (2026-09-09):
- `CreateUserRequest(email="not-an-email")` and a 302-char email both pass
  schema validation.
- Stubbing the admin client to raise `KeycloakPostError(..., response_code=400)`
  (deterministic Keycloak behavior for a malformed email), `create_user` lets
  the raw `KeycloakPostError` escape: no purpose-named exception, no registered
  handler → `_unhandled_exception_handler` (exception_handlers.py:208) → 500.

## Root Cause
1. `src/identity/api/schemas.py:26` — length-only email validation.
2. `src/identity/adapters/keycloak/admin_client.py:34-48` — `_translate` has no
   400 arm (its own docstring's taxonomy skips 400).

## Affected Area
`src/identity/api/schemas.py`, `src/identity/adapters/keycloak/admin_client.py`,
`src/shared/errors/exception_handlers.py` (mapping surface).

## Impact
Admins get an opaque 500 (paged on-call noise, per the repo's own 409-handler
rationale) for a typo'd email; retry storms against a deterministic input error.

## Proposed Fix
Validate the email at the schema boundary (`EmailStr`, or a pragmatic
`pattern` covering `local@domain` plus a 255-char bound to stay under
Keycloak's column), and/or add a 400 arm to `_translate` raising a
purpose-named 4xx for provider-rejected input.

## Regression Test
HTTP-level: `POST /v1/admin/users` with `{"email": "not-an-email"}` and with a
300-char address answer 422 (schema) — never 500; adapter-level: a stubbed
Keycloak 400 maps to a purpose-named exception with a 4xx handler.

## Verification
PASS (independent verification agent, 2026-09-09). Verifier's own battery:
30-case schema table (invalid shapes rejected incl. control chars/300-char;
plus-addressing and 254/255-boundary accepted; 256 rejected); adapter-level
400→`KeycloakInvalidRequestError` with 404/409/401/5xx mappings re-confirmed;
end-to-end HTTP proof that a provider 400 answers **400 Problem Details, never
500**. Pre-fix failure proven by executing the new tests against a worktree of
`e3c8ef3^` (schema accepted the malformed email → 201; adapter raised the raw
error). OpenAPI yaml parses with maxLength 255 + identical pattern.

## Tests
`pytest tests/unit/test_auth.py tests/unit/test_keycloak_admin_errors.py -q`
→ 44 passed. `pytest tests/unit -q` (full) → **501 passed, 1 skipped**
(skip = pre-existing libmagic host gap).

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean; basedpyright →
0 diagnostics on all changed files. Spectral not installed on this host
(pre-commit `npm run validate`); YAML parse-validated instead. Commit stat =
exactly the 7 claimed files; pre-existing dirty files untouched. Residual
(not bugs): pragmatic regex over-accepts degenerate-but-harmless shapes —
Keycloak remains the authoritative validator and its 400 now maps to 4xx;
no HTTP-level regression test pins the 400-handler path in-repo.
