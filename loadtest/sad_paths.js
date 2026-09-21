/*
 * Sad-path scenarios (k6) — the counterpart to checkout.js.
 *
 * checkout.js measures the happy path and gates on a p95 SLO. This script drives
 * the paths a support ticket comes from, and each check asserts an EXPECTED
 * outcome (including the expected 409s), so `checks: rate==1` is the gate: a
 * passing run means the sad paths behaved, not that nothing went wrong. No
 * latency threshold — this is behaviour, not capacity.
 *
 * Scenarios (K6_CASE=decline|contention|replay to run one; default all three):
 *
 *   decline     one user, a token the stub gateway declines (tok_declined):
 *               checkout → 409 "payment was declined", the order is cancelled and
 *               the holds released, but the CART IS KEPT (nothing was sold) — and
 *               the same cart checks out 201 with a fresh Idempotency-Key. The
 *               failure is recoverable, not a dead end.
 *   contention  five buyers, ONE unit in stock (restocked to 1 for this run):
 *               exactly one 201 and four out-of-stock 409s, then `available` is
 *               read back at 0 in teardown. The oversell assertion as observation.
 *   replay      one user, one key twice: the second response is the stored 201 for
 *               the same order, marked `Idempotent-Replay: true` — and a fresh
 *               checkout carries no such header.
 *
 * Isolation: each scenario owns its product AND its user(s), so the three can run
 * in any order (or concurrently) without contaminating each other's carts or stock.
 *
 * Run:  make sad-loadtest               (k6 run loadtest/sad_paths.js)
 *       K6_CASE=contention k6 run loadtest/sad_paths.js
 * Env:  BASE_URL (default http://localhost:8000)
 *       KEYCLOAK_URL (default http://localhost:8080)
 *       K6_DECLINE_TOKEN (default tok_declined — must contain PAYMENT_STUB_FAIL_TOKEN_SUBSTRING)
 *       K6_ADMIN_CLIENT_SECRET / K6_ADMIN_USERNAME / K6_ADMIN_PASSWORD (seeded demo.admin)
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const KEYCLOAK_URL = __ENV.KEYCLOAK_URL || 'http://localhost:8080';
const REALM = 'ecommerce';
const SPA_CLIENT = 'ecommerce-spa';
const ADMIN_CLIENT = 'ecommerce-admin';
const ADMIN_SECRET = __ENV.K6_ADMIN_CLIENT_SECRET || 'change-me-local-only';
const ADMIN_USERNAME = __ENV.K6_ADMIN_USERNAME || 'demo.admin';
const ADMIN_PASSWORD = __ENV.K6_ADMIN_PASSWORD || 'DemoAdmin123!';
const DECLINE_TOKEN = __ENV.K6_DECLINE_TOKEN || 'tok_declined';
const CASE = __ENV.K6_CASE || 'all';
const RUN_TAG = Date.now();

// Winners of the last-unit race: exactly one is allowed. A threshold (below)
// rather than a per-VU check because only the aggregate can see all five.
const contentionSuccess = new Counter('contention_success');

const allScenarios = {
  decline: { executor: 'shared-iterations', vus: 1, iterations: 1, exec: 'declineCase' },
  contention: { executor: 'shared-iterations', vus: 5, iterations: 5, exec: 'contentionCase' },
  replay: { executor: 'shared-iterations', vus: 1, iterations: 1, exec: 'replayCase' },
};

const scenarios = CASE === 'all' ? allScenarios : { [CASE]: allScenarios[CASE] };
const thresholds = { checks: ['rate==1'] };
if (CASE === 'all' || CASE === 'contention') {
  thresholds.contention_success = ['count==1'];
}

export const options = { scenarios, thresholds };

function grant(params) {
  const res = http.post(
    `${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`,
    params,
    { tags: { name: 'token' } },
  );
  const access = res.json('access_token');
  if (!access) throw new Error(`token grant failed: HTTP ${res.status} ${res.body}`);
  return access;
}

// k6 normalises header names to lowercase; look up either spelling so a check
// can't fail on case alone.
function headerValue(res, name) {
  const key = Object.keys(res.headers).find((k) => k.toLowerCase() === name.toLowerCase());
  return key ? res.headers[key] : undefined;
}

function createUser(adminToken, i) {
  const username = `sadpath-${RUN_TAG}-u${i}`;
  const created = http.post(
    `${KEYCLOAK_URL}/admin/realms/${REALM}/users`,
    JSON.stringify({
      username: username,
      email: `${username}@example.com`,
      firstName: 'Sad',
      lastName: `Path ${i}`, // realm profile requires first/last names (else VERIFY_PROFILE blocks login)
      enabled: true,
      emailVerified: true,
      credentials: [{ type: 'password', value: 'SadPath123!', temporary: false }],
    }),
    {
      headers: { Authorization: `Bearer ${adminToken}`, 'Content-Type': 'application/json' },
      tags: { name: 'provision' },
    },
  );
  if (!check(created, { 'user created': (r) => r.status === 201 })) {
    throw new Error(`could not create ${username}: HTTP ${created.status} ${created.body}`);
  }
  const found = http.get(`${KEYCLOAK_URL}/admin/realms/${REALM}/users?username=${username}&exact=true`, {
    headers: { Authorization: `Bearer ${adminToken}` },
    tags: { name: 'provision' },
  });
  const user = (found.json() || [])[0];
  if (!user) throw new Error(`created user ${username} not found afterwards`);
  const token = grant({ grant_type: 'password', client_id: SPA_CLIENT, username, password: 'SadPath123!' });
  return { id: user.id, token };
}

function restock(productId, onHand, demoAdmin) {
  const res = http.put(
    `${BASE_URL}/v1/admin/inventory/${productId}`,
    JSON.stringify({ on_hand: onHand }),
    {
      headers: { Authorization: `Bearer ${demoAdmin}`, 'Content-Type': 'application/json' },
      tags: { name: 'restock' },
    },
  );
  check(res, { [`restocked ${onHand}`]: (r) => r.status === 200 });
}

export function setup() {
  // Admin-API client_credentials: provisioning + teardown only.
  const adminToken = grant({ grant_type: 'client_credentials', client_id: ADMIN_CLIENT, client_secret: ADMIN_SECRET });
  const demoAdmin = grant({ grant_type: 'password', client_id: SPA_CLIENT, username: ADMIN_USERNAME, password: ADMIN_PASSWORD });

  // 1 user for decline + 5 for the last-unit race + 1 for replay. Created even
  // when K6_CASE runs a single scenario: setup stays one shape, teardown removes them all.
  const users = [];
  for (let i = 0; i < 7; i++) users.push(createUser(adminToken, i));

  // Each scenario owns a product, so stock changes can't cross-contaminate runs.
  const listing = http.get(`${BASE_URL}/v1/products?limit=20`, {
    headers: { Authorization: `Bearer ${users[0].token}` },
    tags: { name: 'list' },
  });
  const products = (listing.json('items') || []).filter((p) => p.available !== null);
  if (products.length < 3) throw new Error('need 3 seeded products with stock — run `make seed` first');
  const [declineProduct, contentionProduct, replayProduct] = products;

  restock(declineProduct.id, 1000, demoAdmin);
  restock(contentionProduct.id, 1, demoAdmin); // the last unit: one winner, four refusals
  restock(replayProduct.id, 1000, demoAdmin);

  // Warm the JIT identity row per user OUTSIDE the measured window (the first
  // authenticated request of a fresh sub does an identity INSERT).
  for (const u of users) {
    const warm = http.get(`${BASE_URL}/v1/cart`, {
      headers: { Authorization: `Bearer ${u.token}` },
      tags: { name: 'warm' },
    });
    check(warm, { 'cart warm 200': (r) => r.status === 200 });
  }

  return {
    decline: { token: users[0].token, productId: declineProduct.id },
    contention: { tokens: users.slice(1, 6).map((u) => u.token), productId: contentionProduct.id },
    replay: { token: users[6].token, productId: replayProduct.id },
    userIds: users.map((u) => u.id),
    adminToken,
  };
}

export function teardown(data) {
  // The oversell assertion, read back from the catalog: the contention product's
  // one unit was sold exactly once and no loser's hold leaked. `available` is
  // on_hand - reserved, so a leaked hold reads -1 and a double-sell is impossible
  // by construction (the DB CHECK forbids negative on_hand) — either way != 0 fails.
  if (CASE === 'all' || CASE === 'contention') {
    const read = http.get(`${BASE_URL}/v1/products/${data.contention.productId}`, {
      headers: { Authorization: `Bearer ${data.decline.token}` },
      tags: { name: 'verify' },
    });
    check(read, {
      'last unit sold exactly once (available == 0)': (r) => r.status === 200 && r.json('available') === 0,
    });
  }

  // Delete the per-run users so reruns don't accumulate them in the dev realm.
  for (const id of data.userIds) {
    http.del(`${KEYCLOAK_URL}/admin/realms/${REALM}/users/${id}`, null, {
      headers: { Authorization: `Bearer ${data.adminToken}` },
      tags: { name: 'provision' },
    });
  }
}

function addToCart(token, productId, tag) {
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  const res = http.post(`${BASE_URL}/v1/cart/items`, JSON.stringify({ product_id: productId, quantity: 1 }), {
    headers,
    tags: { name: `cart-${tag}` },
  });
  check(res, { 'cart add 200': (r) => r.status === 200 });
  return headers;
}

export function declineCase(data) {
  const { token, productId } = data.decline;
  const headers = addToCart(token, productId, 'decline');

  const declined = http.post(`${BASE_URL}/v1/checkout`, JSON.stringify({ payment_token: DECLINE_TOKEN }), {
    headers: { ...headers, 'Idempotency-Key': `sad-decline-${RUN_TAG}` },
    tags: { name: 'checkout-decline' },
  });
  check(declined, {
    'declined checkout 409': (r) => r.status === 409,
    'problem says declined': (r) => String(r.json('detail') || '').includes('declined'),
    'a refusal is not a replay': (r) => headerValue(r, 'Idempotent-Replay') === undefined,
  });

  // Nothing was sold, so nothing may have been cleared: the cart still holds the line.
  const cart = http.get(`${BASE_URL}/v1/cart`, { headers, tags: { name: 'cart-read' } });
  check(cart, {
    'cart kept after a decline': (r) =>
      r.status === 200 && (r.json('items') || []).some((item) => item.product_id === productId),
  });

  // The same cart recovers with a fresh key: the sad path is a retry, not a dead end.
  const retry = http.post(`${BASE_URL}/v1/checkout`, JSON.stringify({ payment_token: 'tok_visa' }), {
    headers: { ...headers, 'Idempotency-Key': `sad-decline-retry-${RUN_TAG}` },
    tags: { name: 'checkout-decline-retry' },
  });
  check(retry, {
    'retry with a fresh key checks out 201': (r) => r.status === 201,
    'the retry is a fresh order': (r) => headerValue(r, 'Idempotent-Replay') === undefined,
  });
}

export function contentionCase(data) {
  // __VU is 1..5 for this scenario's five VUs — each racer gets its OWN cart, so
  // the only shared resource is the single unit of stock.
  const token = data.contention.tokens[(__VU - 1) % data.contention.tokens.length];
  const headers = addToCart(token, data.contention.productId, 'contention');

  const res = http.post(`${BASE_URL}/v1/checkout`, JSON.stringify({ payment_token: 'tok_visa' }), {
    headers: { ...headers, 'Idempotency-Key': `sad-contention-${RUN_TAG}-${__VU}` },
    tags: { name: 'checkout-contention' },
  });
  if (res.status === 201) contentionSuccess.add(1); // the threshold allows exactly one
  check(res, {
    'last unit: winner 201 or loser 409': (r) => r.status === 201 || r.status === 409,
    'a refusal carries a problem body': (r) => r.status !== 409 || String(r.json('detail') || '').length > 0,
  });
}

export function replayCase(data) {
  const { token, productId } = data.replay;
  const headers = addToCart(token, productId, 'replay');
  const key = `sad-replay-${RUN_TAG}`;

  const first = http.post(`${BASE_URL}/v1/checkout`, JSON.stringify({ payment_token: 'tok_visa' }), {
    headers: { ...headers, 'Idempotency-Key': key },
    tags: { name: 'checkout-replay-1' },
  });
  check(first, {
    'fresh checkout 201': (r) => r.status === 201,
    'a fresh checkout is not marked as a replay': (r) => headerValue(r, 'Idempotent-Replay') === undefined,
  });

  // Same key, same body: the stored 201, for the same order, marked as a replay.
  const second = http.post(`${BASE_URL}/v1/checkout`, JSON.stringify({ payment_token: 'tok_visa' }), {
    headers: { ...headers, 'Idempotency-Key': key },
    tags: { name: 'checkout-replay-2' },
  });
  check(second, {
    'replay 201': (r) => r.status === 201,
    'the replay marks itself': (r) => headerValue(r, 'Idempotent-Replay') === 'true',
    'the replay returns the same order': (r) => r.json('id') === first.json('id'),
  });
}


