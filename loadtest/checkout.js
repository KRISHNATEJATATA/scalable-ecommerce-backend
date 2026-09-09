/*
 * Checkout load test (k6) — the SLO is the pass/fail line.
 *
 * Drives the REAL checkout flow against a running stack (make compose-up +
 * make seed + make run): setup() provisions one Keycloak user PER VU (via the
 * dev realm's Admin API), warms their JIT identity row, restocks one seeded
 * product, then every iteration adds to THAT VU's cart and checks out.
 *
 * One user per VU is load-bearing, not cosmetics: carts are per-user, so VUs
 * sharing the seeded demo.consumer would race one shared cart — a checkout
 * clearing the cart under another VU's add+checkout produces "cart is empty"
 * 409s that measure the harness, not the service.
 *
 * The threshold below is the assertion — a load test that only prints numbers
 * without a threshold verifies nothing:
 *
 *     p(95) of the checkout request < 300 ms
 *
 * Run:  make loadtest     (k6 run loadtest/checkout.js)
 * Env:  BASE_URL (default http://localhost:8000)
 *       KEYCLOAK_URL (default http://localhost:8080)
 *       K6_RATE (checkouts/s, default 10)   K6_DURATION (default 30s)
 *       K6_VUS (users created & VU cap, default 20)
 *       K6_ADMIN_USERNAME / K6_ADMIN_PASSWORD (seeded demo.admin, restocks)
 */

import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const KEYCLOAK_URL = __ENV.KEYCLOAK_URL || 'http://localhost:8080';
const REALM = 'ecommerce';
const SPA_CLIENT = 'ecommerce-spa'; // public client with direct-access grants (dev realm export)
const ADMIN_CLIENT = 'ecommerce-admin'; // confidential client, secret lives in docker-compose (dev only)
const ADMIN_SECRET = __ENV.K6_ADMIN_CLIENT_SECRET || 'change-me-local-only';
const USERNAME = __ENV.K6_USERNAME || 'demo.consumer';
const PASSWORD = __ENV.K6_PASSWORD || 'DemoConsumer123!';
const ADMIN_USERNAME = __ENV.K6_ADMIN_USERNAME || 'demo.admin';
const ADMIN_PASSWORD = __ENV.K6_ADMIN_PASSWORD || 'DemoAdmin123!';
const RATE = Number(__ENV.K6_RATE || 10);
const DURATION = __ENV.K6_DURATION || '30s';
const VUS = Number(__ENV.K6_VUS || 20);
const RUN_TAG = Date.now(); // unique usernames per run: a rerun never collides with a stale teardown

export const options = {
  scenarios: {
    checkout: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS, // every VU maps to a setup-created user; never scale past the list
    },
  },
  thresholds: {
    'http_req_duration{name:checkout}': ['p(95)<300'],
    checks: ['rate>0.99'],
  },
};

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

function createUser(adminToken, i) {
  const username = `loadtest-${RUN_TAG}-vu${i}`;
  const created = http.post(
    `${KEYCLOAK_URL}/admin/realms/${REALM}/users`,
    JSON.stringify({
      username: username,
      email: `${username}@example.com`,
      firstName: 'Load',
      lastName: `Test ${i}`, // realm profile requires first/last names (else VERIFY_PROFILE blocks login)
      enabled: true,
      emailVerified: true,
      credentials: [{ type: 'password', value: 'LoadTest123!', temporary: false }],
    }),
    {
      headers: { Authorization: `Bearer ${adminToken}`, 'Content-Type': 'application/json' },
      tags: { name: 'provision' },
    },
  );
  check(created, { 'user created': (r) => r.status === 201 });
  const found = http.get(
    `${KEYCLOAK_URL}/admin/realms/${REALM}/users?username=${username}&exact=true`,
    { headers: { Authorization: `Bearer ${adminToken}` }, tags: { name: 'provision' } },
  );
  const user = found.json()[0];
  if (!user) throw new Error(`created user ${username} not found afterwards`);
  return { id: user.id, token: grant({ grant_type: 'password', client_id: SPA_CLIENT, username, password: 'LoadTest123!' }) };
}

export function setup() {
  // The Admin-API client_credentials token: provisioning + teardown cleanup only.
  const adminToken = grant({ grant_type: 'client_credentials', client_id: ADMIN_CLIENT, client_secret: ADMIN_SECRET });
  const demoAdmin = grant({ grant_type: 'password', client_id: SPA_CLIENT, username: ADMIN_USERNAME, password: ADMIN_PASSWORD });

  const users = [];
  for (let i = 0; i < VUS; i++) users.push(createUser(adminToken, i));

  // Warm each user's JIT identity row OUTSIDE the measured window: the first
  // authenticated request of a fresh sub does an identity INSERT, which would
  // otherwise land in VU #1's first checkout.
  for (const u of users) {
    const warm = http.get(`${BASE_URL}/v1/cart`, {
      headers: { Authorization: `Bearer ${u.token}` },
      tags: { name: 'warm' },
    });
    check(warm, { 'cart warm 200': (r) => r.status === 200 });
  }

  // Pick the first seeded product and restock it to survive the whole run —
  // otherwise sold-out 409s silently turn the test into a stock-refusal benchmark.
  const listing = http.get(`${BASE_URL}/v1/products?limit=5`, {
    headers: { Authorization: `Bearer ${users[0].token}` },
    tags: { name: 'list' },
  });
  const products = listing.json('items') || [];
  if (products.length === 0) throw new Error('no products found — run `make seed` first');
  const product = products[0];
  const restock = http.put(
    `${BASE_URL}/v1/admin/inventory/${product.id}`,
    JSON.stringify({ on_hand: 100000 }),
    {
      headers: { Authorization: `Bearer ${demoAdmin}`, 'Content-Type': 'application/json' },
      tags: { name: 'restock' },
    },
  );
  check(restock, { 'restocked': (r) => r.status === 200 });

  return {
    productId: product.id,
    tokens: users.map((u) => u.token),
    userIds: users.map((u) => u.id),
    adminToken: adminToken,
  };
}

export function teardown(data) {
  // Delete the per-run users so reruns don't accumulate them in the dev realm.
  // A teardown that dies mid-list leaves loadtest-<run>-vuN users behind —
  // harmless: the next run's usernames are unique by construction.
  for (const id of data.userIds) {
    http.del(`${KEYCLOAK_URL}/admin/realms/${REALM}/users/${id}`, null, {
      headers: { Authorization: `Bearer ${data.adminToken}` },
      tags: { name: 'provision' },
    });
  }
}

export default function (data) {
  // One user per VU (indexed by k6's stable __VU number) — own cart, no races.
  const headers = {
    Authorization: `Bearer ${data.tokens[__VU - 1]}`,
    'Content-Type': 'application/json',
  };
  const added = http.post(
    `${BASE_URL}/v1/cart/items`,
    JSON.stringify({ product_id: data.productId, quantity: 1 }),
    { headers: headers, tags: { name: 'cart' } },
  );
  check(added, { 'cart add 200': (r) => r.status === 200 });

  // Fresh idempotency key per iteration: the load measures the checkout path
  // itself, not the replay fast path.
  const res = http.post(
    `${BASE_URL}/v1/checkout`,
    JSON.stringify({ payment_token: 'tok_visa' }),
    {
      headers: { ...headers, 'Idempotency-Key': `k6-${__VU}-${__ITER}-${Date.now()}` },
      tags: { name: 'checkout' },
    },
  );
  check(res, {
    'checkout 201': (r) => r.status === 201,
  });
}
