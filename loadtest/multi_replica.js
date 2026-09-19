/*
 * Multi-replica concurrency proof (k6) — "one order per idempotency key" as
 * observation, not architecture diagram.
 *
 * The regular loadtest/checkout.js deliberately removes contention: one user
 * per VU, fresh idempotency key per iteration, so it measures throughput/p95
 * of uncontended checkouts. THIS script is the opposite: it re-adds the
 * contention the README says k6 removed, because "exactly one order per key"
 * is precisely the invariant that only shows up when requests collide.
 *
 * Design — one race per iteration:
 *
 *   - ONE shared user (demo.consumer), ONE cart: the contention k6's
 *     per-VU-cart note warns about, deliberately re-created.
 *   - http.batch() fires TWO checkout POSTs with the SAME Idempotency-Key in
 *     parallel — racer 1 → :8000 (replica 1), racer 2 → :8001 (replica 2) —
 *     so the collision genuinely crosses both app processes.
 *   - The composite UNIQUE (user_id, idempotency_key) must arbitrate: the
 *     winner inserts, the loser rolls back and replays the winner's stored
 *     response. Both racers must answer 201 with the same order id.
 *
 * Any non-201 response from either racer, or mismatched order ids, counts a
 * proof_failure. The threshold gates the run: proof_failures == 0. A "green"
 * run with proof_failures > 0 means the multi-replica stack broke the
 * one-order-per-key invariant — the exact bug this proof exists to catch.
 *
 * Prereqs: make compose-up-multi (2x app, 2x relay, 2x notification-consumer)
 *          + make seed.
 *
 * Run:  k6 run loadtest/multi_replica.js
 * Env:  BASE_URL (replica 1, default http://localhost:8000)
 *       BASE_URL_2 (replica 2, default http://localhost:8001 — compose's port
 *       range binds replica 2 to 8001; racer 2 is sent HERE so the race
 *       genuinely crosses both app replicas)
 *       KEYCLOAK_URL (default http://localhost:8080)
 *       K6_RACES (race pairs, default 40)
 *       K6_ADMIN_CLIENT_SECRET (seeded demo admin client, restocks)
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
// Racer 2's target: compose --scale app=2 binds the second replica to :8001
// (the port range's second port). Without this, both racers hit :8000 and the
// "cross-replica" claim would be false — the race would stay intra-replica.
const BASE_URL_2 = __ENV.BASE_URL_2 || 'http://localhost:8001';
const KEYCLOAK_URL = __ENV.KEYCLOAK_URL || 'http://localhost:8080';
const REALM = 'ecommerce';
const SPA_CLIENT = 'ecommerce-spa';
const ADMIN_CLIENT = 'ecommerce-admin';
const ADMIN_SECRET = __ENV.K6_ADMIN_CLIENT_SECRET || 'change-me-local-only';
const USERNAME = __ENV.K6_USERNAME || 'demo.consumer';
const PASSWORD = __ENV.K6_PASSWORD || 'DemoConsumer123!';
const ADMIN_USERNAME = __ENV.K6_ADMIN_USERNAME || 'demo.admin';
const ADMIN_PASSWORD = __ENV.K6_ADMIN_PASSWORD || 'DemoAdmin123!';
const RACES = Number(__ENV.K6_RACES || 40);
const RUN_TAG = Date.now(); // unique idempotency keys per run: a rerun never replays a stale key

// proof_failures: races where the shared-key double checkout produced anything
// other than two 201s naming the same order — the duplicate-order (or
// loser-failed) signal this proof gates on.
const proofFailures = new Counter('proof_failures');

export const options = {
  scenarios: {
    proof: {
      executor: 'shared-iterations',
      vus: 4,
      iterations: RACES,
      maxDuration: '5m',
    },
  },
  thresholds: {
    proof_failures: ['count==0'],
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

export function setup() {
  const adminToken = grant({ grant_type: 'client_credentials', client_id: ADMIN_CLIENT, client_secret: ADMIN_SECRET });
  const demoAdmin = grant({ grant_type: 'password', client_id: SPA_CLIENT, username: ADMIN_USERNAME, password: ADMIN_PASSWORD });

  // One shared user for the whole run: the shared cart IS the contention
  // (checkout clears the cart, so racers must survive each other's clear).
  const shared = grant({ grant_type: 'password', client_id: SPA_CLIENT, username: USERNAME, password: PASSWORD });

  // Pick the first seeded product; restock generously so a sold-out 409 can't
  // masquerade as a proof failure.
  const listing = http.get(`${BASE_URL}/v1/products?limit=5`, {
    headers: { Authorization: `Bearer ${shared}` },
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

  return { productId: product.id, token: shared };
}

export default function (data) {
  const headers = {
    Authorization: `Bearer ${data.token}`,
    'Content-Type': 'application/json',
  };

  // Refill the shared cart (checkout clears it on success; the next race
  // must start populated — an empty-cart 409 would measure the harness,
  // not the DB arbiter).
  const added = http.post(
    `${BASE_URL}/v1/cart/items`,
    JSON.stringify({ product_id: data.productId, quantity: 1 }),
    { headers: headers, tags: { name: 'cart' } },
  );
  // Abort the iteration on a failed refill: racing on an empty cart would
  // 409 and be misread as an invariant failure — harness breakage must not
  // masquerade as a broken one-order-per-key guarantee.
  if (!check(added, { 'cart add 200': (r) => r.status === 200 })) {
    return;
  }

  // THE RACE: two concurrent checkouts, one key, one user — one per replica.
  // http.batch() fires both in parallel; racer 1 → :8000, racer 2 → :8001, so
  // the UNIQUE arbitration genuinely happens across two app processes.
  const idemKey = `proof-${RUN_TAG}-${__ITER}`;
  const bodies = http.batch([
    {
      method: 'POST',
      url: `${BASE_URL}/v1/checkout`,
      body: JSON.stringify({ payment_token: 'tok_visa' }),
      params: { headers: { ...headers, 'Idempotency-Key': idemKey }, tags: { name: 'checkout' } },
    },
    {
      method: 'POST',
      url: `${BASE_URL_2}/v1/checkout`,
      body: JSON.stringify({ payment_token: 'tok_visa' }),
      params: { headers: { ...headers, 'Idempotency-Key': idemKey }, tags: { name: 'checkout' } },
    },
  ]);

  const first = bodies[0];
  const second = bodies[1];

  // Both racers must succeed with 201 — the loser replays the winner's
  // stored response, never 409/500. Then both must name the SAME order:
  // exactly one order row under one key, decided by the DB.
  const ok =
    check(first, { 'racer1 checkout 201': (r) => r.status === 201 }) &&
    check(second, { 'racer2 checkout 201': (r) => r.status === 201 });

  if (!ok) {
    proofFailures.add(1);
    return;
  }

  const firstBody = first.json();
  const secondBody = second.json();
  const sameOrder = Boolean(
    firstBody && secondBody && firstBody.id && firstBody.id === secondBody.id,
  );
  if (!check(second, { 'both racers report the same order id': () => sameOrder })) {
    proofFailures.add(1);
  }
}