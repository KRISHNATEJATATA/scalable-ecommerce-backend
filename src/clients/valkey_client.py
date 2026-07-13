"""Async Valkey client (redis-py-compatible).

Holds ephemeral shared state only: rate-limit counters, idempotency keys, and
the JWT ``jti`` denylist (TTL = token remaining life). Not for sessions/caching.
Wired in a later phase.
"""
