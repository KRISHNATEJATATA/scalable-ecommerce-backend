"""
Health check endpoints.

Two probes with distinct responsibilities:
  - GET /v1/health  → liveness:   is the process alive?  (always 200)
  - GET /v1/ready   → readiness:  are critical deps reachable?  (200 / 503)
"""
