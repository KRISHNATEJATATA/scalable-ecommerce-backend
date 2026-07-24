"""Dependency-injection wiring.

Repositories and services are instantiated once here and injected into routes
via FastAPI ``Depends``. Keeps construction in one place so layers stay decoupled
and testable. Populated as layers land (Phases 3–4).
"""
