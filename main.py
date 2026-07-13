"""Application entry point.

Thin re-export of the FastAPI app factory. The factory (``create_app``) is
implemented in Phase 1 under ``src/`` — until then this module stays importable
so tooling and ``import main`` succeed.

Run (once the factory lands)::

    uvicorn main:app --reload
"""

try:
    from src.app import create_app  # noqa: F401  (Phase 1)

    app = create_app()
except ImportError:  # pragma: no cover - factory not built yet (pre-Phase 1)
    app = None
