"""Application entry point.

Thin re-export of the FastAPI app factory (``src.app.create_app``). Run with::

    uvicorn main:app --reload

or under gunicorn::

    gunicorn -k uvicorn.workers.UvicornWorker main:app
"""

from src.app import create_app

app = create_app()
