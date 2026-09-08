# Multi-stage build. Slim Python 3.13, non-root runtime, gunicorn + UvicornWorker.

# --- Builder: install deps into a venv ---
FROM python:3.13-slim AS builder

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# libmagic is needed by python-magic (Phase 7); build tools for wheels.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# --- Runtime: copy venv, run as non-root ---
FROM python:3.13-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# libmagic1 for python-magic at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends libmagic1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=appuser:appuser . .

USER appuser
EXPOSE 8000

# Async workers multiplex I/O-bound requests → worker count ≈ CPU cores, NOT (2*CPU)+1.
# CPU-bound work (Pillow) goes to the threadpool, not more processes.
# Exec-form (JSON array) is deliberate: gunicorn is PID 1 and receives ECS/Docker
# SIGTERM directly — a shell-form CMD wraps it in `sh -c`, which does not forward
# signals (ticket 15). Worker count comes from WEB_CONCURRENCY, which gunicorn
# reads natively. --timeout stays below the ALB idle timeout; --graceful-timeout
# bounds the SIGTERM drain (15s) below the ECS stopTimeout (30s default), and the
# app's SHUTDOWN_DRAIN_TIMEOUT_SECONDS (10s) sits inside that — SIGKILL therefore
# never interrupts a pool mid-close. See docs/DEPLOYMENT.md.
ENV WEB_CONCURRENCY=2
CMD ["gunicorn", "main:app", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "--timeout", "30", "--graceful-timeout", "15", "--access-logfile", "-", "--error-logfile", "-"]
