"""Shared polling loop for the ``service``-role SQS workers.

A worker's job is to keep draining. A transient SQS/network error on
``receive_message`` or ``delete_message`` happens outside the per-message
try/except, so without a boundary here it would escape ``run()`` and kill the
process — the container restarts, but a rolling SQS blip becomes a crash loop.
This is the same boundary the outbox relay and the reservation reaper already
have, factored out so every poller retries with backoff instead of dying.

Backoff is exponential with a cap and resets on any successful pass, so a brief
blip costs one short sleep while a sustained outage settles into slow retries
rather than a hot loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from logging import Logger


async def poll_forever(
    poll_once: Callable[[], Awaitable[int]],
    stop: asyncio.Event,
    log: Logger,
    *,
    idle_interval: float = 0.0,
    initial_backoff: float = 1.0,
    max_backoff: float = 30.0,
) -> None:
    """Call ``poll_once`` until ``stop`` is set; never let a transient error escape.

    ``idle_interval`` is the pause after an empty pass — 0 for long-polling
    consumers, which already block in ``receive_message``.
    """
    backoff = initial_backoff
    while not stop.is_set():
        try:
            handled = await poll_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # boundary: a transient SQS error must not kill the worker
            log.exception("poll failed; retrying in %.1fs", backoff)
            await _sleep_unless_stopped(stop, backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        backoff = initial_backoff
        if handled == 0 and idle_interval:
            await _sleep_unless_stopped(stop, idle_interval)


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> None:
    """Sleep ``delay``, waking early if ``stop`` is set (so SIGTERM stays prompt)."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        pass
