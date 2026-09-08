"""Bounded retries + circuit breakers for the remote edges (payment, Keycloak).

Two primitives shared by the payment-gateway decorator and the Keycloak admin
adapter:

- :func:`retry_transient` — exponential backoff with **full jitter**, bounded by
  ``attempts`` from settings; only transient faults (connection errors,
  timeouts, provider 5xx) are retried, so a caller-fixable outcome (4xx) is
  never doubled.
- :class:`CircuitBreaker` — closed/open/half-open per dependency. An open
  breaker **fails fast** (:class:`CircuitOpenError`, a 503) instead of letting
  every caller wait out the outage, and it is observable: every transition logs
  a line and flips the ``circuit_state`` gauge (0 closed, 1 half_open, 2 open),
  so an open breaker is alertable on ``/metrics`` (docs/RUNBOOK.md §11).

The half-open probe is deliberately **one call**: several concurrent probes
against a flapping dependency would multiply its load exactly when it can least
absorb it. One probe decides: success closes, failure re-opens for another
window.

Retries sit *inside* the breaker: one logical call = N bounded attempts, and the
breaker sees a single success/failure outcome — a breaker must trip on repeated
*logical* failures, not on every in-retry blip.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

from prometheus_client import Counter, Gauge

from src.shared.errors.exceptions import DependencyUnavailableError

logger = logging.getLogger(__name__)

#: ``circuit_state{dependency=...}`` — 0 closed, 1 half_open, 2 open. Sustained
#: ``2`` is the alarm signal (runbook §11).
CIRCUIT_STATE = Gauge(
    "circuit_state",
    "Circuit breaker state per dependency (0=closed, 1=half_open, 2=open).",
    ["dependency"],
)

#: ``circuit_transitions_total{dependency, from, to}`` — a breaker flapping
#: (rapid open/close cycling) shows here as a high rate, not on the gauge.
CIRCUIT_TRANSITIONS = Counter(
    "circuit_transitions_total",
    "Circuit breaker state transitions per dependency.",
    ["dependency", "from", "to"],
)

_CLOSED, _HALF_OPEN, _OPEN = 0, 1, 2
_STATE_NAMES = {0: "closed", 1: "half_open", 2: "open"}


class CircuitOpenError(DependencyUnavailableError):
    """The breaker is open: the dependency is known-down — fail fast (503)."""


def is_transient_exception(exc: BaseException) -> bool:
    """Classify a fault as worth retrying / breaker-counting.

    Provider 5xx (and 429) carry ``response_code`` (python-keycloak's error
    objects do); connection-level faults surface as the OSError family, which
    includes ``ConnectionError`` and (since 3.11) ``TimeoutError``. Everything
    else — 4xx outcomes, programming errors — is caller-fixable or a bug and
    must propagate untouched on its first raise.
    """
    if isinstance(exc, CircuitOpenError):
        return False  # our own fail-fast must never be retried or re-counted
    code = getattr(exc, "response_code", None)
    if isinstance(code, int):
        return code >= 500 or code == 429
    return isinstance(exc, OSError)


class CircuitBreaker:
    """Per-dependency closed/open/half-open breaker (one event loop — no locks).

    Usage: ``if not breaker.allow(): fail fast`` → run the (retrying) call →
    ``record_success()`` / ``record_failure()``. Only *transient* faults may
    record failures — a 4xx answer proves the dependency is up.

    State is asyncio-atomic: the check-and-admit in :meth:`allow` and the
    transitions in the record methods contain no awaits, so concurrent callers
    on one loop can never interleave a state change mid-decision.
    """

    def __init__(self, dependency: str, *, failure_threshold: int = 5, reset_timeout_seconds: float = 30.0) -> None:
        self._dependency = dependency
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout_seconds
        self._state = _CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_calls = 0  # in-flight probes while half-open (bounded to one)
        CIRCUIT_STATE.labels(dependency).set(_CLOSED)

    @property
    def dependency(self) -> str:
        """The dependency name this breaker guards (gauge/metric label)."""
        return self._dependency

    @property
    def state(self) -> str:
        """``closed`` | ``half_open`` | ``open`` — for tests and operators."""
        return _STATE_NAMES[self._state]

    def allow(self) -> bool:
        """Entry gate. Fails fast while open; once the reset window elapses the
        breaker flips to half-open and admits exactly one probe."""
        if self._state == _CLOSED:
            return True
        if self._state == _OPEN:
            if time.monotonic() - self._opened_at < self._reset_timeout:
                return False
            self._transition(_HALF_OPEN, "reset window elapsed; admitting one probe")
        # half-open: one in-flight probe, everyone else fails fast.
        if self._half_open_calls >= 1:
            return False
        self._half_open_calls += 1
        return True

    def record_success(self) -> None:
        """A call got a definitive answer. Closes a half-open breaker; resets
        the failure streak while closed. Ignored while open: that is a
        straggler probe arriving after another probe's failure re-opened the
        breaker — one late success must not un-arm it."""
        if self._state == _OPEN:
            return
        if self._state == _HALF_OPEN:
            self._half_open_calls -= 1
            self._transition(_CLOSED, "probe succeeded")
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        """A transient fault. While half-open, the probe failed → re-open for a
        fresh window. While closed, a streak reaching the threshold opens."""
        if self._state == _OPEN:
            return  # straggler from the previous window — already counted
        if self._state == _HALF_OPEN:
            self._half_open_calls -= 1
            self._open("probe failed")
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold:
            self._open(f"{self._consecutive_failures} consecutive transient faults")

    def record_abandoned(self) -> None:
        """The admitted call was cancelled before any outcome was known.

        Cancellation carries no health signal (a saga step timeout or a
        force-exit kills calls mid-flight), so the breaker state is untouched —
        except the half-open probe slot, which **must** be released or the
        breaker wedges: with ``_half_open_calls`` stuck at 1, :meth:`allow`
        would refuse every future call even after the reset window, 503-ing the
        dependency until process restart.
        """
        if self._state == _HALF_OPEN:
            self._half_open_calls = max(0, self._half_open_calls - 1)

    def _open(self, why: str) -> None:
        self._consecutive_failures = 0  # a later re-open needs a fresh streak
        self._opened_at = time.monotonic()
        self._transition(_OPEN, why)

    def _transition(self, to: int, why: str) -> None:
        previous = self._state
        self._state = to
        CIRCUIT_STATE.labels(self._dependency).set(to)
        CIRCUIT_TRANSITIONS.labels(self._dependency, _STATE_NAMES[previous], _STATE_NAMES[to]).inc()
        log = logger.warning if to == _OPEN else logger.info
        log("circuit %s: %s -> %s (%s)", self._dependency, _STATE_NAMES[previous], _STATE_NAMES[to], why)


async def retry_transient[R](
    operation: Callable[[], Awaitable[R]],
    *,
    is_transient: Callable[[BaseException], bool] = is_transient_exception,
    attempts: int = 1,
    base_delay_seconds: float = 0.2,
    max_delay_seconds: float = 2.0,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> R:
    """Run ``operation`` with bounded exponential backoff + **full jitter**.

    ``attempts`` is the *total* number of tries (1 = no retry) — the documented
    max lives in ``AppSettings.resilience_max_attempts``. The sleep is
    ``uniform(0, min(max_delay, base * 2**(attempt-1)))``: full jitter spreads
    concurrent callers over the window so a dependency's recovery is not met by
    a thundering herd retrying in lockstep. A non-transient exception
    propagates on its first raise; a transient one retries, and after the last
    attempt the original exception propagates for the caller to translate.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:  # noqa: BLE001 — the classifier decides, CancelledError is not Exception
            if not is_transient(exc) or attempt == attempts:
                raise
            cap = min(max_delay_seconds, base_delay_seconds * 2 ** (attempt - 1))
            delay = random.uniform(0, cap)  # noqa: S311 — jitter, not a security primitive
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable: the loop either returns or raises")  # pragma: no cover
