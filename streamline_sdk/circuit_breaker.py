"""Circuit breaker pattern for the Streamline SDK.

Implements the circuit breaker pattern with three states:
- CLOSED: Requests flow normally. Failures are counted.
- OPEN: Requests are rejected immediately. After a timeout, transitions to HALF_OPEN.
- HALF_OPEN: A limited number of probe requests are allowed. Successes close the
  circuit; any failure reopens it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Type

from .exceptions import StreamlineError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker state."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerConfig:
    """Configuration for the circuit breaker.

    Attributes:
        failure_threshold: Number of consecutive failures before opening the circuit.
        success_threshold: Consecutive successes in half-open state to close the circuit.
        open_timeout_s: Seconds to wait before transitioning from open to half-open.
        half_open_max_requests: Maximum probe requests allowed in half-open state.
        retryable_exceptions: Exception types that count as failures.
        on_state_change: Optional callback when state changes.
    """

    failure_threshold: int = 5
    success_threshold: int = 2
    open_timeout_s: float = 30.0
    half_open_max_requests: int = 3
    retryable_exceptions: tuple[Type[Exception], ...] = field(
        default_factory=lambda: (ConnectionError, TimeoutError, OSError, asyncio.TimeoutError)
    )
    on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None


class CircuitBreakerOpen(StreamlineError):
    """Raised when the circuit breaker is open and rejecting requests."""

    def __init__(self) -> None:
        super().__init__(
            "Circuit breaker is open — too many recent failures. "
            "The client will retry automatically after the cooldown period.",
            hint="The server may be down or overloaded. Check server health with: "
            "streamline-cli doctor --check connectivity",
        )
        self.retryable = True


class CircuitBreaker:
    """Async-safe circuit breaker for resilient Streamline operations.

    Usage::

        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        async def send_with_breaker(topic: str, value: bytes) -> None:
            if not cb.allow():
                raise CircuitBreakerOpen()
            try:
                await client.producer.send(topic, value=value)
                cb.record_success()
            except ConnectionError:
                cb.record_failure()
                raise
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_count = 0
        self._last_failure_at: float = 0.0
        self._last_state_change: float = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit breaker state (may auto-transition from OPEN to HALF_OPEN)."""
        if (
            self._state == CircuitState.OPEN
            and time.monotonic() - self._last_failure_at >= self._config.open_timeout_s
        ):
            self._transition(CircuitState.HALF_OPEN)
        return self._state

    def allow(self) -> bool:
        """Check if a request should be allowed through.

        Returns:
            True if the request can proceed, False if the circuit is open.
        """
        current = self.state  # triggers auto-transition check

        if current == CircuitState.CLOSED:
            return True
        elif current == CircuitState.OPEN:
            return False
        elif current == CircuitState.HALF_OPEN:
            if self._half_open_count < self._config.half_open_max_requests:
                self._half_open_count += 1
                return True
            return False
        return True

    def record_success(self) -> None:
        """Record a successful operation."""
        if self._state == CircuitState.CLOSED:
            self._failure_count = 0
        elif self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self._config.success_threshold:
                self._transition(CircuitState.CLOSED)

    def record_failure(self) -> None:
        """Record a failed operation (only for retryable errors)."""
        self._last_failure_at = time.monotonic()

        if self._state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self._config.failure_threshold:
                self._transition(CircuitState.OPEN)
        elif self._state == CircuitState.HALF_OPEN:
            self._transition(CircuitState.OPEN)

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        self._transition(CircuitState.CLOSED)

    @property
    def counts(self) -> tuple[int, int]:
        """Return (failure_count, success_count)."""
        return self._failure_count, self._success_count

    def _transition(self, to: CircuitState) -> None:
        from_state = self._state
        if from_state == to:
            return

        self._state = to
        self._last_state_change = time.monotonic()
        self._failure_count = 0
        self._success_count = 0
        self._half_open_count = 0

        logger.info("Circuit breaker: %s -> %s", from_state.value, to.value)

        if self._config.on_state_change is not None:
            self._config.on_state_change(from_state, to)
