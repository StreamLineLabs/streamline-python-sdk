"""Client-side metrics for monitoring Streamline SDK health and performance."""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field


@dataclass(frozen=True)
class MetricsSnapshot:
    """Point-in-time snapshot of client metrics."""

    messages_produced: int = 0
    messages_consumed: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors_total: int = 0
    produce_latency_avg_ms: float = 0.0
    consume_latency_avg_ms: float = 0.0
    uptime_ms: float = 0.0


class ClientMetrics:
    """Thread-safe client metrics collector.

    Tracks production, consumption, and error metrics for observability.

    Example::

        metrics = ClientMetrics()
        metrics.record_produce(1, 256, 5.2)
        metrics.record_consume(10, 4096, 12.1)
        print(metrics.snapshot())
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._messages_produced = 0
        self._messages_consumed = 0
        self._bytes_sent = 0
        self._bytes_received = 0
        self._errors_total = 0
        self._produce_latency_sum = 0.0
        self._produce_latency_count = 0
        self._consume_latency_sum = 0.0
        self._consume_latency_count = 0
        self._start_time = time.monotonic()

    def record_produce(self, message_count: int, nbytes: int, latency_ms: float) -> None:
        """Record a successful produce operation."""
        with self._lock:
            self._messages_produced += message_count
            self._bytes_sent += nbytes
            self._produce_latency_sum += latency_ms
            self._produce_latency_count += 1

    def record_consume(self, message_count: int, nbytes: int, latency_ms: float) -> None:
        """Record a successful consume operation."""
        with self._lock:
            self._messages_consumed += message_count
            self._bytes_received += nbytes
            self._consume_latency_sum += latency_ms
            self._consume_latency_count += 1

    def record_error(self) -> None:
        """Record an error occurrence."""
        with self._lock:
            self._errors_total += 1

    def reset(self) -> None:
        """Reset all metrics to zero."""
        with self._lock:
            self._messages_produced = 0
            self._messages_consumed = 0
            self._bytes_sent = 0
            self._bytes_received = 0
            self._errors_total = 0
            self._produce_latency_sum = 0.0
            self._produce_latency_count = 0
            self._consume_latency_sum = 0.0
            self._consume_latency_count = 0
            self._start_time = time.monotonic()

    def snapshot(self) -> MetricsSnapshot:
        """Return a point-in-time snapshot of all metrics."""
        with self._lock:
            produce_avg = (
                self._produce_latency_sum / self._produce_latency_count
                if self._produce_latency_count > 0
                else 0.0
            )
            consume_avg = (
                self._consume_latency_sum / self._consume_latency_count
                if self._consume_latency_count > 0
                else 0.0
            )
            return MetricsSnapshot(
                messages_produced=self._messages_produced,
                messages_consumed=self._messages_consumed,
                bytes_sent=self._bytes_sent,
                bytes_received=self._bytes_received,
                errors_total=self._errors_total,
                produce_latency_avg_ms=produce_avg,
                consume_latency_avg_ms=consume_avg,
                uptime_ms=(time.monotonic() - self._start_time) * 1000,
            )
