"""Tests for the client metrics module."""

import threading
import time

import pytest

from streamline_sdk.metrics import ClientMetrics, MetricsSnapshot


class TestMetricsSnapshot:
    """Test the MetricsSnapshot dataclass."""

    def test_default_values(self):
        snapshot = MetricsSnapshot()
        assert snapshot.messages_produced == 0
        assert snapshot.messages_consumed == 0
        assert snapshot.bytes_sent == 0
        assert snapshot.bytes_received == 0
        assert snapshot.errors_total == 0
        assert snapshot.produce_latency_avg_ms == 0.0
        assert snapshot.consume_latency_avg_ms == 0.0
        assert snapshot.uptime_ms == 0.0

    def test_frozen(self):
        snapshot = MetricsSnapshot(messages_produced=10)
        with pytest.raises(AttributeError):
            snapshot.messages_produced = 20


class TestClientMetricsRecordProduce:
    """Test record_produce() behavior."""

    def test_increments_produced_count(self):
        metrics = ClientMetrics()
        metrics.record_produce(5, 1024, 10.0)

        snap = metrics.snapshot()
        assert snap.messages_produced == 5

    def test_accumulates_bytes_sent(self):
        metrics = ClientMetrics()
        metrics.record_produce(1, 256, 5.0)
        metrics.record_produce(1, 512, 3.0)

        snap = metrics.snapshot()
        assert snap.bytes_sent == 768

    def test_multiple_produces_increment(self):
        metrics = ClientMetrics()
        metrics.record_produce(3, 100, 2.0)
        metrics.record_produce(7, 200, 4.0)

        snap = metrics.snapshot()
        assert snap.messages_produced == 10
        assert snap.bytes_sent == 300

    def test_produce_latency_average(self):
        metrics = ClientMetrics()
        metrics.record_produce(1, 100, 10.0)
        metrics.record_produce(1, 100, 20.0)

        snap = metrics.snapshot()
        assert snap.produce_latency_avg_ms == pytest.approx(15.0)


class TestClientMetricsRecordConsume:
    """Test record_consume() behavior."""

    def test_increments_consumed_count(self):
        metrics = ClientMetrics()
        metrics.record_consume(10, 4096, 12.0)

        snap = metrics.snapshot()
        assert snap.messages_consumed == 10

    def test_accumulates_bytes_received(self):
        metrics = ClientMetrics()
        metrics.record_consume(1, 1024, 5.0)
        metrics.record_consume(1, 2048, 8.0)

        snap = metrics.snapshot()
        assert snap.bytes_received == 3072

    def test_consume_latency_average(self):
        metrics = ClientMetrics()
        metrics.record_consume(1, 100, 6.0)
        metrics.record_consume(1, 100, 12.0)
        metrics.record_consume(1, 100, 9.0)

        snap = metrics.snapshot()
        assert snap.consume_latency_avg_ms == pytest.approx(9.0)


class TestClientMetricsRecordError:
    """Test record_error() behavior."""

    def test_increments_error_count(self):
        metrics = ClientMetrics()
        metrics.record_error()

        snap = metrics.snapshot()
        assert snap.errors_total == 1

    def test_multiple_errors_increment(self):
        metrics = ClientMetrics()
        for _ in range(5):
            metrics.record_error()

        snap = metrics.snapshot()
        assert snap.errors_total == 5


class TestClientMetricsSnapshot:
    """Test snapshot() behavior."""

    def test_snapshot_with_zero_values(self):
        metrics = ClientMetrics()
        snap = metrics.snapshot()

        assert snap.messages_produced == 0
        assert snap.messages_consumed == 0
        assert snap.bytes_sent == 0
        assert snap.bytes_received == 0
        assert snap.errors_total == 0
        assert snap.produce_latency_avg_ms == 0.0
        assert snap.consume_latency_avg_ms == 0.0

    def test_snapshot_no_division_by_zero(self):
        """Ensure latency avg is 0.0 when no records have been produced/consumed."""
        metrics = ClientMetrics()
        snap = metrics.snapshot()

        assert snap.produce_latency_avg_ms == 0.0
        assert snap.consume_latency_avg_ms == 0.0

    def test_snapshot_returns_correct_averages(self):
        metrics = ClientMetrics()
        metrics.record_produce(1, 100, 10.0)
        metrics.record_produce(1, 100, 20.0)
        metrics.record_produce(1, 100, 30.0)
        metrics.record_consume(5, 500, 5.0)

        snap = metrics.snapshot()
        assert snap.produce_latency_avg_ms == pytest.approx(20.0)
        assert snap.consume_latency_avg_ms == pytest.approx(5.0)
        assert snap.messages_produced == 3
        assert snap.messages_consumed == 5
        assert snap.bytes_sent == 300
        assert snap.bytes_received == 500


class TestClientMetricsUptime:
    """Test uptime tracking."""

    def test_uptime_increases_over_time(self):
        metrics = ClientMetrics()
        snap1 = metrics.snapshot()
        time.sleep(0.05)
        snap2 = metrics.snapshot()

        assert snap2.uptime_ms > snap1.uptime_ms
        assert snap2.uptime_ms >= 40  # at least ~50ms minus scheduling jitter

    def test_uptime_is_positive(self):
        metrics = ClientMetrics()
        snap = metrics.snapshot()
        assert snap.uptime_ms >= 0


class TestClientMetricsReset:
    """Test the reset() method."""

    def test_reset_clears_all_counters(self):
        metrics = ClientMetrics()
        metrics.record_produce(5, 1024, 10.0)
        metrics.record_consume(10, 2048, 15.0)
        metrics.record_error()

        metrics.reset()
        snap = metrics.snapshot()

        assert snap.messages_produced == 0
        assert snap.messages_consumed == 0
        assert snap.bytes_sent == 0
        assert snap.bytes_received == 0
        assert snap.errors_total == 0
        assert snap.produce_latency_avg_ms == 0.0
        assert snap.consume_latency_avg_ms == 0.0

    def test_reset_resets_uptime(self):
        metrics = ClientMetrics()
        time.sleep(0.05)
        metrics.reset()
        snap = metrics.snapshot()
        # Uptime should be very small after reset
        assert snap.uptime_ms < 50


class TestClientMetricsThreadSafety:
    """Test thread safety of ClientMetrics."""

    def test_concurrent_produce_records(self):
        metrics = ClientMetrics()
        num_threads = 10
        ops_per_thread = 100
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(ops_per_thread):
                metrics.record_produce(1, 64, 1.0)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = metrics.snapshot()
        assert snap.messages_produced == num_threads * ops_per_thread
        assert snap.bytes_sent == num_threads * ops_per_thread * 64

    def test_concurrent_mixed_operations(self):
        metrics = ClientMetrics()
        num_threads = 5
        ops_per_thread = 50
        barrier = threading.Barrier(num_threads * 3)

        def producer():
            barrier.wait()
            for _ in range(ops_per_thread):
                metrics.record_produce(1, 100, 2.0)

        def consumer():
            barrier.wait()
            for _ in range(ops_per_thread):
                metrics.record_consume(1, 200, 3.0)

        def error_recorder():
            barrier.wait()
            for _ in range(ops_per_thread):
                metrics.record_error()

        threads = []
        for _ in range(num_threads):
            threads.append(threading.Thread(target=producer))
            threads.append(threading.Thread(target=consumer))
            threads.append(threading.Thread(target=error_recorder))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = metrics.snapshot()
        assert snap.messages_produced == num_threads * ops_per_thread
        assert snap.messages_consumed == num_threads * ops_per_thread
        assert snap.errors_total == num_threads * ops_per_thread
