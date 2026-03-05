"""Tests for the circuit breaker module."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
    CircuitState,
)


class TestCircuitBreakerStates:
    """Test circuit breaker state transitions."""

    def test_starts_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow() is True

    def test_opens_after_failure_threshold(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        for _ in range(3):
            cb.allow()
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow() is False

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, open_timeout_s=0.05)
        )
        cb.allow()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow() is True

    def test_closes_on_half_open_success(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=1,
                success_threshold=1,
                open_timeout_s=0.05,
            )
        )
        cb.allow()
        cb.record_failure()
        time.sleep(0.06)

        cb.allow()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_reopens_on_half_open_failure(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=1,
                success_threshold=2,
                open_timeout_s=0.05,
            )
        )
        cb.allow()
        cb.record_failure()
        time.sleep(0.06)

        cb.allow()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN


class TestCircuitBreakerBehavior:
    """Test circuit breaker operational behavior."""

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        cb.allow()
        cb.record_failure()
        cb.allow()
        cb.record_failure()
        # Success resets count
        cb.allow()
        cb.record_success()
        # Two more failures — should not trip (count was reset)
        cb.allow()
        cb.record_failure()
        cb.allow()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED

    def test_half_open_limits_requests(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=1,
                open_timeout_s=0.05,
                half_open_max_requests=2,
            )
        )
        cb.allow()
        cb.record_failure()
        time.sleep(0.06)

        assert cb.allow() is True
        assert cb.allow() is True
        assert cb.allow() is False

    def test_reset(self):
        cb = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, open_timeout_s=600)
        )
        cb.allow()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow() is True

    def test_counts(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=10))
        cb.allow()
        cb.record_failure()
        cb.allow()
        cb.record_failure()
        failures, successes = cb.counts
        assert failures == 2
        assert successes == 0


class TestCircuitBreakerCallbacks:
    """Test state change callbacks."""

    def test_on_state_change_called(self):
        transitions: list[tuple[CircuitState, CircuitState]] = []
        cb = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=1,
                success_threshold=1,
                open_timeout_s=0.05,
                on_state_change=lambda f, t: transitions.append((f, t)),
            )
        )
        cb.allow()
        cb.record_failure()
        time.sleep(0.06)
        cb.allow()
        cb.record_success()

        assert transitions == [
            (CircuitState.CLOSED, CircuitState.OPEN),
            (CircuitState.OPEN, CircuitState.HALF_OPEN),
            (CircuitState.HALF_OPEN, CircuitState.CLOSED),
        ]


class TestCircuitBreakerOpen:
    """Test the CircuitBreakerOpen exception."""

    def test_is_streamline_error(self):
        exc = CircuitBreakerOpen()
        assert isinstance(exc, Exception)
        assert exc.retryable is True
        assert "circuit breaker" in str(exc).lower()


class TestProducerCircuitBreakerIntegration:
    """Test circuit breaker integration with Producer."""

    @pytest.fixture
    def circuit_breaker(self):
        return CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

    @pytest.fixture
    def producer_with_cb(self, circuit_breaker):
        from streamline_sdk.producer import Producer
        from streamline_sdk.client import ClientConfig, ProducerConfig

        return Producer(
            ClientConfig(), ProducerConfig(), circuit_breaker=circuit_breaker
        )

    @pytest.fixture
    def producer_without_cb(self):
        from streamline_sdk.producer import Producer
        from streamline_sdk.client import ClientConfig, ProducerConfig

        return Producer(ClientConfig(), ProducerConfig())

    def test_circuit_breaker_is_optional(self, producer_without_cb):
        assert producer_without_cb._circuit_breaker is None

    @pytest.mark.asyncio
    async def test_raises_circuit_breaker_open_after_threshold(
        self, producer_with_cb, circuit_breaker
    ):
        # Manually trip the breaker
        for _ in range(3):
            circuit_breaker.allow()
            circuit_breaker.record_failure()
        assert circuit_breaker.state == CircuitState.OPEN

        # send() should raise without attempting the actual send
        producer_with_cb._producer = AsyncMock()
        producer_with_cb._started = True

        with pytest.raises(CircuitBreakerOpen):
            await producer_with_cb.send("topic", value=b"msg")

        # Underlying producer.send should never have been called
        producer_with_cb._producer.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_records_success_on_send(self, producer_with_cb, circuit_breaker):
        mock_result = MagicMock(
            topic="t", partition=0, offset=1, timestamp=1000000
        )
        future = asyncio.get_event_loop().create_future()
        future.set_result(mock_result)

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(return_value=future)
        producer_with_cb._producer = mock_producer
        producer_with_cb._started = True

        await producer_with_cb.send("t", value=b"data")
        failures, successes = circuit_breaker.counts
        assert failures == 0

    @pytest.mark.asyncio
    async def test_records_failure_on_retryable_error(
        self, producer_with_cb, circuit_breaker
    ):
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(side_effect=OSError("connection lost"))
        producer_with_cb._producer = mock_producer
        producer_with_cb._started = True

        with pytest.raises(OSError):
            await producer_with_cb.send("t", value=b"data")

        failures, _ = circuit_breaker.counts
        assert failures == 1

    @pytest.mark.asyncio
    async def test_does_not_record_failure_on_non_retryable_error(
        self, producer_with_cb, circuit_breaker
    ):
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(side_effect=ValueError("bad value"))
        producer_with_cb._producer = mock_producer
        producer_with_cb._started = True

        with pytest.raises(ValueError):
            await producer_with_cb.send("t", value=b"data")

        failures, _ = circuit_breaker.counts
        assert failures == 0

    @pytest.mark.asyncio
    async def test_no_cb_does_not_interfere(self, producer_without_cb):
        """Producer without circuit breaker still raises normal errors."""
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(side_effect=OSError("connection lost"))
        producer_without_cb._producer = mock_producer
        producer_without_cb._started = True

        with pytest.raises(OSError):
            await producer_without_cb.send("t", value=b"data")


class TestConsumerCircuitBreakerIntegration:
    """Test circuit breaker integration with Consumer."""

    @pytest.fixture
    def circuit_breaker(self):
        return CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

    @pytest.fixture
    def consumer_with_cb(self, circuit_breaker):
        from streamline_sdk.consumer import Consumer
        from streamline_sdk.client import ClientConfig, ConsumerConfig

        return Consumer(
            ClientConfig(), ConsumerConfig(), circuit_breaker=circuit_breaker
        )

    @pytest.mark.asyncio
    async def test_raises_circuit_breaker_open_after_threshold(
        self, consumer_with_cb, circuit_breaker
    ):
        for _ in range(3):
            circuit_breaker.allow()
            circuit_breaker.record_failure()

        consumer_with_cb._consumer = AsyncMock()
        consumer_with_cb._started = True

        with pytest.raises(CircuitBreakerOpen):
            await consumer_with_cb.poll()

        consumer_with_cb._consumer.getmany.assert_not_called()

    @pytest.mark.asyncio
    async def test_records_success_on_poll(self, consumer_with_cb, circuit_breaker):
        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={})
        consumer_with_cb._consumer = mock_consumer
        consumer_with_cb._started = True

        result = await consumer_with_cb.poll()
        assert result == []
        failures, _ = circuit_breaker.counts
        assert failures == 0

    @pytest.mark.asyncio
    async def test_records_failure_on_retryable_error(
        self, consumer_with_cb, circuit_breaker
    ):
        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(
            side_effect=OSError("connection reset")
        )
        consumer_with_cb._consumer = mock_consumer
        consumer_with_cb._started = True

        with pytest.raises(OSError):
            await consumer_with_cb.poll()

        failures, _ = circuit_breaker.counts
        assert failures == 1
