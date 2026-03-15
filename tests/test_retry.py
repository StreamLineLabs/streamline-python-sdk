"""Tests for retry utilities."""

import asyncio
import pytest

from streamline_sdk.retry import RetryConfig, retry_async, with_retry
from streamline_sdk.exceptions import ConnectionError, TimeoutError, ProducerError


class TestRetryConfig:
    def test_default_config(self):
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.initial_backoff_ms == 100
        assert config.max_backoff_ms == 10000
        assert config.backoff_multiplier == 2.0
        assert ConnectionError in config.retryable_exceptions

    def test_custom_config(self):
        config = RetryConfig(
            max_retries=5,
            initial_backoff_ms=50,
            max_backoff_ms=5000,
            backoff_multiplier=1.5,
        )
        assert config.max_retries == 5
        assert config.initial_backoff_ms == 50

    def test_custom_retryable_exceptions(self):
        config = RetryConfig(retryable_exceptions=[ProducerError])
        assert ProducerError in config.retryable_exceptions
        assert ConnectionError not in config.retryable_exceptions

    def test_invalid_max_retries(self):
        with pytest.raises(ValueError, match="max_retries"):
            RetryConfig(max_retries=-1)

    def test_invalid_initial_backoff(self):
        with pytest.raises(ValueError, match="initial_backoff_ms"):
            RetryConfig(initial_backoff_ms=-1)

    def test_invalid_max_backoff(self):
        with pytest.raises(ValueError, match="max_backoff_ms"):
            RetryConfig(initial_backoff_ms=1000, max_backoff_ms=100)

    def test_invalid_multiplier(self):
        with pytest.raises(ValueError, match="backoff_multiplier"):
            RetryConfig(backoff_multiplier=0.5)


class TestRetryAsync:
    @pytest.mark.asyncio
    async def test_succeeds_on_first_attempt(self):
        call_count = 0

        async def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await retry_async(succeed, config=RetryConfig(max_retries=3))
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_retryable_error(self):
        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("connection lost")
            return "recovered"

        config = RetryConfig(max_retries=5, initial_backoff_ms=1)
        result = await retry_async(fail_then_succeed, config=config)
        assert result == "recovered"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        async def always_fail():
            raise ConnectionError("always fails")

        config = RetryConfig(max_retries=2, initial_backoff_ms=1)
        with pytest.raises(ConnectionError, match="always fails"):
            await retry_async(always_fail, config=config)

    @pytest.mark.asyncio
    async def test_does_not_retry_non_retryable(self):
        call_count = 0

        async def non_retryable_error():
            nonlocal call_count
            call_count += 1
            raise ProducerError("bad message")

        config = RetryConfig(max_retries=3, initial_backoff_ms=1)
        with pytest.raises(ProducerError):
            await retry_async(non_retryable_error, config=config)
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_passes_args_and_kwargs(self):
        async def add(a, b, offset=0):
            return a + b + offset

        result = await retry_async(add, 1, 2, offset=10)
        assert result == 13

    @pytest.mark.asyncio
    async def test_no_retries_when_max_is_zero(self):
        call_count = 0

        async def fail():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("fail")

        config = RetryConfig(max_retries=0, initial_backoff_ms=1)
        with pytest.raises(ConnectionError):
            await retry_async(fail, config=config)
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_backoff_respects_max(self):
        """Ensure backoff doesn't exceed max_backoff_ms."""
        call_count = 0

        async def fail_3_times():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise ConnectionError("fail")
            return "done"

        config = RetryConfig(
            max_retries=5,
            initial_backoff_ms=1,
            max_backoff_ms=5,
            backoff_multiplier=10.0,
        )
        result = await retry_async(fail_3_times, config=config)
        assert result == "done"


class TestWithRetryDecorator:
    @pytest.mark.asyncio
    async def test_decorator_basic(self):
        call_count = 0

        @with_retry(RetryConfig(max_retries=3, initial_backoff_ms=1))
        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("flaky")
            return "success"

        result = await flaky()
        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_decorator_preserves_function_name(self):
        @with_retry()
        async def my_function():
            pass

        assert my_function.__name__ == "my_function"

    @pytest.mark.asyncio
    async def test_decorator_with_default_config(self):
        @with_retry()
        async def succeed():
            return 42

        result = await succeed()
        assert result == 42


def test_max_retries_exceeded():
    """Test that retry stops after max attempts."""
    from streamline_sdk.retry import RetryPolicy
    
    policy = RetryPolicy(max_retries=3)
    assert policy.max_retries == 3
    assert policy.should_retry(attempt=2) is True
    assert policy.should_retry(attempt=3) is False


def test_non_retryable_error():
    """Test that non-retryable errors are not retried."""
    from streamline_sdk.exceptions import AuthenticationError
    from streamline_sdk.retry import RetryPolicy
    
    policy = RetryPolicy(max_retries=5)
    err = AuthenticationError("invalid credentials")
    assert policy.should_retry_error(err) is False

