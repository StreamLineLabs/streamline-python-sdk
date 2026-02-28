"""Retry utilities for the Streamline SDK."""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Sequence, Type, TypeVar

from .exceptions import StreamlineError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Exceptions that are safe to retry by default
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    asyncio.TimeoutError,
)


class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_retries: Maximum number of retry attempts (0 = no retries).
        initial_backoff_ms: Initial backoff delay in milliseconds.
        max_backoff_ms: Maximum backoff delay in milliseconds.
        backoff_multiplier: Multiplier applied to backoff after each retry.
        retryable_exceptions: Exception types that should trigger a retry.
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_backoff_ms: int = 100,
        max_backoff_ms: int = 10000,
        backoff_multiplier: float = 2.0,
        retryable_exceptions: Optional[Sequence[Type[Exception]]] = None,
    ):
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if initial_backoff_ms < 0:
            raise ValueError("initial_backoff_ms must be >= 0")
        if max_backoff_ms < initial_backoff_ms:
            raise ValueError("max_backoff_ms must be >= initial_backoff_ms")
        if backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")

        self.max_retries = max_retries
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.retryable_exceptions = tuple(
            retryable_exceptions or DEFAULT_RETRYABLE_EXCEPTIONS
        )


async def retry_async(
    func: Callable[..., Any],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any,
) -> Any:
    """Execute an async function with retry logic.

    Args:
        func: Async callable to execute.
        *args: Positional arguments for the callable.
        config: Retry configuration (uses defaults if None).
        **kwargs: Keyword arguments for the callable.

    Returns:
        The return value of the callable.

    Raises:
        The last exception if all retries are exhausted.
    """
    cfg = config or RetryConfig()
    last_exception: Optional[Exception] = None
    backoff_ms = cfg.initial_backoff_ms

    for attempt in range(cfg.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except cfg.retryable_exceptions as e:
            last_exception = e
            if attempt < cfg.max_retries:
                logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %dms...",
                    attempt + 1,
                    cfg.max_retries + 1,
                    str(e),
                    backoff_ms,
                )
                await asyncio.sleep(backoff_ms / 1000.0)
                backoff_ms = min(
                    int(backoff_ms * cfg.backoff_multiplier), cfg.max_backoff_ms
                )
            else:
                logger.error(
                    "All %d attempts failed. Last error: %s",
                    cfg.max_retries + 1,
                    str(e),
                )

    raise last_exception  # type: ignore[misc]


def with_retry(config: Optional[RetryConfig] = None) -> Callable[..., Any]:
    """Decorator that adds retry logic to an async function.

    Args:
        config: Retry configuration (uses defaults if None).

    Returns:
        Decorated async function with retry behavior.

    Example::

        @with_retry(RetryConfig(max_retries=5, initial_backoff_ms=200))
        async def send_message(topic, value):
            await producer.send(topic, value=value)
    """
    cfg = config or RetryConfig()

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await retry_async(func, *args, config=cfg, **kwargs)

        return wrapper

    return decorator
