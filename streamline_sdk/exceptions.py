"""Exceptions for the Streamline SDK."""
from typing import Any, Optional


class StreamlineError(Exception):
    """Base exception for Streamline SDK errors."""

    def __init__(self, *args: Any, hint: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.hint = hint

    def __str__(self) -> str:
        s = super().__str__()
        if self.hint:
            s += f" (hint: {self.hint})"
        return s


class ConnectionError(StreamlineError):
    """Error connecting to Streamline."""

    def __init__(self, *args: Any, hint: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(
            *args,
            hint=hint or "Check that Streamline server is running and accessible",
            **kwargs,
        )


class ProducerError(StreamlineError):
    """Error in producer operations."""
    pass


class ConsumerError(StreamlineError):
    """Error in consumer operations."""
    pass


class TopicError(StreamlineError):
    """Error in topic operations."""

    def __init__(self, *args: Any, hint: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(
            *args,
            hint=hint or "Use admin client to create the topic first, or enable auto-creation",
            **kwargs,
        )


class AuthenticationError(StreamlineError):
    """Error in authentication."""

    def __init__(self, *args: Any, hint: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(
            *args,
            hint=hint or "Verify your SASL credentials and mechanism",
            **kwargs,
        )


class AuthorizationError(StreamlineError):
    """Error in authorization (ACL denied)."""
    pass


class SerializationError(StreamlineError):
    """Error in message serialization/deserialization."""
    pass


class TimeoutError(StreamlineError):
    """Operation timed out."""

    def __init__(self, *args: Any, hint: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(
            *args,
            hint=hint or "Consider increasing timeout settings or checking server load",
            **kwargs,
        )