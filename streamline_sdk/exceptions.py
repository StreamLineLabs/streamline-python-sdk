"""Exceptions for the Streamline SDK."""


class StreamlineError(Exception):
    """Base exception for Streamline SDK errors."""

    def __init__(self, *args, hint: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.hint = hint

    def __str__(self):
        s = super().__str__()
        if self.hint:
            s += f" (hint: {self.hint})"
        return s


class ConnectionError(StreamlineError):
    """Error connecting to Streamline."""

    def __init__(self, *args, hint: str | None = None, **kwargs):
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

    def __init__(self, *args, hint: str | None = None, **kwargs):
        super().__init__(
            *args,
            hint=hint
            or "Use admin client to create the topic first, or enable auto-creation",
            **kwargs,
        )


class AuthenticationError(StreamlineError):
    """Error in authentication."""

    def __init__(self, *args, hint: str | None = None, **kwargs):
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

    def __init__(self, *args, hint: str | None = None, **kwargs):
        super().__init__(
            *args,
            hint=hint
            or "Consider increasing timeout settings or checking server load",
            **kwargs,
        )


class RateLimitError(StreamlineError):
    """Raised when the client is rate-limited by the broker."""

    def __init__(self, retry_after_ms: int = 0):
        self.retry_after_ms = retry_after_ms
        super().__init__(
            f"Rate limited. Retry after {retry_after_ms}ms",
            hint="Reduce request rate or increase broker rate limits",
            retryable=True,
        )


class ConfigurationError(StreamlineError):
    """Raised when a configuration value is invalid."""

    pass


class ContractViolationError(StreamlineError):
    """Raised when a record violates a topic's data contract."""
    pass


class AttestationVerificationError(StreamlineError):
    """Raised when attestation signature verification fails."""
    pass


class MemoryAccessDeniedError(StreamlineError):
    """Raised when an agent lacks permission to access memory."""
    pass


class BranchQuotaExceededError(StreamlineError):
    """Raised when a branch exceeds its storage/lifetime quota."""
    pass


class SemanticSearchUnavailableError(StreamlineError):
    """Raised when semantic search is unavailable (embedding provider down)."""

    def __init__(self, *args, hint: str | None = None, **kwargs):
        super().__init__(
            *args,
            hint=hint or "Check embedding provider connectivity and configuration",
            **kwargs,
        )
