"""OpenTelemetry integration for Streamline Python SDK.

Provides automatic tracing for produce and consume operations when
``opentelemetry-api`` is installed. Gracefully degrades to a no-op
when the package is not available.

Install with::

    pip install streamline-sdk[telemetry]

Span conventions follow the OTel semantic conventions for messaging:

- Span name: ``{topic} {operation}`` (e.g., "orders produce")
- Attributes: messaging.system=streamline, messaging.destination.name={topic},
  messaging.operation={produce|consume}
- Kind: PRODUCER for produce, CONSUMER for consume

Example usage::

    from streamline_sdk.telemetry import StreamlineTracing

    tracing = StreamlineTracing()

    # As a context manager
    async with tracing.trace_produce("my-topic"):
        await producer.send("my-topic", value=b"hello")

    # As a decorator
    @tracing.traced_consume("events")
    async def handle_messages(records):
        for record in records:
            process(record)
"""

from __future__ import annotations

import functools
import logging
from contextlib import asynccontextmanager, contextmanager
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    Optional,
    TypeVar,
)

logger = logging.getLogger(__name__)

# Try to import OpenTelemetry; set a flag for availability.
try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.context import Context
    from opentelemetry.trace import (
        SpanKind,
        StatusCode,
        Tracer,
    )
    from opentelemetry.trace.propagation import get_current_span

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

T = TypeVar("T")


class StreamlineTracing:
    """OpenTelemetry tracing wrapper for Streamline operations.

    When OpenTelemetry is not installed, all methods are no-ops.

    Args:
        tracer_name: Name of the instrumentation scope (default: "streamline-python-sdk").
        tracer_version: Version of the instrumentation scope (default: "0.2.0").
        enabled: Explicitly enable/disable tracing. ``None`` means auto-detect.
    """

    def __init__(
        self,
        tracer_name: str = "streamline-python-sdk",
        tracer_version: str = "0.2.0",
        enabled: Optional[bool] = None,
    ) -> None:
        self._enabled = enabled if enabled is not None else _OTEL_AVAILABLE
        self._tracer: Any = None

        if self._enabled and _OTEL_AVAILABLE:
            self._tracer = trace.get_tracer(tracer_name, tracer_version)
            logger.debug("OpenTelemetry tracing enabled (scope=%s)", tracer_name)
        elif self._enabled and not _OTEL_AVAILABLE:
            logger.warning(
                "OpenTelemetry tracing requested but opentelemetry-api is not installed; "
                "install with: pip install streamline-sdk[telemetry]"
            )
            self._enabled = False
        else:
            logger.debug("OpenTelemetry tracing disabled")

    @property
    def is_enabled(self) -> bool:
        """Return whether tracing is active."""
        return self._enabled

    # ── Context managers ──────────────────────────────────────────────

    @asynccontextmanager
    async def trace_produce(
        self,
        topic: str,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> AsyncIterator[None]:
        """Async context manager that creates a PRODUCER span.

        Injects trace context into *headers* if provided.

        Args:
            topic: The destination topic name.
            headers: Message headers dict; trace context is injected in-place.
        """
        if not self._enabled:
            yield
            return

        span = self._tracer.start_span(
            name=f"{topic} produce",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "streamline",
                "messaging.destination.name": topic,
                "messaging.operation": "produce",
            },
        )

        # Inject trace context into headers
        if headers is not None:
            _inject_context(span, headers)

        ctx = trace.set_span_in_context(span)
        token = otel_context.attach(ctx)
        try:
            yield
            span.set_status(StatusCode.OK)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        finally:
            span.end()
            otel_context.detach(token)

    @asynccontextmanager
    async def trace_consume(
        self,
        topic: str,
    ) -> AsyncIterator[None]:
        """Async context manager that creates a CONSUMER span.

        Args:
            topic: The source topic name.
        """
        if not self._enabled:
            yield
            return

        span = self._tracer.start_span(
            name=f"{topic} consume",
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "streamline",
                "messaging.destination.name": topic,
                "messaging.operation": "consume",
            },
        )

        ctx = trace.set_span_in_context(span)
        token = otel_context.attach(ctx)
        try:
            yield
            span.set_status(StatusCode.OK)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        finally:
            span.end()
            otel_context.detach(token)

    @asynccontextmanager
    async def trace_process(
        self,
        topic: str,
        partition: int,
        offset: int,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> AsyncIterator[None]:
        """Async context manager for tracing individual record processing.

        Extracts parent trace context from *headers* when available.

        Args:
            topic: Source topic name.
            partition: Partition number.
            offset: Record offset.
            headers: Message headers (for context extraction).
        """
        if not self._enabled:
            yield
            return

        attributes = {
            "messaging.system": "streamline",
            "messaging.destination.name": topic,
            "messaging.operation": "process",
            "messaging.destination.partition.id": str(partition),
            "messaging.message.id": str(offset),
        }

        # Try to extract parent context from headers
        parent_ctx = _extract_context(headers) if headers else None

        span = self._tracer.start_span(
            name=f"{topic} process",
            kind=SpanKind.CONSUMER,
            attributes=attributes,
            context=parent_ctx,
        )

        ctx = trace.set_span_in_context(span)
        token = otel_context.attach(ctx)
        try:
            yield
            span.set_status(StatusCode.OK)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        finally:
            span.end()
            otel_context.detach(token)

    # ── Synchronous context managers ──────────────────────────────────

    @contextmanager
    def trace_produce_sync(
        self,
        topic: str,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> Iterator[None]:
        """Sync context manager variant of :meth:`trace_produce`."""
        if not self._enabled:
            yield
            return

        span = self._tracer.start_span(
            name=f"{topic} produce",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "streamline",
                "messaging.destination.name": topic,
                "messaging.operation": "produce",
            },
        )

        if headers is not None:
            _inject_context(span, headers)

        ctx = trace.set_span_in_context(span)
        token = otel_context.attach(ctx)
        try:
            yield
            span.set_status(StatusCode.OK)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        finally:
            span.end()
            otel_context.detach(token)

    @contextmanager
    def trace_consume_sync(
        self,
        topic: str,
    ) -> Iterator[None]:
        """Sync context manager variant of :meth:`trace_consume`."""
        if not self._enabled:
            yield
            return

        span = self._tracer.start_span(
            name=f"{topic} consume",
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "streamline",
                "messaging.destination.name": topic,
                "messaging.operation": "consume",
            },
        )

        ctx = trace.set_span_in_context(span)
        token = otel_context.attach(ctx)
        try:
            yield
            span.set_status(StatusCode.OK)
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        finally:
            span.end()
            otel_context.detach(token)

    # ── Decorators ────────────────────────────────────────────────────

    def traced_produce(self, topic: str) -> Callable[..., Any]:
        """Decorator that traces an async produce function.

        Args:
            topic: The destination topic name.

        Example::

            @tracing.traced_produce("events")
            async def publish(data):
                await producer.send("events", value=data)
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if not self._enabled:
                return func

            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self.trace_produce(topic):
                    return await func(*args, **kwargs)

            return wrapper

        return decorator

    def traced_consume(self, topic: str) -> Callable[..., Any]:
        """Decorator that traces an async consume function.

        Args:
            topic: The source topic name.

        Example::

            @tracing.traced_consume("events")
            async def handle(records):
                for record in records:
                    process(record)
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if not self._enabled:
                return func

            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self.trace_consume(topic):
                    return await func(*args, **kwargs)

            return wrapper

        return decorator


# ── Internal helpers ──────────────────────────────────────────────────

def _inject_context(span: Any, headers: Dict[str, bytes]) -> None:
    """Inject W3C traceparent into message headers."""
    if not _OTEL_AVAILABLE:
        return

    try:
        from opentelemetry.propagate import inject

        carrier: Dict[str, str] = {}
        ctx = trace.set_span_in_context(span)
        inject(carrier, context=ctx)

        for key, value in carrier.items():
            headers[key] = value.encode("utf-8")
    except Exception:
        logger.debug("Failed to inject trace context into headers", exc_info=True)


def _extract_context(headers: Dict[str, bytes]) -> Optional[Any]:
    """Extract trace context from message headers."""
    if not _OTEL_AVAILABLE:
        return None

    try:
        from opentelemetry.propagate import extract

        carrier: Dict[str, str] = {}
        for key, value in headers.items():
            if isinstance(value, bytes):
                carrier[key] = value.decode("utf-8", errors="replace")
            else:
                carrier[key] = str(value)

        return extract(carrier)
    except Exception:
        logger.debug("Failed to extract trace context from headers", exc_info=True)
        return None
