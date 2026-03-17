"""Tests for the OpenTelemetry telemetry module."""

from unittest.mock import MagicMock, patch, call

import pytest

from streamline_sdk.telemetry import (
    StreamlineTracing,
    _inject_context,
    _extract_context,
)


class TestStreamlineTracingInit:
    """Test StreamlineTracing initialization."""

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.trace")
    def test_enabled_with_otel_available(self, mock_trace):
        mock_tracer = MagicMock()
        mock_trace.get_tracer.return_value = mock_tracer

        tracing = StreamlineTracing()

        assert tracing.is_enabled is True
        mock_trace.get_tracer.assert_called_once_with(
            "streamline-python-sdk", "0.2.0"
        )

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.trace")
    def test_custom_tracer_name(self, mock_trace):
        StreamlineTracing(tracer_name="my-app", tracer_version="1.0.0")

        mock_trace.get_tracer.assert_called_once_with("my-app", "1.0.0")

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.trace")
    def test_explicitly_disabled(self, mock_trace):
        tracing = StreamlineTracing(enabled=False)

        assert tracing.is_enabled is False
        mock_trace.get_tracer.assert_not_called()

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", False)
    def test_disabled_when_otel_not_installed(self):
        tracing = StreamlineTracing()

        assert tracing.is_enabled is False

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", False)
    def test_requested_but_otel_not_installed_falls_back(self):
        tracing = StreamlineTracing(enabled=True)

        assert tracing.is_enabled is False


class TestTraceProduceDisabled:
    """Test trace_produce when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_trace_produce_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)
        executed = False

        async with tracing.trace_produce("test-topic"):
            executed = True

        assert executed is True

    @pytest.mark.asyncio
    async def test_trace_consume_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)
        executed = False

        async with tracing.trace_consume("test-topic"):
            executed = True

        assert executed is True

    @pytest.mark.asyncio
    async def test_trace_process_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)
        executed = False

        async with tracing.trace_process("test-topic", partition=0, offset=0):
            executed = True

        assert executed is True


class TestTraceProduceEnabled:
    """Test trace_produce with tracing enabled and OTel mocked."""

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    async def test_creates_producer_span(self, mock_trace, mock_otel_context):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        async with tracing.trace_produce("orders"):
            pass

        mock_tracer.start_span.assert_called_once()
        call_kwargs = mock_tracer.start_span.call_args
        assert call_kwargs.kwargs["name"] == "orders produce"
        attrs = call_kwargs.kwargs["attributes"]
        assert attrs["messaging.system"] == "streamline"
        assert attrs["messaging.destination.name"] == "orders"
        assert attrs["messaging.operation"] == "produce"

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    @patch("streamline_sdk.telemetry.StatusCode")
    async def test_sets_ok_status_on_success(
        self, mock_status, mock_trace, mock_otel_context
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        async with tracing.trace_produce("orders"):
            pass

        mock_span.set_status.assert_called_once_with(mock_status.OK)
        mock_span.end.assert_called_once()
        mock_otel_context.detach.assert_called_once_with("token")

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    @patch("streamline_sdk.telemetry.StatusCode")
    async def test_sets_error_status_on_exception(
        self, mock_status, mock_trace, mock_otel_context
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        with pytest.raises(ValueError, match="test error"):
            async with tracing.trace_produce("orders"):
                raise ValueError("test error")

        mock_span.set_status.assert_called_once_with(
            mock_status.ERROR, "test error"
        )
        mock_span.record_exception.assert_called_once()
        mock_span.end.assert_called_once()
        mock_otel_context.detach.assert_called_once_with("token")


class TestTraceConsumeEnabled:
    """Test trace_consume with tracing enabled."""

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    @patch("streamline_sdk.telemetry.SpanKind")
    async def test_creates_consumer_span(
        self, mock_span_kind, mock_trace, mock_otel_context
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        async with tracing.trace_consume("events"):
            pass

        call_kwargs = mock_tracer.start_span.call_args
        assert call_kwargs.kwargs["name"] == "events consume"
        assert call_kwargs.kwargs["kind"] == mock_span_kind.CONSUMER
        attrs = call_kwargs.kwargs["attributes"]
        assert attrs["messaging.operation"] == "consume"


class TestTraceProcessEnabled:
    """Test trace_process with tracing enabled."""

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry._extract_context")
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    async def test_creates_process_span_with_partition_offset(
        self, mock_trace, mock_otel_context, mock_extract
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"
        mock_extract.return_value = MagicMock()

        tracing = StreamlineTracing()

        headers = {b"traceparent": b"00-abc-def-01"}
        async with tracing.trace_process(
            "events", partition=3, offset=42, headers=headers
        ):
            pass

        call_kwargs = mock_tracer.start_span.call_args
        assert call_kwargs.kwargs["name"] == "events process"
        attrs = call_kwargs.kwargs["attributes"]
        assert attrs["messaging.destination.partition.id"] == "3"
        assert attrs["messaging.message.id"] == "42"
        assert attrs["messaging.operation"] == "process"

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    async def test_process_without_headers(self, mock_trace, mock_otel_context):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        async with tracing.trace_process("events", partition=0, offset=0):
            pass

        call_kwargs = mock_tracer.start_span.call_args
        assert call_kwargs.kwargs["context"] is None


class TestContextInjection:
    """Test _inject_context helper."""

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    def test_inject_context_adds_traceparent(self):
        mock_span = MagicMock()
        headers: dict[str, bytes] = {}

        with patch("streamline_sdk.telemetry.trace") as mock_trace, \
             patch("opentelemetry.propagate.inject") as mock_inject:

            # Simulate inject() populating the carrier
            def side_effect(carrier, context=None):
                carrier["traceparent"] = "00-abc-def-01"

            mock_inject.side_effect = side_effect
            mock_trace.set_span_in_context.return_value = MagicMock()

            _inject_context(mock_span, headers)

        assert headers["traceparent"] == b"00-abc-def-01"

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", False)
    def test_inject_noop_without_otel(self):
        headers: dict[str, bytes] = {}
        _inject_context(MagicMock(), headers)
        assert headers == {}


class TestContextExtraction:
    """Test _extract_context helper."""

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    def test_extract_context_decodes_headers(self):
        headers = {
            "traceparent": b"00-abc-def-01",
            "tracestate": b"vendor=value",
        }

        with patch("opentelemetry.propagate.extract") as mock_extract:
            mock_extract.return_value = MagicMock()
            result = _extract_context(headers)

        mock_extract.assert_called_once()
        carrier = mock_extract.call_args[0][0]
        assert carrier["traceparent"] == "00-abc-def-01"
        assert carrier["tracestate"] == "vendor=value"
        assert result is not None

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", False)
    def test_extract_returns_none_without_otel(self):
        result = _extract_context({"traceparent": b"00-abc-def-01"})
        assert result is None

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    def test_extract_handles_exception_gracefully(self):
        with patch("opentelemetry.propagate.extract", side_effect=Exception("boom")):
            result = _extract_context({"traceparent": b"00-abc-def-01"})

        assert result is None


class TestSyncContextManagers:
    """Test synchronous context manager variants."""

    def test_trace_produce_sync_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)
        executed = False

        with tracing.trace_produce_sync("test-topic"):
            executed = True

        assert executed is True

    def test_trace_consume_sync_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)
        executed = False

        with tracing.trace_consume_sync("test-topic"):
            executed = True

        assert executed is True

    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    @patch("streamline_sdk.telemetry.StatusCode")
    def test_sync_produce_creates_span(
        self, mock_status, mock_trace, mock_otel_context
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        with tracing.trace_produce_sync("orders"):
            pass

        call_kwargs = mock_tracer.start_span.call_args
        assert call_kwargs.kwargs["name"] == "orders produce"
        mock_span.set_status.assert_called_once_with(mock_status.OK)
        mock_span.end.assert_called_once()


class TestTracingDecorators:
    """Test traced_produce and traced_consume decorators."""

    @pytest.mark.asyncio
    async def test_traced_produce_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)

        @tracing.traced_produce("events")
        async def my_produce():
            return "produced"

        result = await my_produce()
        assert result == "produced"

    @pytest.mark.asyncio
    async def test_traced_consume_noop_when_disabled(self):
        tracing = StreamlineTracing(enabled=False)

        @tracing.traced_consume("events")
        async def my_consume():
            return "consumed"

        result = await my_consume()
        assert result == "consumed"

    @pytest.mark.asyncio
    @patch("streamline_sdk.telemetry._OTEL_AVAILABLE", True)
    @patch("streamline_sdk.telemetry.otel_context")
    @patch("streamline_sdk.telemetry.trace")
    @patch("streamline_sdk.telemetry.StatusCode")
    async def test_traced_produce_creates_span(
        self, mock_status, mock_trace, mock_otel_context
    ):
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        mock_trace.get_tracer.return_value = mock_tracer
        mock_trace.set_span_in_context.return_value = MagicMock()
        mock_otel_context.attach.return_value = "token"

        tracing = StreamlineTracing()

        @tracing.traced_produce("events")
        async def my_produce():
            return "produced"

        result = await my_produce()
        assert result == "produced"
        mock_tracer.start_span.assert_called_once()
