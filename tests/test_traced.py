"""Tests for TracedProducer and TracedConsumer wrappers."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from streamline_sdk.producer import ProducerRecord, RecordMetadata
from streamline_sdk.traced import TracedProducer, TracedConsumer


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_mock_producer() -> AsyncMock:
    """Create an AsyncMock that mimics the Producer interface."""
    producer = AsyncMock()
    producer.send = AsyncMock(
        return_value=RecordMetadata(
            topic="t",
            partition=0,
            offset=1,
            timestamp=0,
            serialized_key_size=0,
            serialized_value_size=5,
        )
    )
    producer.send_record = AsyncMock(
        return_value=RecordMetadata(
            topic="t",
            partition=0,
            offset=2,
            timestamp=0,
            serialized_key_size=0,
            serialized_value_size=5,
        )
    )
    producer.send_batch = AsyncMock(return_value=[])
    producer.flush = AsyncMock()
    producer.start = AsyncMock()
    producer.close = AsyncMock()
    producer.begin_transaction = AsyncMock()
    producer.commit_transaction = AsyncMock(return_value=[])
    producer.abort_transaction = AsyncMock()
    producer.is_started = True
    producer.in_transaction = False
    return producer


def _make_mock_consumer() -> AsyncMock:
    """Create an AsyncMock that mimics the Consumer interface."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.close = AsyncMock()
    consumer.subscribe = AsyncMock()
    consumer.unsubscribe = AsyncMock()
    consumer.poll = AsyncMock(return_value=[])
    consumer.commit = AsyncMock()
    consumer.seek = AsyncMock()
    consumer.subscription = MagicMock(return_value={"events"})
    consumer.is_started = True
    consumer.group_id = "test-group"
    return consumer


def _make_disabled_tracing() -> MagicMock:
    """Create a StreamlineTracing with tracing disabled (no-op context managers)."""
    tracing = MagicMock()
    tracing.is_enabled = False

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _noop_produce(topic, headers=None):
        yield

    @asynccontextmanager
    async def _noop_consume(topic):
        yield

    tracing.trace_produce = _noop_produce
    tracing.trace_consume = _noop_consume
    return tracing


# ── TracedProducer tests ──────────────────────────────────────────────────


class TestTracedProducerDelegation:
    """TracedProducer delegates all operations to the wrapped producer."""

    @pytest.mark.asyncio
    async def test_send_delegates_to_producer(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        result = await traced.send("my-topic", value=b"hello", key=b"k1")

        producer.send.assert_awaited_once_with(
            "my-topic",
            value=b"hello",
            key=b"k1",
            partition=None,
            timestamp_ms=None,
            headers=None,
        )
        assert result.topic == "t"

    @pytest.mark.asyncio
    async def test_send_with_all_params(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        headers = {"x-id": b"abc"}
        await traced.send(
            "orders",
            value=b"data",
            key=b"key",
            partition=2,
            timestamp_ms=1000,
            headers=headers,
        )

        producer.send.assert_awaited_once_with(
            "orders",
            value=b"data",
            key=b"key",
            partition=2,
            timestamp_ms=1000,
            headers=headers,
        )

    @pytest.mark.asyncio
    async def test_send_record_delegates_to_producer(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        record = ProducerRecord(topic="t", value=b"v")
        await traced.send_record(record)

        producer.send_record.assert_awaited_once_with(record)

    @pytest.mark.asyncio
    async def test_send_batch_delegates_to_producer(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        records = [ProducerRecord(topic="t", value=b"a")]
        await traced.send_batch(records)

        producer.send_batch.assert_awaited_once_with(records)

    @pytest.mark.asyncio
    async def test_flush_delegates(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        await traced.flush()
        producer.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_delegates(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        await traced.start()
        producer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_delegates(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        await traced.close()
        producer.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_transaction_methods_delegate(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        await traced.begin_transaction()
        producer.begin_transaction.assert_awaited_once()

        await traced.commit_transaction()
        producer.commit_transaction.assert_awaited_once()

        await traced.abort_transaction()
        producer.abort_transaction.assert_awaited_once()

    def test_is_started_delegates(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        assert traced.is_started is True

    def test_in_transaction_delegates(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        assert traced.in_transaction is False


class TestTracedProducerTracing:
    """TracedProducer invokes tracing context managers."""

    @pytest.mark.asyncio
    async def test_send_calls_trace_produce(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        await traced.send("orders", value=b"data", headers={"x": b"1"})

        # trace_produce was replaced by our helper but we can verify
        # by replacing it with a tracking version
        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_produce(topic, headers=None):
            calls.append(("produce", topic, headers))
            yield

        tracing.trace_produce = tracking_produce
        await traced.send("events", value=b"v2", headers={"h": b"2"})

        assert len(calls) == 1
        assert calls[0][0] == "produce"
        assert calls[0][1] == "events"
        assert calls[0][2] == {"h": b"2"}

    @pytest.mark.asyncio
    async def test_send_batch_calls_trace_produce(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_produce(topic, headers=None):
            calls.append(("produce", topic))
            yield

        tracing.trace_produce = tracking_produce
        records = [ProducerRecord(topic="batch-topic", value=b"x")]
        await traced.send_batch(records)

        assert len(calls) == 1
        assert calls[0][1] == "batch-topic"

    @pytest.mark.asyncio
    async def test_send_batch_empty_uses_unknown_topic(self):
        producer = _make_mock_producer()
        tracing = _make_disabled_tracing()
        traced = TracedProducer(producer, tracing)

        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_produce(topic, headers=None):
            calls.append(topic)
            yield

        tracing.trace_produce = tracking_produce
        await traced.send_batch([])

        assert calls == ["unknown"]


class TestTracedProducerContextManager:
    """TracedProducer works as an async context manager."""

    @pytest.mark.asyncio
    async def test_async_with_calls_start_and_close(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer, _make_disabled_tracing())

        async with traced as tp:
            assert tp is traced
            producer.start.assert_awaited_once()

        producer.close.assert_awaited_once()


# ── TracedConsumer tests ──────────────────────────────────────────────────


class TestTracedConsumerDelegation:
    """TracedConsumer delegates all operations to the wrapped consumer."""

    @pytest.mark.asyncio
    async def test_poll_delegates_to_consumer(self):
        consumer = _make_mock_consumer()
        tracing = _make_disabled_tracing()
        traced = TracedConsumer(consumer, tracing)

        result = await traced.poll(timeout_ms=500, max_records=10)

        consumer.poll.assert_awaited_once_with(timeout_ms=500, max_records=10)
        assert result == []

    @pytest.mark.asyncio
    async def test_poll_default_params(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.poll()

        consumer.poll.assert_awaited_once_with(timeout_ms=1000, max_records=None)

    @pytest.mark.asyncio
    async def test_subscribe_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.subscribe(["t1", "t2"])
        consumer.subscribe.assert_awaited_once_with(["t1", "t2"])

    @pytest.mark.asyncio
    async def test_unsubscribe_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.unsubscribe()
        consumer.unsubscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_commit_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.commit()
        consumer.commit.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_seek_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())
        from aiokafka import TopicPartition

        tp = TopicPartition("t", 0)
        await traced.seek(tp, 42)
        consumer.seek.assert_awaited_once_with(tp, 42)

    @pytest.mark.asyncio
    async def test_start_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.start()
        consumer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        await traced.close()
        consumer.close.assert_awaited_once()

    def test_subscription_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        result = traced.subscription()
        assert result == {"events"}

    def test_is_started_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        assert traced.is_started is True

    def test_group_id_delegates(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        assert traced.group_id == "test-group"


class TestTracedConsumerTracing:
    """TracedConsumer invokes tracing context managers."""

    @pytest.mark.asyncio
    async def test_poll_calls_trace_consume(self):
        consumer = _make_mock_consumer()
        tracing = _make_disabled_tracing()
        traced = TracedConsumer(consumer, tracing)

        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_consume(topic):
            calls.append(("consume", topic))
            yield

        tracing.trace_consume = tracking_consume
        await traced.poll()

        assert len(calls) == 1
        assert calls[0] == ("consume", "events")

    @pytest.mark.asyncio
    async def test_poll_uses_sorted_topic_label(self):
        consumer = _make_mock_consumer()
        consumer.subscription = MagicMock(return_value={"z-topic", "a-topic"})
        tracing = _make_disabled_tracing()
        traced = TracedConsumer(consumer, tracing)

        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_consume(topic):
            calls.append(topic)
            yield

        tracing.trace_consume = tracking_consume
        await traced.poll()

        assert calls == ["a-topic,z-topic"]

    @pytest.mark.asyncio
    async def test_poll_uses_unknown_when_no_subscription(self):
        consumer = _make_mock_consumer()
        consumer.subscription = MagicMock(return_value=set())
        tracing = _make_disabled_tracing()
        traced = TracedConsumer(consumer, tracing)

        calls = []

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def tracking_consume(topic):
            calls.append(topic)
            yield

        tracing.trace_consume = tracking_consume
        await traced.poll()

        assert calls == ["unknown"]


class TestTracedConsumerContextManager:
    """TracedConsumer works as an async context manager."""

    @pytest.mark.asyncio
    async def test_async_with_calls_start_and_close(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer, _make_disabled_tracing())

        async with traced as tc:
            assert tc is traced
            consumer.start.assert_awaited_once()

        consumer.close.assert_awaited_once()


# ── Default tracing ───────────────────────────────────────────────────────


class TestDefaultTracing:
    """Default StreamlineTracing is created when none is provided."""

    def test_traced_producer_creates_default_tracing(self):
        producer = _make_mock_producer()
        traced = TracedProducer(producer)

        from streamline_sdk.telemetry import StreamlineTracing

        assert isinstance(traced._tracing, StreamlineTracing)

    def test_traced_consumer_creates_default_tracing(self):
        consumer = _make_mock_consumer()
        traced = TracedConsumer(consumer)

        from streamline_sdk.telemetry import StreamlineTracing

        assert isinstance(traced._tracing, StreamlineTracing)
