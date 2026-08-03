"""Tests for Producer, ProducerRecord, and RecordMetadata."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk.client import ClientConfig, ProducerConfig
from streamline_sdk.exceptions import ProducerError
from streamline_sdk.producer import Producer, ProducerRecord, RecordMetadata


class TestProducerRecord:
    """Tests for ProducerRecord dataclass."""

    def test_minimal_record(self):
        """Test creating a record with only required field."""
        record = ProducerRecord(topic="my-topic")
        assert record.topic == "my-topic"
        assert record.value is None
        assert record.key is None
        assert record.partition == -1
        assert record.timestamp_ms is None
        assert record.headers is None

    def test_record_with_value(self):
        """Test creating a record with topic and value."""
        record = ProducerRecord(topic="test", value=b"hello")
        assert record.topic == "test"
        assert record.value == b"hello"

    def test_record_with_all_fields(self):
        """Test creating a record with all fields populated."""
        headers = {"content-type": b"application/json", "trace-id": b"abc123"}
        record = ProducerRecord(
            topic="orders",
            value=b'{"item": "widget"}',
            key=b"order-42",
            partition=3,
            timestamp_ms=1700000000000,
            headers=headers,
        )
        assert record.topic == "orders"
        assert record.value == b'{"item": "widget"}'
        assert record.key == b"order-42"
        assert record.partition == 3
        assert record.timestamp_ms == 1700000000000
        assert record.headers == headers
        assert len(record.headers) == 2

    def test_record_partition_default_is_negative_one(self):
        """Default partition should be -1 (auto-assign)."""
        record = ProducerRecord(topic="t")
        assert record.partition == -1

    def test_record_with_empty_value(self):
        """Test record with empty bytes value (tombstone-like)."""
        record = ProducerRecord(topic="t", value=b"", key=b"k")
        assert record.value == b""
        assert record.key == b"k"

    def test_record_with_none_headers(self):
        """Headers default to None."""
        record = ProducerRecord(topic="t", value=b"v")
        assert record.headers is None

    def test_record_with_empty_headers(self):
        """Test record with explicitly empty headers dict."""
        record = ProducerRecord(topic="t", headers={})
        assert record.headers == {}

    def test_record_equality(self):
        """Dataclass equality should work for identical records."""
        r1 = ProducerRecord(topic="t", value=b"v", key=b"k", partition=0)
        r2 = ProducerRecord(topic="t", value=b"v", key=b"k", partition=0)
        assert r1 == r2

    def test_record_inequality(self):
        """Different records should not be equal."""
        r1 = ProducerRecord(topic="t1", value=b"v")
        r2 = ProducerRecord(topic="t2", value=b"v")
        assert r1 != r2


class TestRecordMetadata:
    """Tests for RecordMetadata dataclass."""

    def test_metadata_creation(self):
        """Test creating RecordMetadata with all fields."""
        ts = datetime(2024, 1, 15, 12, 0, 0)
        meta = RecordMetadata(
            topic="events",
            partition=2,
            offset=42,
            timestamp=ts,
            serialized_key_size=5,
            serialized_value_size=100,
        )
        assert meta.topic == "events"
        assert meta.partition == 2
        assert meta.offset == 42
        assert meta.timestamp == ts
        assert meta.serialized_key_size == 5
        assert meta.serialized_value_size == 100

    def test_metadata_zero_sizes(self):
        """Test metadata with zero key/value sizes (no key, no value)."""
        meta = RecordMetadata(
            topic="t",
            partition=0,
            offset=0,
            timestamp=datetime.now(),
            serialized_key_size=0,
            serialized_value_size=0,
        )
        assert meta.serialized_key_size == 0
        assert meta.serialized_value_size == 0

    def test_metadata_equality(self):
        """Test equality of identical metadata objects."""
        ts = datetime(2024, 6, 1)
        m1 = RecordMetadata("t", 0, 10, ts, 3, 50)
        m2 = RecordMetadata("t", 0, 10, ts, 3, 50)
        assert m1 == m2

    def test_metadata_large_offset(self):
        """Test metadata with large offset value."""
        meta = RecordMetadata(
            topic="t",
            partition=0,
            offset=9999999999,
            timestamp=datetime.now(),
            serialized_key_size=0,
            serialized_value_size=10,
        )
        assert meta.offset == 9999999999


class TestProducerInit:
    """Tests for Producer initialization and properties."""

    def test_producer_init(self):
        """Test producer can be initialized with configs."""
        client_config = ClientConfig()
        producer_config = ProducerConfig()
        producer = Producer(client_config, producer_config)
        assert producer.is_started is False
        assert producer._producer is None

    def test_producer_init_custom_configs(self):
        """Test producer with custom configurations."""
        client_config = ClientConfig(
            bootstrap_servers="broker1:9092",
            client_id="my-producer",
        )
        producer_config = ProducerConfig(
            acks=1,
            compression_type="gzip",
            batch_size=32768,
            linger_ms=50,
        )
        producer = Producer(client_config, producer_config)
        assert producer.is_started is False
        assert producer._client_config.bootstrap_servers == "broker1:9092"
        assert producer._producer_config.acks == 1
        assert producer._producer_config.compression_type == "gzip"

    def test_producer_not_started_by_default(self):
        """Producer should not be started after init."""
        producer = Producer(ClientConfig(), ProducerConfig())
        assert producer.is_started is False


class TestProducerSendErrors:
    """Tests for Producer error handling without a broker."""

    @pytest.mark.asyncio
    async def test_send_before_start_raises(self):
        """Sending before start should raise ProducerError."""
        producer = Producer(ClientConfig(), ProducerConfig())
        with pytest.raises(ProducerError, match="Producer not started"):
            await producer.send("topic", value=b"msg")

    @pytest.mark.asyncio
    async def test_flush_before_start_is_noop(self):
        """Flushing when producer is None should be a no-op."""
        producer = Producer(ClientConfig(), ProducerConfig())
        # Should not raise
        await producer.flush()

    @pytest.mark.asyncio
    async def test_close_before_start_is_noop(self):
        """Closing a producer that was never started should be safe."""
        producer = Producer(ClientConfig(), ProducerConfig())
        await producer.close()
        assert producer.is_started is False

    @pytest.mark.asyncio
    async def test_send_record_before_start_raises(self):
        """send_record should propagate ProducerError if not started."""
        producer = Producer(ClientConfig(), ProducerConfig())
        record = ProducerRecord(topic="t", value=b"v")
        with pytest.raises(ProducerError, match="Producer not started"):
            await producer.send_record(record)

    @pytest.mark.asyncio
    async def test_send_batch_before_start_raises(self):
        """send_batch should raise ProducerError if not started."""
        producer = Producer(ClientConfig(), ProducerConfig())
        records = [ProducerRecord(topic="t", value=b"v")]
        with pytest.raises(ProducerError, match="Producer not started"):
            await producer.send_batch(records)


class TestProducerSendRecord:
    """Tests for send_record partition logic."""

    @pytest.mark.asyncio
    async def test_send_record_auto_partition(self):
        """send_record with partition=-1 should pass None to send."""
        producer = Producer(ClientConfig(), ProducerConfig())
        producer._producer = MagicMock()  # fake started state

        with patch.object(producer, "send", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = RecordMetadata(
                "t", 0, 0, datetime.now(), 0, 5
            )
            record = ProducerRecord(topic="t", value=b"hello", partition=-1)
            await producer.send_record(record)
            mock_send.assert_called_once_with(
                topic="t",
                value=b"hello",
                key=None,
                partition=None,
                timestamp_ms=None,
                headers=None,
            )

    @pytest.mark.asyncio
    async def test_send_record_explicit_partition(self):
        """send_record with partition>=0 should pass it through."""
        producer = Producer(ClientConfig(), ProducerConfig())
        producer._producer = MagicMock()

        with patch.object(producer, "send", new_callable=AsyncMock) as mock_send:
            mock_send.return_value = RecordMetadata(
                "t", 2, 0, datetime.now(), 0, 5
            )
            record = ProducerRecord(topic="t", value=b"hello", partition=2)
            await producer.send_record(record)
            mock_send.assert_called_once_with(
                topic="t",
                value=b"hello",
                key=None,
                partition=2,
                timestamp_ms=None,
                headers=None,
            )
