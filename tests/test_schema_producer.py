"""Tests for SchemaProducer, SchemaConsumer, and DeserializedRecord."""

import json
import struct
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from streamline_sdk.consumer import ConsumerRecord
from streamline_sdk.exceptions import SerializationError
from streamline_sdk.producer import RecordMetadata
from streamline_sdk.schema_producer import (
    WIRE_FORMAT_HEADER_SIZE,
    WIRE_FORMAT_MAGIC_BYTE,
    DeserializedRecord,
    SchemaConsumer,
    SchemaProducer,
)
from streamline_sdk.serializers import SchemaRegistryClient, SchemaRegistryConfig


SAMPLE_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ],
    }
)
SAMPLE_VALUE = {"id": 1, "name": "Alice"}
SAMPLE_SCHEMA_ID = 42


def _make_wire_bytes(schema_id: int, value: dict) -> bytes:
    """Build Confluent wire-format bytes for testing."""
    return b"\x00" + struct.pack(">I", schema_id) + json.dumps(value).encode("utf-8")


def _make_consumer_record(
    value: bytes,
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 0,
) -> ConsumerRecord:
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        key=None,
        value=value,
        timestamp=datetime(2024, 1, 15, 12, 0, 0),
        headers={},
    )


# ---------------------------------------------------------------------------
# Wire format helpers
# ---------------------------------------------------------------------------


class TestWireFormat:
    """Test the wire format constants and layout."""

    def test_magic_byte_value(self):
        assert WIRE_FORMAT_MAGIC_BYTE == b"\x00"

    def test_header_size(self):
        assert WIRE_FORMAT_HEADER_SIZE == 5

    def test_wire_bytes_structure(self):
        """Verify: magic byte (0x00) + 4-byte big-endian ID + JSON payload."""
        wire = _make_wire_bytes(SAMPLE_SCHEMA_ID, SAMPLE_VALUE)
        assert wire[0:1] == b"\x00"
        assert struct.unpack(">I", wire[1:5])[0] == SAMPLE_SCHEMA_ID
        assert json.loads(wire[5:]) == SAMPLE_VALUE


# ---------------------------------------------------------------------------
# SchemaProducer
# ---------------------------------------------------------------------------


class TestSchemaProducer:
    """Tests for SchemaProducer."""

    def _make_producer(
        self, schema_id: int = SAMPLE_SCHEMA_ID, auto_register: bool = True
    ) -> tuple[SchemaProducer, AsyncMock, AsyncMock]:
        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(
            return_value=RecordMetadata(
                topic="test-topic",
                partition=0,
                offset=1,
                timestamp=datetime.now(),
                serialized_key_size=0,
                serialized_value_size=0,
            )
        )

        mock_registry = AsyncMock(spec=SchemaRegistryClient)
        mock_registry.register_schema = AsyncMock(return_value=schema_id)

        sp = SchemaProducer(
            producer=mock_producer,
            schema_registry=mock_registry,
            subject="test-topic-value",
            schema=SAMPLE_SCHEMA,
            auto_register=auto_register,
        )
        return sp, mock_producer, mock_registry

    @pytest.mark.asyncio
    async def test_send_serializes_to_wire_format(self):
        sp, mock_producer, _ = self._make_producer()

        await sp.send("test-topic", SAMPLE_VALUE)

        mock_producer.send.assert_called_once()
        call_kwargs = mock_producer.send.call_args
        sent_bytes = call_kwargs.kwargs.get("value") or call_kwargs[1].get("value")

        assert sent_bytes[0:1] == b"\x00"
        assert struct.unpack(">I", sent_bytes[1:5])[0] == SAMPLE_SCHEMA_ID
        assert json.loads(sent_bytes[5:]) == SAMPLE_VALUE

    @pytest.mark.asyncio
    async def test_send_registers_schema_once(self):
        sp, _, mock_registry = self._make_producer()

        await sp.send("test-topic", SAMPLE_VALUE)
        await sp.send("test-topic", SAMPLE_VALUE)

        mock_registry.register_schema.assert_called_once_with(
            "test-topic-value", SAMPLE_SCHEMA, "AVRO"
        )

    @pytest.mark.asyncio
    async def test_send_passes_key_and_headers(self):
        sp, mock_producer, _ = self._make_producer()
        headers = {"trace-id": b"abc"}

        await sp.send("test-topic", SAMPLE_VALUE, key=b"key1", headers=headers)

        call_kwargs = mock_producer.send.call_args
        assert call_kwargs.kwargs.get("key") or call_kwargs[1].get("key") == b"key1"
        assert call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers") == headers

    @pytest.mark.asyncio
    async def test_send_auto_register_disabled_raises(self):
        sp, _, _ = self._make_producer(auto_register=False)

        with pytest.raises(SerializationError, match="auto_register is disabled"):
            await sp.send("test-topic", SAMPLE_VALUE)

    @pytest.mark.asyncio
    async def test_send_non_serializable_value_raises(self):
        sp, _, _ = self._make_producer()

        with pytest.raises(SerializationError, match="Failed to serialize"):
            await sp.send("test-topic", object())

    @pytest.mark.asyncio
    async def test_send_returns_record_metadata(self):
        sp, _, _ = self._make_producer()
        result = await sp.send("test-topic", SAMPLE_VALUE)
        assert isinstance(result, RecordMetadata)
        assert result.topic == "test-topic"


# ---------------------------------------------------------------------------
# SchemaConsumer
# ---------------------------------------------------------------------------


class TestSchemaConsumer:
    """Tests for SchemaConsumer."""

    def _make_consumer(self) -> tuple[SchemaConsumer, AsyncMock]:
        mock_consumer = AsyncMock()
        mock_registry = AsyncMock(spec=SchemaRegistryClient)
        sc = SchemaConsumer(consumer=mock_consumer, schema_registry=mock_registry)
        return sc, mock_consumer

    @pytest.mark.asyncio
    async def test_poll_deserializes_records(self):
        sc, mock_consumer = self._make_consumer()
        wire = _make_wire_bytes(SAMPLE_SCHEMA_ID, SAMPLE_VALUE)
        mock_consumer.poll = AsyncMock(
            return_value=[_make_consumer_record(wire)]
        )

        results = await sc.poll(timeout_ms=500)

        assert len(results) == 1
        rec = results[0]
        assert isinstance(rec, DeserializedRecord)
        assert rec.value == SAMPLE_VALUE
        assert rec.schema_id == SAMPLE_SCHEMA_ID
        assert rec.topic == "test-topic"

    @pytest.mark.asyncio
    async def test_poll_invalid_magic_byte_raises(self):
        sc, mock_consumer = self._make_consumer()
        bad_wire = b"\x01" + struct.pack(">I", 1) + b'{"x":1}'
        mock_consumer.poll = AsyncMock(
            return_value=[_make_consumer_record(bad_wire)]
        )

        with pytest.raises(SerializationError, match="Invalid magic byte"):
            await sc.poll()

    @pytest.mark.asyncio
    async def test_poll_message_too_short_raises(self):
        sc, mock_consumer = self._make_consumer()
        mock_consumer.poll = AsyncMock(
            return_value=[_make_consumer_record(b"\x00\x01")]
        )

        with pytest.raises(SerializationError, match="too short"):
            await sc.poll()

    @pytest.mark.asyncio
    async def test_poll_invalid_json_raises(self):
        sc, mock_consumer = self._make_consumer()
        bad_payload = b"\x00" + struct.pack(">I", 1) + b"not-json"
        mock_consumer.poll = AsyncMock(
            return_value=[_make_consumer_record(bad_payload)]
        )

        with pytest.raises(SerializationError, match="Failed to deserialize"):
            await sc.poll()

    @pytest.mark.asyncio
    async def test_poll_forwards_timeout_and_max_records(self):
        sc, mock_consumer = self._make_consumer()
        mock_consumer.poll = AsyncMock(return_value=[])

        await sc.poll(timeout_ms=2000, max_records=10)

        mock_consumer.poll.assert_called_once_with(timeout_ms=2000, max_records=10)

    @pytest.mark.asyncio
    async def test_aiter_yields_deserialized_records(self):
        mock_consumer = MagicMock()
        wire = _make_wire_bytes(SAMPLE_SCHEMA_ID, SAMPLE_VALUE)
        record = _make_consumer_record(wire)

        async def _fake_aiter(self_inner):
            yield record

        mock_consumer.__aiter__ = _fake_aiter
        mock_registry = AsyncMock(spec=SchemaRegistryClient)
        sc = SchemaConsumer(consumer=mock_consumer, schema_registry=mock_registry)

        results = []
        async for r in sc:
            results.append(r)

        assert len(results) == 1
        assert results[0].value == SAMPLE_VALUE
        assert results[0].schema_id == SAMPLE_SCHEMA_ID


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    """Test end-to-end serialization / deserialization."""

    @pytest.mark.asyncio
    async def test_round_trip(self):
        """Produce -> capture wire bytes -> consume -> compare."""
        captured_bytes = None

        async def capture_send(topic, *, value=None, key=None, headers=None):
            nonlocal captured_bytes
            captured_bytes = value
            return RecordMetadata(
                topic=topic,
                partition=0,
                offset=0,
                timestamp=datetime.now(),
                serialized_key_size=0,
                serialized_value_size=len(value) if value else 0,
            )

        mock_producer = AsyncMock()
        mock_producer.send = AsyncMock(side_effect=capture_send)

        mock_registry = AsyncMock(spec=SchemaRegistryClient)
        mock_registry.register_schema = AsyncMock(return_value=SAMPLE_SCHEMA_ID)

        sp = SchemaProducer(
            producer=mock_producer,
            schema_registry=mock_registry,
            subject="round-trip-value",
            schema=SAMPLE_SCHEMA,
        )

        original = {"id": 99, "name": "Bob"}
        await sp.send("round-trip", original)
        assert captured_bytes is not None

        # Deserialize via SchemaConsumer
        mock_consumer = AsyncMock()
        mock_consumer.poll = AsyncMock(
            return_value=[_make_consumer_record(captured_bytes, topic="round-trip")]
        )

        sc = SchemaConsumer(consumer=mock_consumer, schema_registry=mock_registry)
        records = await sc.poll()

        assert len(records) == 1
        assert records[0].value == original
        assert records[0].schema_id == SAMPLE_SCHEMA_ID
        assert records[0].topic == "round-trip"


# ---------------------------------------------------------------------------
# DeserializedRecord
# ---------------------------------------------------------------------------


class TestDeserializedRecord:
    """Tests for the DeserializedRecord dataclass."""

    def test_fields(self):
        rec = DeserializedRecord(
            topic="t",
            partition=1,
            offset=10,
            key=b"k",
            value={"a": 1},
            schema_id=5,
            headers={"h": b"v"},
            timestamp=1700000000000,
        )
        assert rec.topic == "t"
        assert rec.partition == 1
        assert rec.offset == 10
        assert rec.key == b"k"
        assert rec.value == {"a": 1}
        assert rec.schema_id == 5
        assert rec.headers == {"h": b"v"}
        assert rec.timestamp == 1700000000000

    def test_optional_fields_default_none(self):
        rec = DeserializedRecord(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={},
            schema_id=1,
            headers=None,
            timestamp=None,
        )
        assert rec.key is None
        assert rec.headers is None
        assert rec.timestamp is None
