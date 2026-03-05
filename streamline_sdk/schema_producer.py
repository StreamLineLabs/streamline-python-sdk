"""Schema-aware producer and consumer wrappers.

Provides SchemaProducer and SchemaConsumer that automatically
serialize/deserialize messages using the Streamline Schema Registry
with the Confluent wire format (magic byte + 4-byte schema ID + payload).

Usage:
    from streamline_sdk import SchemaProducer, SchemaConsumer

    schema_producer = SchemaProducer(
        producer=producer,
        schema_registry=registry,
        subject="users-value",
        schema='{"type":"record","name":"User","fields":[...]}',
    )
    await schema_producer.send("users", {"id": 1, "name": "Alice"})
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

from .consumer import Consumer, ConsumerRecord
from .exceptions import SerializationError
from .producer import Producer, RecordMetadata
from .serializers import SchemaRegistryClient

WIRE_FORMAT_MAGIC_BYTE = b"\x00"
WIRE_FORMAT_HEADER_SIZE = 5  # 1 byte magic + 4 bytes schema ID


@dataclass
class DeserializedRecord:
    """A consumer record with a deserialized value.

    Attributes:
        topic: Topic the message was received from.
        partition: Partition the message was received from.
        offset: Offset of the message.
        key: Raw message key.
        value: Deserialized Python object.
        schema_id: Schema ID extracted from the wire format header.
        headers: Message headers.
        timestamp: Message timestamp in milliseconds.
    """

    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Any
    schema_id: int
    headers: Optional[dict[str, bytes]]
    timestamp: Optional[int]


class SchemaProducer:
    """Producer that automatically serializes values using schema registry.

    Wraps an existing Producer and handles schema registration and
    wire-format serialization transparently.
    """

    def __init__(
        self,
        producer: Producer,
        schema_registry: SchemaRegistryClient,
        subject: str,
        schema: str,
        schema_type: str = "AVRO",
        auto_register: bool = True,
    ) -> None:
        self._producer = producer
        self._schema_registry = schema_registry
        self._subject = subject
        self._schema = schema
        self._schema_type = schema_type
        self._auto_register = auto_register
        self._schema_id: Optional[int] = None

    async def _ensure_registered(self) -> int:
        """Register the schema if needed and return the cached schema ID."""
        if self._schema_id is not None:
            return self._schema_id

        if not self._auto_register:
            raise SerializationError(
                "Schema not registered and auto_register is disabled"
            )

        self._schema_id = await self._schema_registry.register_schema(
            self._subject, self._schema, self._schema_type
        )
        return self._schema_id

    async def send(
        self,
        topic: str,
        value: Any,
        key: Optional[bytes] = None,
        headers: Optional[dict[str, bytes]] = None,
    ) -> RecordMetadata:
        """Serialize value to wire format and send to a topic.

        Args:
            topic: Target topic name.
            value: Python object to serialize (must be JSON-serializable).
            key: Optional message key.
            headers: Optional message headers.

        Returns:
            Metadata about the sent message.

        Raises:
            SerializationError: If serialization or schema registration fails.
        """
        schema_id = await self._ensure_registered()

        try:
            payload = json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Failed to serialize value to JSON: {e}") from e

        serialized = (
            WIRE_FORMAT_MAGIC_BYTE + struct.pack(">I", schema_id) + payload
        )

        return await self._producer.send(
            topic, value=serialized, key=key, headers=headers
        )


class SchemaConsumer:
    """Consumer that automatically deserializes values using schema registry.

    Wraps an existing Consumer and handles wire-format deserialization
    transparently, resolving schema IDs via the registry.
    """

    def __init__(
        self,
        consumer: Consumer,
        schema_registry: SchemaRegistryClient,
    ) -> None:
        self._consumer = consumer
        self._schema_registry = schema_registry

    def _deserialize_record(self, record: ConsumerRecord) -> DeserializedRecord:
        """Deserialize a single consumer record from wire format.

        Raises:
            SerializationError: If the wire format is invalid.
        """
        raw = record.value
        if raw is None or len(raw) < WIRE_FORMAT_HEADER_SIZE:
            raise SerializationError(
                f"Message too short for wire format (got {len(raw) if raw else 0} bytes, "
                f"need at least {WIRE_FORMAT_HEADER_SIZE})"
            )

        if raw[0:1] != WIRE_FORMAT_MAGIC_BYTE:
            raise SerializationError(
                f"Invalid magic byte: expected 0x00, got 0x{raw[0]:02x}"
            )

        schema_id = struct.unpack(">I", raw[1:5])[0]

        try:
            value = json.loads(raw[5:].decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(
                f"Failed to deserialize JSON payload: {e}"
            ) from e

        timestamp_ms: Optional[int] = None
        if record.timestamp is not None:
            timestamp_ms = int(record.timestamp.timestamp() * 1000)

        return DeserializedRecord(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            value=value,
            schema_id=schema_id,
            headers=record.headers if record.headers else None,
            timestamp=timestamp_ms,
        )

    async def poll(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
    ) -> list[DeserializedRecord]:
        """Poll for messages and deserialize them.

        Args:
            timeout_ms: Timeout in milliseconds.
            max_records: Maximum records to return.

        Returns:
            List of deserialized records.
        """
        raw_records = await self._consumer.poll(
            timeout_ms=timeout_ms, max_records=max_records
        )
        return [self._deserialize_record(r) for r in raw_records]

    async def __aiter__(self) -> AsyncIterator[DeserializedRecord]:
        """Async iterator that yields deserialized records."""
        async for record in self._consumer:
            yield self._deserialize_record(record)
