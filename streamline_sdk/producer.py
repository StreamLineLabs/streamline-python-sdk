"""Producer for sending messages to Streamline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Dict, List

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .exceptions import ProducerError


@dataclass
class ProducerRecord:
    """A message to be sent to Streamline.

    Attributes:
        topic: Target topic name.
        value: Message value (bytes or None).
        key: Message key (bytes or None).
        partition: Target partition (-1 for automatic).
        timestamp_ms: Message timestamp in milliseconds (None for broker time).
        headers: Message headers as key-value pairs.
    """

    topic: str
    value: Optional[bytes] = None
    key: Optional[bytes] = None
    partition: int = -1
    timestamp_ms: Optional[int] = None
    headers: Optional[Dict[str, bytes]] = None


@dataclass
class RecordMetadata:
    """Metadata about a successfully sent message.

    Attributes:
        topic: Topic the message was sent to.
        partition: Partition the message was written to.
        offset: Offset of the message in the partition.
        timestamp: Timestamp of the message.
        serialized_key_size: Size of the serialized key in bytes.
        serialized_value_size: Size of the serialized value in bytes.
    """

    topic: str
    partition: int
    offset: int
    timestamp: datetime
    serialized_key_size: int
    serialized_value_size: int


class Producer:
    """Asynchronous producer for sending messages.

    Example:
        async with client.producer as producer:
            await producer.send("topic", value=b"message")
    """

    def __init__(self, client_config: Any, producer_config: Any):
        """Initialize the producer.

        Args:
            client_config: Client configuration.
            producer_config: Producer-specific configuration.
        """
        self._client_config = client_config
        self._producer_config = producer_config
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False

    async def start(self) -> None:
        """Start the producer."""
        if self._started:
            return

        acks = self._producer_config.acks
        if acks == "all":
            acks = -1
        elif isinstance(acks, str):
            acks = int(acks)

        security_kwargs = {}
        if self._client_config.security_protocol != "PLAINTEXT":
            security_kwargs["security_protocol"] = self._client_config.security_protocol

        if self._client_config.sasl_mechanism:
            security_kwargs["sasl_mechanism"] = self._client_config.sasl_mechanism
            security_kwargs["sasl_plain_username"] = self._client_config.sasl_username
            security_kwargs["sasl_plain_password"] = self._client_config.sasl_password

        if self._client_config.ssl_cafile:
            security_kwargs["ssl_context"] = True  # Create default SSL context

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._client_config.bootstrap_servers,
            client_id=self._client_config.client_id,
            acks=acks,
            compression_type=self._producer_config.compression_type,
            max_batch_size=self._producer_config.batch_size,
            linger_ms=self._producer_config.linger_ms,
            max_request_size=self._producer_config.max_request_size,
            enable_idempotence=self._producer_config.enable_idempotence,
            **security_kwargs,
        )

        try:
            await self._producer.start()
            self._started = True
        except KafkaError as e:
            raise ProducerError(f"Failed to start producer: {e}") from e

    async def close(self) -> None:
        """Close the producer."""
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._started = False

    async def send(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> RecordMetadata:
        """Send a message to a topic.

        Args:
            topic: Target topic name.
            value: Message value.
            key: Message key (optional).
            partition: Target partition (optional, auto-assigned if not specified).
            timestamp_ms: Message timestamp in milliseconds (optional).
            headers: Message headers (optional).

        Returns:
            Metadata about the sent message.

        Raises:
            ProducerError: If sending fails.
        """
        if self._producer is None:
            raise ProducerError("Producer not started")

        # Convert headers to list of tuples
        header_list = None
        if headers:
            header_list = [(k, v) for k, v in headers.items()]

        try:
            future = await self._producer.send(
                topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=header_list,
            )
            result = await future

            return RecordMetadata(
                topic=result.topic,
                partition=result.partition,
                offset=result.offset,
                timestamp=datetime.fromtimestamp(result.timestamp / 1000),
                serialized_key_size=len(key) if key else 0,
                serialized_value_size=len(value) if value else 0,
            )
        except KafkaError as e:
            raise ProducerError(f"Failed to send message: {e}") from e

    async def send_record(self, record: ProducerRecord) -> RecordMetadata:
        """Send a ProducerRecord.

        Args:
            record: The record to send.

        Returns:
            Metadata about the sent message.
        """
        partition = record.partition if record.partition >= 0 else None
        return await self.send(
            topic=record.topic,
            value=record.value,
            key=record.key,
            partition=partition,
            timestamp_ms=record.timestamp_ms,
            headers=record.headers,
        )

    async def send_batch(
        self, records: List[ProducerRecord]
    ) -> List[RecordMetadata]:
        """Send multiple records.

        Args:
            records: List of records to send.

        Returns:
            List of metadata for each sent message.
        """
        results = []
        for record in records:
            result = await self.send_record(record)
            results.append(result)
        return results

    async def flush(self) -> None:
        """Flush all buffered messages.

        Waits for all buffered messages to be sent.
        """
        if self._producer is not None:
            await self._producer.flush()

    @property
    def is_started(self) -> bool:
        """Check if producer is started."""
        return self._started

    async def __aenter__(self) -> "Producer":
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Exit async context manager."""
        await self.close()
