"""Consumer for receiving messages from Streamline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from .exceptions import ConsumerError


@dataclass
class ConsumerRecord:
    """A message received from Streamline.

    Attributes:
        topic: Topic the message was received from.
        partition: Partition the message was received from.
        offset: Offset of the message.
        key: Message key (bytes or None).
        value: Message value (bytes or None).
        timestamp: Message timestamp.
        headers: Message headers.
    """

    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Optional[bytes]
    timestamp: datetime
    headers: Dict[str, bytes]


class Consumer:
    """Asynchronous consumer for receiving messages.

    Example:
        async with client.consumer(group_id="my-group") as consumer:
            await consumer.subscribe(["my-topic"])
            async for message in consumer:
                print(f"Received: {message.value}")
    """

    def __init__(self, client_config: Any, consumer_config: Any):
        """Initialize the consumer.

        Args:
            client_config: Client configuration.
            consumer_config: Consumer-specific configuration.
        """
        self._client_config = client_config
        self._consumer_config = consumer_config
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._subscribed_topics: Set[str] = set()
        self._started = False

    async def start(self) -> None:
        """Start the consumer."""
        if self._started:
            return

        security_kwargs = {}
        if self._client_config.security_protocol != "PLAINTEXT":
            security_kwargs["security_protocol"] = self._client_config.security_protocol

        if self._client_config.sasl_mechanism:
            security_kwargs["sasl_mechanism"] = self._client_config.sasl_mechanism
            security_kwargs["sasl_plain_username"] = self._client_config.sasl_username
            security_kwargs["sasl_plain_password"] = self._client_config.sasl_password

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._client_config.bootstrap_servers,
            client_id=self._client_config.client_id,
            group_id=self._consumer_config.group_id,
            auto_offset_reset=self._consumer_config.auto_offset_reset,
            enable_auto_commit=self._consumer_config.enable_auto_commit,
            auto_commit_interval_ms=self._consumer_config.auto_commit_interval_ms,
            max_poll_records=self._consumer_config.max_poll_records,
            session_timeout_ms=self._consumer_config.session_timeout_ms,
            heartbeat_interval_ms=self._consumer_config.heartbeat_interval_ms,
            isolation_level=self._consumer_config.isolation_level,
            **security_kwargs,
        )

        try:
            await self._consumer.start()
            self._started = True
        except KafkaError as e:
            raise ConsumerError(f"Failed to start consumer: {e}") from e

    async def close(self) -> None:
        """Close the consumer."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        self._started = False
        self._subscribed_topics.clear()

    async def subscribe(self, topics: List[str]) -> None:
        """Subscribe to topics.

        Args:
            topics: List of topic names to subscribe to.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        self._consumer.subscribe(topics)
        self._subscribed_topics = set(topics)

    async def unsubscribe(self) -> None:
        """Unsubscribe from all topics."""
        if self._consumer is not None:
            self._consumer.unsubscribe()
            self._subscribed_topics.clear()

    def assign(self, partitions: List[TopicPartition]) -> None:
        """Manually assign partitions.

        Args:
            partitions: List of TopicPartition objects to assign.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        self._consumer.assign(partitions)

    async def seek(self, partition: TopicPartition, offset: int) -> None:
        """Seek to a specific offset.

        Args:
            partition: TopicPartition to seek.
            offset: Offset to seek to.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        self._consumer.seek(partition, offset)

    async def seek_to_beginning(
        self, partitions: Optional[List[TopicPartition]] = None
    ) -> None:
        """Seek to the beginning of partitions.

        Args:
            partitions: Partitions to seek (all if None).
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        if partitions is None:
            partitions = list(self._consumer.assignment())

        await self._consumer.seek_to_beginning(*partitions)

    async def seek_to_end(
        self, partitions: Optional[List[TopicPartition]] = None
    ) -> None:
        """Seek to the end of partitions.

        Args:
            partitions: Partitions to seek (all if None).
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        if partitions is None:
            partitions = list(self._consumer.assignment())

        await self._consumer.seek_to_end(*partitions)

    async def commit(self, offsets: Optional[Dict[TopicPartition, int]] = None) -> None:
        """Commit offsets.

        Args:
            offsets: Specific offsets to commit (current position if None).
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        if offsets is None:
            await self._consumer.commit()
        else:
            await self._consumer.commit(offsets)

    async def position(self, partition: TopicPartition) -> int:
        """Get current position for a partition.

        Args:
            partition: TopicPartition to get position for.

        Returns:
            Current offset position.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        return int(await self._consumer.position(partition))

    async def committed(self, partition: TopicPartition) -> Optional[int]:
        """Get committed offset for a partition.

        Args:
            partition: TopicPartition to get committed offset for.

        Returns:
            Committed offset or None.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        result = await self._consumer.committed(partition)
        return int(result) if result is not None else None

    def assignment(self) -> Set[TopicPartition]:
        """Get assigned partitions.

        Returns:
            Set of assigned TopicPartition objects.
        """
        if self._consumer is None:
            return set()

        return set(self._consumer.assignment())

    def subscription(self) -> Set[str]:
        """Get subscribed topics.

        Returns:
            Set of subscribed topic names.
        """
        return self._subscribed_topics.copy()

    async def poll(
        self, timeout_ms: int = 1000, max_records: Optional[int] = None
    ) -> List[ConsumerRecord]:
        """Poll for messages.

        Args:
            timeout_ms: Timeout in milliseconds.
            max_records: Maximum records to return.

        Returns:
            List of ConsumerRecord objects.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        records = []
        try:
            data = await self._consumer.getmany(
                timeout_ms=timeout_ms, max_records=max_records
            )

            for tp, messages in data.items():
                for msg in messages:
                    headers = {}
                    if msg.headers:
                        for key, value in msg.headers:
                            headers[key] = value

                    records.append(
                        ConsumerRecord(
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                            key=msg.key,
                            value=msg.value,
                            timestamp=datetime.fromtimestamp(msg.timestamp / 1000),
                            headers=headers,
                        )
                    )
        except KafkaError as e:
            raise ConsumerError(f"Failed to poll: {e}") from e

        return records

    async def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        """Async iterator for consuming messages.

        Yields:
            ConsumerRecord for each message.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        async for msg in self._consumer:
            headers = {}
            if msg.headers:
                for key, value in msg.headers:
                    headers[key] = value

            yield ConsumerRecord(
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
                key=msg.key,
                value=msg.value,
                timestamp=datetime.fromtimestamp(msg.timestamp / 1000),
                headers=headers,
            )

    @property
    def is_started(self) -> bool:
        """Check if consumer is started."""
        return self._started

    @property
    def group_id(self) -> Optional[str]:
        """Get the consumer group ID."""
        return str(self._consumer_config.group_id) if self._consumer_config.group_id is not None else None

    async def __aenter__(self) -> "Consumer":
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Exit async context manager."""
        await self.close()
