"""Traced producer and consumer wrappers that automatically instrument
every operation with OpenTelemetry spans.

Usage::

    from streamline_sdk.traced import TracedProducer, TracedConsumer

    traced_producer = TracedProducer(producer, tracing)
    await traced_producer.send("topic", value=b"data")  # Automatically traced

    traced_consumer = TracedConsumer(consumer, tracing)
    records = await traced_consumer.poll()  # Automatically traced
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional, Set

from aiokafka import TopicPartition

from .consumer import Consumer, ConsumerRecord
from .producer import Producer, ProducerRecord, RecordMetadata
from .telemetry import StreamlineTracing


class TracedProducer:
    """Producer wrapper that auto-traces every send operation.

    Wraps an existing :class:`Producer` and creates an OpenTelemetry
    PRODUCER span around each ``send`` and ``send_batch`` call.  All other
    methods are delegated unchanged.
    """

    def __init__(
        self,
        producer: Producer,
        tracing: StreamlineTracing | None = None,
    ) -> None:
        self._producer = producer
        self._tracing = tracing or StreamlineTracing()

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._producer.start()

    async def close(self) -> None:
        await self._producer.close()

    # ── Send with tracing ─────────────────────────────────────────────

    async def send(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> RecordMetadata:
        """Send a message, wrapped in a PRODUCER span."""
        async with self._tracing.trace_produce(topic, headers=headers):
            return await self._producer.send(
                topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )

    async def send_record(self, record: ProducerRecord) -> RecordMetadata:
        """Send a :class:`ProducerRecord`, wrapped in a PRODUCER span."""
        async with self._tracing.trace_produce(record.topic, headers=record.headers):
            return await self._producer.send_record(record)

    async def send_batch(
        self, records: List[ProducerRecord]
    ) -> List[RecordMetadata]:
        """Send multiple records, wrapped in a single PRODUCER span."""
        topic = records[0].topic if records else "unknown"
        async with self._tracing.trace_produce(topic):
            return await self._producer.send_batch(records)

    # ── Pass-through ──────────────────────────────────────────────────

    async def flush(self) -> None:
        await self._producer.flush()

    async def begin_transaction(self) -> None:
        await self._producer.begin_transaction()

    async def commit_transaction(self) -> List[RecordMetadata]:
        return await self._producer.commit_transaction()

    async def abort_transaction(self) -> None:
        await self._producer.abort_transaction()

    @property
    def is_started(self) -> bool:
        return self._producer.is_started

    @property
    def in_transaction(self) -> bool:
        return self._producer.in_transaction

    # ── Context manager ───────────────────────────────────────────────

    async def __aenter__(self) -> "TracedProducer":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


class TracedConsumer:
    """Consumer wrapper that auto-traces every poll operation.

    Wraps an existing :class:`Consumer` and creates an OpenTelemetry
    CONSUMER span around each ``poll`` call.  All other methods are
    delegated unchanged.
    """

    def __init__(
        self,
        consumer: Consumer,
        tracing: StreamlineTracing | None = None,
    ) -> None:
        self._consumer = consumer
        self._tracing = tracing or StreamlineTracing()

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._consumer.start()

    async def close(self) -> None:
        await self._consumer.close()

    # ── Subscription ──────────────────────────────────────────────────

    async def subscribe(self, topics: List[str]) -> None:
        await self._consumer.subscribe(topics)

    async def unsubscribe(self) -> None:
        await self._consumer.unsubscribe()

    def subscription(self) -> Set[str]:
        return self._consumer.subscription()

    # ── Poll with tracing ─────────────────────────────────────────────

    async def poll(
        self, timeout_ms: int = 1000, max_records: Optional[int] = None
    ) -> List[ConsumerRecord]:
        """Poll for messages, wrapped in a CONSUMER span."""
        topic_label = ",".join(sorted(self._consumer.subscription())) or "unknown"
        async with self._tracing.trace_consume(topic_label):
            return await self._consumer.poll(
                timeout_ms=timeout_ms, max_records=max_records
            )

    # ── Pass-through ──────────────────────────────────────────────────

    async def commit(self, offsets: Optional[Dict[TopicPartition, int]] = None) -> None:
        await self._consumer.commit(offsets)

    async def seek(self, partition: TopicPartition, offset: int) -> None:
        await self._consumer.seek(partition, offset)

    async def seek_to_beginning(
        self, partitions: Optional[List[TopicPartition]] = None
    ) -> None:
        await self._consumer.seek_to_beginning(partitions)

    async def seek_to_end(
        self, partitions: Optional[List[TopicPartition]] = None
    ) -> None:
        await self._consumer.seek_to_end(partitions)

    async def position(self, partition: TopicPartition) -> int:
        return await self._consumer.position(partition)

    async def committed(self, partition: TopicPartition) -> Optional[int]:
        return await self._consumer.committed(partition)

    def assign(self, partitions: List[TopicPartition]) -> None:
        self._consumer.assign(partitions)

    def assignment(self) -> Set[TopicPartition]:
        return self._consumer.assignment()

    @property
    def is_started(self) -> bool:
        return self._consumer.is_started

    @property
    def group_id(self) -> Optional[str]:
        return self._consumer.group_id

    # ── Async iteration ───────────────────────────────────────────────

    async def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        async for record in self._consumer:
            yield record

    # ── Context manager ───────────────────────────────────────────────

    async def __aenter__(self) -> "TracedConsumer":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
