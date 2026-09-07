"""Producer for sending messages to Streamline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from ._security import build_security_kwargs
from .circuit_breaker import CircuitBreakerOpen
from .exceptions import (
    ConnectionError as _ConnectionError,
)
from .exceptions import (
    ProducerError,
)
from .exceptions import (
    TimeoutError as _TimeoutError,
)
from .validation import validate_topic_name

if TYPE_CHECKING:
    from .circuit_breaker import CircuitBreaker

_RETRYABLE_EXCEPTIONS = (_ConnectionError, _TimeoutError, OSError, asyncio.TimeoutError)


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
    value: bytes | None = None
    key: bytes | None = None
    partition: int = -1
    timestamp_ms: int | None = None
    headers: dict[str, bytes] | None = None


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

    .. important::
        ``begin_transaction`` / ``commit_transaction`` / ``abort_transaction``
        implement **client-buffered, non-atomic pseudo-transactions**. The
        Streamline broker (via the Kafka wire protocol used here) provides no
        transactional coordinator, so "committing" a transaction means the
        client buffers ``send``/``send_record`` calls locally and then
        replays them as ordinary, independent produce requests. There is
        **no all-or-nothing guarantee**: if the broker connection fails
        partway through a commit, some buffered messages may have already
        been delivered while later ones are not. Do not rely on this
        mechanism for cross-message atomicity or exactly-once semantics.
    """

    def __init__(
        self,
        client_config: Any,
        producer_config: Any,
        *,
        circuit_breaker: CircuitBreaker | None = None,
        telemetry: Any | None = None,
    ):
        """Initialize the producer.

        Args:
            client_config: Client configuration.
            producer_config: Producer-specific configuration.
            circuit_breaker: Optional circuit breaker for resilience.
            telemetry: Optional StreamlineTracing instance for OTel tracing.
        """
        self._client_config = client_config
        self._producer_config = producer_config
        self._circuit_breaker = circuit_breaker
        self._telemetry = telemetry
        self._producer: AIOKafkaProducer | None = None
        self._started = False
        self._in_transaction = False
        self._transaction_buffer: list[ProducerRecord] = []

    async def start(self) -> None:
        """Start the producer."""
        if self._started:
            return

        acks = self._producer_config.acks
        if acks == "all":
            acks = -1
        elif isinstance(acks, str):
            acks = int(acks)

        security_kwargs = build_security_kwargs(self._client_config)

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
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: dict[str, bytes] | None = None,
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

        Note:
            If a client-buffered transaction is active (see
            :meth:`begin_transaction`), this call is buffered rather than
            sent to the broker — it cannot bypass the active transaction.
            The buffered send is only actually transmitted when
            :meth:`commit_transaction` is called.
        """
        if self._producer is None:
            raise ProducerError("Producer not started")

        producer = self._producer

        validate_topic_name(topic)

        if self._in_transaction:
            record = ProducerRecord(
                topic=topic,
                value=value,
                key=key,
                partition=partition if partition is not None else -1,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )
            return self._buffer_record(record)

        # Convert headers to list of tuples
        header_list = None
        if headers:
            header_list = [(k, v) for k, v in headers.items()]

        try:
            if self._circuit_breaker is not None and not self._circuit_breaker.allow():
                raise CircuitBreakerOpen()

            async def _do_send() -> RecordMetadata:
                future = await producer.send(
                    topic,
                    value=value,
                    key=key,
                    partition=partition,
                    timestamp_ms=timestamp_ms,
                    headers=header_list,
                )
                result = await future

                if self._circuit_breaker is not None:
                    self._circuit_breaker.record_success()

                return RecordMetadata(
                    topic=result.topic,
                    partition=result.partition,
                    offset=result.offset,
                    timestamp=datetime.fromtimestamp(result.timestamp / 1000),
                    serialized_key_size=len(key) if key else 0,
                    serialized_value_size=len(value) if value else 0,
                )

            if self._telemetry is not None:
                async with self._telemetry.trace_produce(topic):
                    return await _do_send()
            else:
                return await _do_send()
        except CircuitBreakerOpen:
            raise
        except KafkaError as e:
            if self._circuit_breaker is not None and isinstance(
                e.__cause__, _RETRYABLE_EXCEPTIONS
            ):
                self._circuit_breaker.record_failure()
            raise ProducerError(f"Failed to send message: {e}") from e
        except _RETRYABLE_EXCEPTIONS:
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure()
            raise

    def _buffer_record(self, record: ProducerRecord) -> RecordMetadata:
        """Append a record to the active transaction buffer.

        Returns synthetic metadata (partition/offset of ``-1``) because the
        record has not actually been sent to the broker yet — it is only
        replayed as a real send when :meth:`commit_transaction` runs.
        """
        self._transaction_buffer.append(record)
        return RecordMetadata(
            topic=record.topic,
            partition=-1,
            offset=-1,
            timestamp=datetime.fromtimestamp((record.timestamp_ms or 0) / 1000),
            serialized_key_size=len(record.key) if record.key else 0,
            serialized_value_size=len(record.value) if record.value else 0,
        )

    async def send_record(self, record: ProducerRecord) -> RecordMetadata:
        """Send a ProducerRecord.

        If a client-buffered transaction is in progress, the record is
        buffered instead of being sent immediately (see :meth:`send`, which
        this delegates to and which enforces the same buffering so a
        transaction can never be bypassed).

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

    async def send_batch(self, records: list[ProducerRecord]) -> list[RecordMetadata]:
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

    async def begin_transaction(self) -> None:
        """Begin a new client-buffered (non-atomic) pseudo-transaction.

        Every :meth:`send` / :meth:`send_record` call made after this call —
        and until :meth:`commit_transaction` or :meth:`abort_transaction` is
        called — is buffered on the client instead of being transmitted to
        the broker. There is no way to bypass an active transaction: all
        send paths route through this buffer while it is active.

        This is **not** broker-side transactional atomicity — no such
        coordinator exists on the wire protocol this SDK speaks. See the
        class docstring for details.

        Raises:
            RuntimeError: If a transaction is already in progress.
        """
        if self._in_transaction:
            raise RuntimeError("Transaction already in progress")
        self._in_transaction = True
        self._transaction_buffer = []

    async def commit_transaction(self) -> list[RecordMetadata]:
        """Commit the buffered pseudo-transaction by replaying buffered sends.

        This snapshots and clears the transaction buffer and exits buffering
        mode *before* replaying the buffered records as ordinary
        (non-buffering) sends, so those replayed sends are transmitted to
        the broker rather than being re-buffered into the same list.

        .. warning::
            This is **client-buffered and non-atomic**: the Streamline
            broker provides no cross-message transactional coordinator, so
            "commit" is only a local replay of independently-sent produce
            requests. If a send fails partway through, earlier buffered
            messages may already have been delivered while later ones were
            not — there is no all-or-nothing guarantee.

        Returns:
            List of metadata for each sent message.

        Raises:
            RuntimeError: If no transaction is in progress.
        """
        if not self._in_transaction:
            raise RuntimeError("No transaction in progress")
        # Snapshot and clear the buffer, and exit buffering mode, before
        # issuing any real (non-buffering) sends. Doing this first ensures
        # send()/send_record() calls made below hit the broker instead of
        # re-appending onto the buffer they were just drained from.
        buffered_records = self._transaction_buffer
        self._transaction_buffer = []
        self._in_transaction = False

        if not buffered_records:
            return []
        return await self.send_batch(buffered_records)

    async def abort_transaction(self) -> None:
        """Abort the current transaction, discarding all buffered messages.

        Raises:
            RuntimeError: If no transaction is in progress.
        """
        if not self._in_transaction:
            raise RuntimeError("No transaction in progress")
        self._in_transaction = False
        self._transaction_buffer = []

    @property
    def in_transaction(self) -> bool:
        """Return True if a transaction is currently active."""
        return self._in_transaction

    async def __aenter__(self) -> Producer:
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
