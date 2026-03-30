"""Consumer for receiving messages from Streamline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Set, TYPE_CHECKING

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from .circuit_breaker import CircuitBreakerOpen
from .exceptions import (
    ConsumerError,
    ConnectionError as _ConnectionError,
    TimeoutError as _TimeoutError,
)

if TYPE_CHECKING:
    from .circuit_breaker import CircuitBreaker

_RETRYABLE_EXCEPTIONS = (_ConnectionError, _TimeoutError, OSError, asyncio.TimeoutError)


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

    def __init__(
        self,
        client_config: Any,
        consumer_config: Any,
        *,
        circuit_breaker: Optional[CircuitBreaker] = None,
        telemetry: Optional[Any] = None,
    ):
        """Initialize the consumer.

        Args:
            client_config: Client configuration.
            consumer_config: Consumer-specific configuration.
            circuit_breaker: Optional circuit breaker for resilience.
            telemetry: Optional StreamlineTracing instance for OTel tracing.
        """
        self._client_config = client_config
        self._consumer_config = consumer_config
        self._circuit_breaker = circuit_breaker
        self._telemetry = telemetry
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

        return await self._consumer.position(partition)

    async def committed(self, partition: TopicPartition) -> Optional[int]:
        """Get committed offset for a partition.

        Args:
            partition: TopicPartition to get committed offset for.

        Returns:
            Committed offset or None.
        """
        if self._consumer is None:
            raise ConsumerError("Consumer not started")

        return await self._consumer.committed(partition)

    def assignment(self) -> Set[TopicPartition]:
        """Get assigned partitions.

        Returns:
            Set of assigned TopicPartition objects.
        """
        if self._consumer is None:
            return set()

        return self._consumer.assignment()

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
        topic_label = ",".join(sorted(self._subscribed_topics)) or "unknown"
        try:
            if self._circuit_breaker is not None and not self._circuit_breaker.allow():
                raise CircuitBreakerOpen()

            async def _do_poll() -> None:
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

            if self._telemetry is not None:
                async with self._telemetry.trace_consume(topic_label):
                    await _do_poll()
            else:
                await _do_poll()

            if self._circuit_breaker is not None:
                self._circuit_breaker.record_success()
        except CircuitBreakerOpen:
            raise
        except KafkaError as e:
            if self._circuit_breaker is not None and isinstance(e.__cause__, _RETRYABLE_EXCEPTIONS):
                self._circuit_breaker.record_failure()
            raise ConsumerError(f"Failed to poll: {e}") from e
        except _RETRYABLE_EXCEPTIONS:
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure()
            raise

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
        return self._consumer_config.group_id

    async def search(
        self,
        topic: str,
        query: str,
        k: int = 10,
    ) -> List["SearchHit"]:
        """Search a topic using semantic search via the HTTP API.

        Args:
            topic: Target topic name.
            query: Free-text search query.
            k: Maximum number of results to return (default 10).

        Returns:
            A list of :class:`SearchHit` ordered by descending score.

        Raises:
            ConsumerError: If the search request fails.
        """
        try:
            import aiohttp
            _has_aiohttp = True
        except ImportError:
            _has_aiohttp = False

        http_url = getattr(self._client_config, "http_url", None)
        if http_url is None:
            host = self._client_config.bootstrap_servers.split(",")[0].split(":")[0]
            http_url = f"http://{host}:9094"

        url = f"{http_url}/api/v1/topics/{topic}/search"
        payload = {"query": query, "k": k}

        if _has_aiohttp:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        raise ConsumerError(
                            f"search failed: HTTP {resp.status}: {body}"
                        )
                    data = await resp.json()
        else:
            import json as _json
            import urllib.request

            req = urllib.request.Request(
                url,
                data=_json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )

            def _sync_post():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status != 200:
                        raise ConsumerError(
                            f"search failed: HTTP {resp.status}"
                        )
                    return _json.loads(resp.read())

            data = await asyncio.to_thread(_sync_post)

        return [
            SearchHit(
                topic=topic,
                partition=int(h["partition"]),
                offset=int(h["offset"]),
                score=float(h["score"]),
                value=(
                    h.get("value", "").encode()
                    if h.get("value")
                    else None
                ),
            )
            for h in data.get("hits", [])
        ]

    async def __aenter__(self) -> "Consumer":
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()


# ---------------------------------------------------------------------------
# M2 — Semantic search (Experimental)
# ---------------------------------------------------------------------------
#
# The `search()` method below issues a `POST /topics/{topic}/search` request
# to the Streamline broker's HTTP admin port (default 9094). When the broker
# negotiates Kafka API key 80 (StreamlineSearch) via ApiVersions, a future
# revision of this SDK will switch to the wire-protocol path automatically.
#
# Stability tier: Experimental. The API may change before M2 GA.

@dataclass
class SearchHit:
    """A single semantic-search result.

    Attributes:
        topic: Topic the hit came from.
        partition: Partition number.
        offset: Record offset.
        score: Similarity score (cosine; higher = more relevant).
        value: Record value (bytes) when ``include_value=True``, else ``None``.
    """

    topic: str
    partition: int
    offset: int
    score: float
    value: Optional[bytes] = None


async def search(
    bootstrap_servers: str,
    topic: str,
    query: str,
    k: int = 10,
    include_value: bool = False,
    admin_port: int = 9094,
    timeout: float = 5.0,
) -> List[SearchHit]:
    """Run a semantic search against a topic configured with ``semantic.embed=true``.

    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., ``"localhost:9092"``).
            The hostname is reused for the HTTP search endpoint.
        topic: Target topic name.
        query: Free-text query.
        k: Number of nearest neighbors (default 10).
        include_value: If True, broker rehydrates record values into the response.
        admin_port: HTTP admin port (default 9094).
        timeout: HTTP request timeout in seconds.

    Returns:
        A list of :class:`SearchHit` ordered by descending score.

    Raises:
        ConsumerError: If the topic is not configured for semantic indexing
            or the broker rejects the request.

    Example:
        >>> hits = await search("localhost:9092", "logs", "payment failure", k=5)
        >>> for h in hits:
        ...     print(h.partition, h.offset, h.score)
    """
    try:
        import aiohttp
    except ImportError as exc:
        raise ConsumerError(
            "search() requires the 'aiohttp' extra: pip install streamline-sdk[search]"
        ) from exc

    host = bootstrap_servers.split(",")[0].split(":")[0]
    url = f"http://{host}:{admin_port}/topics/{topic}/search"
    qs = "?include=value" if include_value else ""

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url + qs,
            json={"query": query, "k": k},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise ConsumerError(f"search failed: HTTP {resp.status}: {body}")
            data = await resp.json()

    return [
        SearchHit(
            topic=topic,
            partition=int(h["partition"]),
            offset=int(h["offset"]),
            score=float(h["score"]),
            value=h.get("value", "").encode() if include_value and h.get("value") else None,
        )
        for h in data.get("hits", [])
    ]
