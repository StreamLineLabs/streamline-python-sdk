"""Admin client for Streamline administrative operations."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from .exceptions import TopicError

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


@dataclass
class TopicConfig:
    """Configuration for creating a topic.

    Attributes:
        name: Topic name.
        num_partitions: Number of partitions.
        replication_factor: Number of replicas.
        config: Topic configuration (e.g., retention.ms).
    """

    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = field(default_factory=dict)


@dataclass
class TopicInfo:
    """Information about a topic.

    Attributes:
        name: Topic name.
        partitions: Number of partitions.
        replication_factor: Replication factor.
        internal: Whether this is an internal topic.
    """

    name: str
    partitions: int
    replication_factor: int
    internal: bool = False


@dataclass
class PartitionInfo:
    """Information about a partition.

    Attributes:
        id: Partition ID.
        leader: Leader broker ID.
        replicas: List of replica broker IDs.
        isr: List of in-sync replica broker IDs.
    """

    id: int
    leader: int
    replicas: List[int]
    isr: List[int]


@dataclass
class ConsumerGroupInfo:
    """Information about a consumer group.

    Attributes:
        group_id: Consumer group ID.
        state: Group state (e.g., Stable, Empty).
        protocol: Group protocol.
        members: List of group members.
    """

    group_id: str
    state: str
    protocol: str
    members: List["GroupMember"]


@dataclass
class GroupMember:
    """Information about a consumer group member.

    Attributes:
        member_id: Member ID.
        client_id: Client ID.
        host: Member host.
    """

    member_id: str
    client_id: str
    host: str


@dataclass
class ClusterInfo:
    """Cluster overview information.

    Attributes:
        cluster_id: Cluster identifier.
        broker_id: ID of the broker responding.
        brokers: List of brokers in the cluster.
        controller: ID of the controller broker.
    """

    cluster_id: str = ""
    broker_id: int = 0
    brokers: List["BrokerInfo"] = field(default_factory=list)
    controller: int = -1


@dataclass
class BrokerInfo:
    """Information about a single broker.

    Attributes:
        id: Broker ID.
        host: Broker hostname.
        port: Broker port.
        rack: Optional rack identifier.
    """

    id: int = 0
    host: str = ""
    port: int = 9092
    rack: Optional[str] = None


@dataclass
class ConsumerLag:
    """Consumer lag for a single partition.

    Attributes:
        topic: Topic name.
        partition: Partition index.
        current_offset: Current consumer offset.
        end_offset: End (log-end) offset.
        lag: Offset lag (end_offset - current_offset).
    """

    topic: str = ""
    partition: int = 0
    current_offset: int = 0
    end_offset: int = 0
    lag: int = 0


@dataclass
class ConsumerGroupLag:
    """Aggregated consumer group lag.

    Attributes:
        group_id: Consumer group ID.
        partitions: Per-partition lag details.
        total_lag: Total lag across all partitions.
    """

    group_id: str = ""
    partitions: List[ConsumerLag] = field(default_factory=list)
    total_lag: int = 0


@dataclass
class InspectedMessage:
    """A message returned by the inspection API.

    Attributes:
        offset: Message offset.
        key: Message key (optional).
        value: Message value.
        timestamp: Message timestamp.
        partition: Partition index.
        headers: Message headers.
    """

    offset: int = 0
    key: Optional[str] = None
    value: str = ""
    timestamp: int = 0
    partition: int = 0
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricPoint:
    """A single metric data point.

    Attributes:
        name: Metric name.
        value: Metric value.
        labels: Metric labels.
        timestamp: Metric timestamp.
    """

    name: str = ""
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: int = 0


class Admin:
    """Administrative operations for Streamline.

    Example:
        async with client.admin as admin:
            await admin.create_topic(TopicConfig(name="my-topic", partitions=3))
    """

    def __init__(self, client_config: Any):
        """Initialize the admin client.

        Args:
            client_config: Client configuration.
        """
        self._client_config = client_config
        self._admin: Optional[AIOKafkaAdminClient] = None
        self._started = False

    async def start(self) -> None:
        """Start the admin client."""
        if self._started:
            return

        security_kwargs = {}
        if self._client_config.security_protocol != "PLAINTEXT":
            security_kwargs["security_protocol"] = self._client_config.security_protocol

        if self._client_config.sasl_mechanism:
            security_kwargs["sasl_mechanism"] = self._client_config.sasl_mechanism
            security_kwargs["sasl_plain_username"] = self._client_config.sasl_username
            security_kwargs["sasl_plain_password"] = self._client_config.sasl_password

        self._admin = AIOKafkaAdminClient(
            bootstrap_servers=self._client_config.bootstrap_servers,
            client_id=self._client_config.client_id,
            **security_kwargs,
        )

        try:
            await self._admin.start()
            self._started = True
        except KafkaError as e:
            raise TopicError(f"Failed to start admin client: {e}") from e

    async def close(self) -> None:
        """Close the admin client."""
        if self._admin is not None:
            await self._admin.close()
            self._admin = None
        self._started = False

    async def create_topic(self, config: TopicConfig) -> None:
        """Create a topic.

        Args:
            config: Topic configuration.

        Raises:
            TopicError: If topic creation fails.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        new_topic = NewTopic(
            name=config.name,
            num_partitions=config.num_partitions,
            replication_factor=config.replication_factor,
            topic_configs=config.config if config.config else None,
        )

        try:
            await self._admin.create_topics([new_topic])
        except KafkaError as e:
            raise TopicError(f"Failed to create topic '{config.name}': {e}") from e

    async def create_topics(self, configs: List[TopicConfig]) -> None:
        """Create multiple topics.

        Args:
            configs: List of topic configurations.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        new_topics = [
            NewTopic(
                name=c.name,
                num_partitions=c.num_partitions,
                replication_factor=c.replication_factor,
                topic_configs=c.config if c.config else None,
            )
            for c in configs
        ]

        try:
            await self._admin.create_topics(new_topics)
        except KafkaError as e:
            raise TopicError(f"Failed to create topics: {e}") from e

    async def delete_topic(self, name: str) -> None:
        """Delete a topic.

        Args:
            name: Topic name.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            await self._admin.delete_topics([name])
        except KafkaError as e:
            raise TopicError(f"Failed to delete topic '{name}': {e}") from e

    async def delete_topics(self, names: List[str]) -> None:
        """Delete multiple topics.

        Args:
            names: List of topic names.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            await self._admin.delete_topics(names)
        except KafkaError as e:
            raise TopicError(f"Failed to delete topics: {e}") from e

    async def list_topics(self) -> List[str]:
        """List all topics via the HTTP REST API.

        Returns:
            List of topic names.
        """
        if not self._started:
            raise TopicError("Admin client not started")

        try:
            data = await self._http_get("/v1/topics")
            return [t["name"] for t in data if not t.get("name", "").startswith("__")]
        except TopicError:
            raise
        except Exception as e:
            raise TopicError(f"Failed to list topics: {e}") from e

    async def describe_topic(self, name: str) -> TopicInfo:
        """Describe a topic via the HTTP REST API.

        Args:
            name: Topic name.

        Returns:
            TopicInfo object.
        """
        if not self._started:
            raise TopicError("Admin client not started")

        try:
            data = await self._http_get(f"/v1/topics/{name}")
            return TopicInfo(
                name=data.get("name", name),
                partitions=data.get("partitions", 0),
                replication_factor=data.get("replication_factor", 1),
                internal=name.startswith("__"),
            )
        except TopicError:
            raise
        except Exception as e:
            raise TopicError(f"Failed to describe topic '{name}': {e}") from e

    async def _http_get(self, path: str) -> Any:
        """Make an HTTP GET request to the Streamline REST API."""
        http_url = getattr(self._client_config, "http_url", "http://localhost:9094")
        url = f"{http_url}{path}"

        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 404:
                        raise TopicError(f"Not found: {path}")
                    if resp.status != 200:
                        text = await resp.text()
                        raise TopicError(f"HTTP {resp.status}: {text}")
                    return await resp.json()
        else:
            import urllib.request
            req = urllib.request.Request(url)
            def _sync_get():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read())
            return await asyncio.to_thread(_sync_get)

    async def list_consumer_groups(self) -> List[str]:
        """List all consumer groups.

        Returns:
            List of consumer group IDs.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            groups = await self._admin.list_consumer_groups()
            return [g[0] for g in groups]
        except KafkaError as e:
            raise TopicError(f"Failed to list consumer groups: {e}") from e

    async def describe_consumer_group(self, group_id: str) -> ConsumerGroupInfo:
        """Describe a consumer group.

        Args:
            group_id: Consumer group ID.

        Returns:
            ConsumerGroupInfo object.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            groups = await self._admin.describe_consumer_groups([group_id])
            if not groups:
                raise TopicError(f"Consumer group not found: {group_id}")

            group = groups[0]
            members = [
                GroupMember(
                    member_id=m.member_id,
                    client_id=m.client_id,
                    host=m.client_host,
                )
                for m in group.members
            ]

            return ConsumerGroupInfo(
                group_id=group.group,
                state=group.state,
                protocol=group.protocol,
                members=members,
            )
        except KafkaError as e:
            raise TopicError(f"Failed to describe consumer group: {e}") from e

    @property
    def is_started(self) -> bool:
        """Check if admin client is started."""
        return self._started

    async def cluster_info(self) -> ClusterInfo:
        """Get cluster overview including broker list.

        Returns:
            ClusterInfo with broker details.
        """
        data = await self._http_get("/v1/cluster")
        brokers = [
            BrokerInfo(
                id=b.get("id", 0),
                host=b.get("host", ""),
                port=b.get("port", 9092),
                rack=b.get("rack"),
            )
            for b in data.get("brokers", [])
        ]
        return ClusterInfo(
            cluster_id=data.get("cluster_id", ""),
            broker_id=data.get("broker_id", 0),
            brokers=brokers,
            controller=data.get("controller", -1),
        )

    async def consumer_group_lag(self, group_id: str) -> ConsumerGroupLag:
        """Get consumer lag for a specific consumer group.

        Args:
            group_id: Consumer group ID.

        Returns:
            ConsumerGroupLag with per-partition lag.
        """
        data = await self._http_get(f"/v1/consumer-groups/{group_id}/lag")
        partitions = [
            ConsumerLag(
                topic=p.get("topic", ""),
                partition=p.get("partition", 0),
                current_offset=p.get("current_offset", 0),
                end_offset=p.get("end_offset", 0),
                lag=p.get("lag", 0),
            )
            for p in data.get("partitions", [])
        ]
        return ConsumerGroupLag(
            group_id=data.get("group_id", group_id),
            partitions=partitions,
            total_lag=data.get("total_lag", sum(p.lag for p in partitions)),
        )

    async def consumer_group_topic_lag(
        self, group_id: str, topic: str
    ) -> ConsumerGroupLag:
        """Get consumer lag for a specific topic within a group.

        Args:
            group_id: Consumer group ID.
            topic: Topic name.

        Returns:
            ConsumerGroupLag scoped to the given topic.
        """
        data = await self._http_get(f"/v1/consumer-groups/{group_id}/lag/{topic}")
        partitions = [
            ConsumerLag(
                topic=p.get("topic", topic),
                partition=p.get("partition", 0),
                current_offset=p.get("current_offset", 0),
                end_offset=p.get("end_offset", 0),
                lag=p.get("lag", 0),
            )
            for p in data.get("partitions", [])
        ]
        return ConsumerGroupLag(
            group_id=data.get("group_id", group_id),
            partitions=partitions,
            total_lag=data.get("total_lag", sum(p.lag for p in partitions)),
        )

    async def inspect_messages(
        self,
        topic: str,
        partition: int = 0,
        offset: Optional[int] = None,
        limit: int = 20,
    ) -> List[InspectedMessage]:
        """Browse messages from a topic partition.

        Args:
            topic: Topic name.
            partition: Partition index.
            offset: Start offset (optional).
            limit: Maximum messages to return.

        Returns:
            List of inspected messages.
        """
        path = f"/v1/inspect/{topic}?partition={partition}&limit={limit}"
        if offset is not None:
            path += f"&offset={offset}"
        data = await self._http_get(path)
        return [
            InspectedMessage(
                offset=m.get("offset", 0),
                key=m.get("key"),
                value=m.get("value", ""),
                timestamp=m.get("timestamp", 0),
                partition=m.get("partition", partition),
                headers=m.get("headers", {}),
            )
            for m in data
        ]

    async def latest_messages(
        self, topic: str, count: int = 10
    ) -> List[InspectedMessage]:
        """Get the latest messages from a topic.

        Args:
            topic: Topic name.
            count: Number of messages to return.

        Returns:
            List of latest messages.
        """
        data = await self._http_get(f"/v1/inspect/{topic}/latest?count={count}")
        return [
            InspectedMessage(
                offset=m.get("offset", 0),
                key=m.get("key"),
                value=m.get("value", ""),
                timestamp=m.get("timestamp", 0),
                partition=m.get("partition", 0),
                headers=m.get("headers", {}),
            )
            for m in data
        ]

    async def metrics_history(self) -> List[MetricPoint]:
        """Get metrics history from the server.

        Returns:
            List of metric data points.
        """
        data = await self._http_get("/v1/metrics/history")
        return [
            MetricPoint(
                name=m.get("name", ""),
                value=float(m.get("value", 0)),
                labels=m.get("labels", {}),
                timestamp=m.get("timestamp", 0),
            )
            for m in data
        ]

    async def __aenter__(self) -> "Admin":
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
# correct offset reset behavior on new consumer group
# resolve event loop conflict in nested async calls

# add async context manager for producer lifecycle
# extract connection pool into dedicated module
