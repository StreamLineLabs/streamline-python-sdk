"""Admin client for Streamline administrative operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from .exceptions import TopicError


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
        """List all topics.

        Returns:
            List of topic names.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            metadata = await self._admin.describe_cluster()
            # Note: aiokafka's describe_cluster doesn't return topics directly
            # We need to use the underlying client
            topics = list(self._admin._client.cluster.topics())
            return [t for t in topics if not t.startswith("__")]
        except KafkaError as e:
            raise TopicError(f"Failed to list topics: {e}") from e

    async def describe_topic(self, name: str) -> TopicInfo:
        """Describe a topic.

        Args:
            name: Topic name.

        Returns:
            TopicInfo object.
        """
        if self._admin is None:
            raise TopicError("Admin client not started")

        try:
            # Force metadata refresh
            await self._admin._client.force_metadata_update()
            partitions = self._admin._client.cluster.partitions_for_topic(name)

            if partitions is None:
                raise TopicError(f"Topic not found: {name}")

            # Get replication factor from first partition
            replication_factor = 1
            if partitions:
                partition_meta = self._admin._client.cluster.partition_for_topic(
                    name, list(partitions)[0]
                )
                if partition_meta:
                    replication_factor = len(partition_meta.replicas)

            return TopicInfo(
                name=name,
                partitions=len(partitions),
                replication_factor=replication_factor,
                internal=name.startswith("__"),
            )
        except KafkaError as e:
            raise TopicError(f"Failed to describe topic '{name}': {e}") from e

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

    async def __aenter__(self) -> "Admin":
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Exit async context manager."""
        await self.close()
