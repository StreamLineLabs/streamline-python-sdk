"""Tests for Admin, TopicConfig, TopicInfo, PartitionInfo, and related classes."""

import pytest

from streamline_sdk.admin import (
    Admin,
    TopicConfig,
    TopicInfo,
    PartitionInfo,
    ConsumerGroupInfo,
    GroupMember,
)
from streamline_sdk.client import ClientConfig
from streamline_sdk.exceptions import TopicError


class TestTopicConfig:
    """Tests for admin.TopicConfig dataclass."""

    def test_minimal_config(self):
        """Test TopicConfig with only name."""
        config = TopicConfig(name="events")
        assert config.name == "events"
        assert config.num_partitions == 1
        assert config.replication_factor == 1
        assert config.config == {}

    def test_full_config(self):
        """Test TopicConfig with all fields."""
        config = TopicConfig(
            name="orders",
            num_partitions=12,
            replication_factor=3,
            config={"retention.ms": "604800000", "cleanup.policy": "compact"},
        )
        assert config.name == "orders"
        assert config.num_partitions == 12
        assert config.replication_factor == 3
        assert config.config["retention.ms"] == "604800000"
        assert config.config["cleanup.policy"] == "compact"

    def test_config_dict_is_independent(self):
        """Default config dict should be independent per instance."""
        c1 = TopicConfig(name="t1")
        c2 = TopicConfig(name="t2")
        c1.config["key"] = "val"
        assert "key" not in c2.config

    def test_config_equality(self):
        """Identical TopicConfig instances should be equal."""
        c1 = TopicConfig(name="t", num_partitions=3, replication_factor=2)
        c2 = TopicConfig(name="t", num_partitions=3, replication_factor=2)
        assert c1 == c2


class TestTopicInfo:
    """Tests for admin.TopicInfo dataclass."""

    def test_topic_info_creation(self):
        """Test creating TopicInfo with all fields."""
        info = TopicInfo(
            name="events",
            partitions=6,
            replication_factor=3,
            internal=False,
        )
        assert info.name == "events"
        assert info.partitions == 6
        assert info.replication_factor == 3
        assert info.internal is False

    def test_topic_info_default_internal(self):
        """Internal should default to False."""
        info = TopicInfo(name="t", partitions=1, replication_factor=1)
        assert info.internal is False

    def test_topic_info_internal_topic(self):
        """Test representing an internal topic."""
        info = TopicInfo(name="__consumer_offsets", partitions=50, replication_factor=3, internal=True)
        assert info.internal is True
        assert info.name == "__consumer_offsets"

    def test_topic_info_equality(self):
        """Identical TopicInfo should be equal."""
        i1 = TopicInfo("t", 3, 1, False)
        i2 = TopicInfo("t", 3, 1, False)
        assert i1 == i2


class TestPartitionInfo:
    """Tests for admin.PartitionInfo dataclass."""

    def test_partition_info_creation(self):
        """Test creating PartitionInfo with all fields."""
        info = PartitionInfo(
            id=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2, 3],
        )
        assert info.id == 0
        assert info.leader == 1
        assert info.replicas == [1, 2, 3]
        assert info.isr == [1, 2, 3]

    def test_partition_info_partial_isr(self):
        """Test partition where ISR is a subset of replicas."""
        info = PartitionInfo(id=2, leader=1, replicas=[1, 2, 3], isr=[1, 3])
        assert len(info.isr) < len(info.replicas)
        assert set(info.isr).issubset(set(info.replicas))

    def test_partition_info_single_replica(self):
        """Test partition with single broker (no replication)."""
        info = PartitionInfo(id=0, leader=0, replicas=[0], isr=[0])
        assert info.replicas == [0]
        assert info.isr == [0]

    def test_partition_info_equality(self):
        """Identical PartitionInfo should be equal."""
        p1 = PartitionInfo(0, 1, [1, 2], [1, 2])
        p2 = PartitionInfo(0, 1, [1, 2], [1, 2])
        assert p1 == p2


class TestConsumerGroupInfo:
    """Tests for admin.ConsumerGroupInfo dataclass."""

    def test_consumer_group_info(self):
        """Test creating ConsumerGroupInfo with members."""
        members = [
            GroupMember(member_id="m-1", client_id="c-1", host="/10.0.0.1"),
            GroupMember(member_id="m-2", client_id="c-2", host="/10.0.0.2"),
        ]
        info = ConsumerGroupInfo(
            group_id="my-group",
            state="Stable",
            protocol="range",
            members=members,
        )
        assert info.group_id == "my-group"
        assert info.state == "Stable"
        assert info.protocol == "range"
        assert len(info.members) == 2
        assert info.members[0].member_id == "m-1"

    def test_consumer_group_info_empty_members(self):
        """Test group with no members."""
        info = ConsumerGroupInfo(
            group_id="empty-group",
            state="Empty",
            protocol="",
            members=[],
        )
        assert info.members == []
        assert info.state == "Empty"


class TestGroupMember:
    """Tests for admin.GroupMember dataclass."""

    def test_group_member_creation(self):
        """Test creating GroupMember."""
        member = GroupMember(
            member_id="consumer-1-uuid",
            client_id="my-consumer",
            host="/192.168.1.100",
        )
        assert member.member_id == "consumer-1-uuid"
        assert member.client_id == "my-consumer"
        assert member.host == "/192.168.1.100"

    def test_group_member_equality(self):
        """Identical members should be equal."""
        m1 = GroupMember("m1", "c1", "/host")
        m2 = GroupMember("m1", "c1", "/host")
        assert m1 == m2


class TestAdminInit:
    """Tests for Admin initialization."""

    def test_admin_init(self):
        """Test Admin can be created with a config."""
        admin = Admin(ClientConfig())
        assert admin.is_started is False
        assert admin._admin is None

    def test_admin_init_custom_config(self):
        """Test Admin with custom client config."""
        config = ClientConfig(
            bootstrap_servers="broker:9092",
            client_id="admin-client",
        )
        admin = Admin(config)
        assert admin._client_config.bootstrap_servers == "broker:9092"
        assert admin._client_config.client_id == "admin-client"
        assert admin.is_started is False


class TestAdminErrors:
    """Tests for Admin operations when not started."""

    @pytest.mark.asyncio
    async def test_create_topic_before_start_raises(self):
        """Creating a topic before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.create_topic(TopicConfig(name="t"))

    @pytest.mark.asyncio
    async def test_create_topics_before_start_raises(self):
        """Creating multiple topics before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.create_topics([TopicConfig(name="t")])

    @pytest.mark.asyncio
    async def test_delete_topic_before_start_raises(self):
        """Deleting a topic before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.delete_topic("t")

    @pytest.mark.asyncio
    async def test_delete_topics_before_start_raises(self):
        """Deleting multiple topics before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.delete_topics(["t1", "t2"])

    @pytest.mark.asyncio
    async def test_list_topics_before_start_raises(self):
        """Listing topics before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.list_topics()

    @pytest.mark.asyncio
    async def test_describe_topic_before_start_raises(self):
        """Describing a topic before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.describe_topic("t")

    @pytest.mark.asyncio
    async def test_list_consumer_groups_before_start_raises(self):
        """Listing consumer groups before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.list_consumer_groups()

    @pytest.mark.asyncio
    async def test_describe_consumer_group_before_start_raises(self):
        """Describing a consumer group before start should raise TopicError."""
        admin = Admin(ClientConfig())
        with pytest.raises(TopicError, match="Admin client not started"):
            await admin.describe_consumer_group("g")

    @pytest.mark.asyncio
    async def test_close_before_start_is_safe(self):
        """Closing admin that was never started should be safe."""
        admin = Admin(ClientConfig())
        await admin.close()
        assert admin.is_started is False

