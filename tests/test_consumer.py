"""Tests for Consumer and ConsumerRecord."""

from datetime import datetime

import pytest

from streamline_sdk.client import ClientConfig, ConsumerConfig
from streamline_sdk.consumer import Consumer, ConsumerRecord
from streamline_sdk.exceptions import ConsumerError


class TestConsumerRecord:
    """Tests for ConsumerRecord dataclass."""

    def test_record_with_all_fields(self):
        """Test creating a ConsumerRecord with all fields."""
        ts = datetime(2024, 3, 15, 10, 30, 0)
        headers = {"trace-id": b"abc", "content-type": b"text/plain"}
        record = ConsumerRecord(
            topic="events",
            partition=1,
            offset=99,
            key=b"user-42",
            value=b'{"event": "login"}',
            timestamp=ts,
            headers=headers,
        )
        assert record.topic == "events"
        assert record.partition == 1
        assert record.offset == 99
        assert record.key == b"user-42"
        assert record.value == b'{"event": "login"}'
        assert record.timestamp == ts
        assert record.headers == headers

    def test_record_with_none_key_and_value(self):
        """Test record where key and value are None."""
        record = ConsumerRecord(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            timestamp=datetime.now(),
            headers={},
        )
        assert record.key is None
        assert record.value is None

    def test_record_with_empty_headers(self):
        """Test record with empty headers dict."""
        record = ConsumerRecord(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value=b"v",
            timestamp=datetime.now(),
            headers={},
        )
        assert record.headers == {}
        assert len(record.headers) == 0

    def test_record_equality(self):
        """Identical ConsumerRecords should be equal."""
        ts = datetime(2024, 1, 1)
        kwargs = dict(
            topic="t",
            partition=0,
            offset=5,
            key=b"k",
            value=b"v",
            timestamp=ts,
            headers={},
        )
        assert ConsumerRecord(**kwargs) == ConsumerRecord(**kwargs)

    def test_record_inequality(self):
        """ConsumerRecords with different offsets should differ."""
        ts = datetime(2024, 1, 1)
        r1 = ConsumerRecord(
            topic="t",
            partition=0,
            offset=1,
            key=None,
            value=b"v",
            timestamp=ts,
            headers={},
        )
        r2 = ConsumerRecord(
            topic="t",
            partition=0,
            offset=2,
            key=None,
            value=b"v",
            timestamp=ts,
            headers={},
        )
        assert r1 != r2

    def test_record_large_offset(self):
        """Test record with a large offset value."""
        record = ConsumerRecord(
            topic="t", partition=0, offset=2**40, key=None, value=b"v",
            timestamp=datetime.now(), headers={},
        )
        assert record.offset == 2**40


class TestConsumerConfig:
    """Tests for ConsumerConfig defaults and custom values."""

    def test_default_config(self):
        """Test all default values."""
        config = ConsumerConfig()
        assert config.group_id is None
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True
        assert config.auto_commit_interval_ms == 5000
        assert config.max_poll_records == 500
        assert config.session_timeout_ms == 30000
        assert config.heartbeat_interval_ms == 3000
        assert config.isolation_level == "read_uncommitted"

    def test_custom_config(self):
        """Test custom consumer config values."""
        config = ConsumerConfig(
            group_id="my-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            auto_commit_interval_ms=1000,
            max_poll_records=100,
            session_timeout_ms=10000,
            heartbeat_interval_ms=1000,
            isolation_level="read_committed",
        )
        assert config.group_id == "my-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
        assert config.auto_commit_interval_ms == 1000
        assert config.max_poll_records == 100
        assert config.session_timeout_ms == 10000
        assert config.heartbeat_interval_ms == 1000
        assert config.isolation_level == "read_committed"


class TestConsumerInit:
    """Tests for Consumer initialization and properties."""

    def test_consumer_init(self):
        """Test consumer can be created with configs."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        assert consumer.is_started is False
        assert consumer.group_id is None
        assert consumer._consumer is None

    def test_consumer_init_with_group(self):
        """Test consumer created with a group_id."""
        config = ConsumerConfig(group_id="test-group")
        consumer = Consumer(ClientConfig(), config)
        assert consumer.group_id == "test-group"

    def test_consumer_not_started_by_default(self):
        """Consumer should not be started right after init."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        assert consumer.is_started is False

    def test_subscription_empty_initially(self):
        """Subscription set should be empty on new consumer."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        assert consumer.subscription() == set()

    def test_assignment_empty_when_not_started(self):
        """Assignment should return empty set when consumer is None."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        assert consumer.assignment() == set()


class TestConsumerErrors:
    """Tests for Consumer error handling without a broker."""

    @pytest.mark.asyncio
    async def test_subscribe_before_start_raises(self):
        """Subscribing before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.subscribe(["test-topic"])

    @pytest.mark.asyncio
    async def test_unsubscribe_before_start_is_safe(self):
        """Unsubscribing when consumer is None should be safe (no-op)."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        # _consumer is None, so the if-check passes without error
        await consumer.unsubscribe()

    @pytest.mark.asyncio
    async def test_assign_before_start_raises(self):
        """Assigning partitions before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            consumer.assign([])

    @pytest.mark.asyncio
    async def test_seek_before_start_raises(self):
        """Seeking before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.seek(None, 0)

    @pytest.mark.asyncio
    async def test_seek_to_beginning_before_start_raises(self):
        """seek_to_beginning before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.seek_to_beginning()

    @pytest.mark.asyncio
    async def test_seek_to_end_before_start_raises(self):
        """seek_to_end before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.seek_to_end()

    @pytest.mark.asyncio
    async def test_commit_before_start_raises(self):
        """Committing before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.commit()

    @pytest.mark.asyncio
    async def test_position_before_start_raises(self):
        """Getting position before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.position(None)

    @pytest.mark.asyncio
    async def test_committed_before_start_raises(self):
        """Getting committed offset before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.committed(None)

    @pytest.mark.asyncio
    async def test_poll_before_start_raises(self):
        """Polling before start should raise ConsumerError."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        with pytest.raises(ConsumerError, match="Consumer not started"):
            await consumer.poll()

    @pytest.mark.asyncio
    async def test_close_before_start_is_safe(self):
        """Closing a consumer that was never started should be safe."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        await consumer.close()
        assert consumer.is_started is False

    @pytest.mark.asyncio
    async def test_close_clears_subscriptions(self):
        """Close should clear subscribed topics."""
        consumer = Consumer(ClientConfig(), ConsumerConfig())
        consumer._subscribed_topics = {"topic-a", "topic-b"}
        await consumer.close()
        assert consumer.subscription() == set()
