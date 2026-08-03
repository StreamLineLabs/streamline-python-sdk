"""Tests for type definitions in the Streamline SDK."""

from datetime import datetime

from streamline_sdk.types import (
    Message,
    OffsetInfo,
    PartitionInfo,
    ProduceResult,
    QueryResult,
    Record,
    TopicConfig,
    TopicInfo,
)


class TestMessage:
    """Tests for the Message dataclass."""

    def test_creation_with_all_fields(self):
        """Test creating Message with all fields."""
        msg = Message(
            topic="events",
            partition=2,
            offset=100,
            timestamp=1700000000000,
            key="user-42",
            value={"event": "click"},
            headers={"trace": "abc"},
        )
        assert msg.topic == "events"
        assert msg.partition == 2
        assert msg.offset == 100
        assert msg.timestamp == 1700000000000
        assert msg.key == "user-42"
        assert msg.value == {"event": "click"}
        assert msg.headers == {"trace": "abc"}

    def test_default_headers(self):
        """Headers should default to empty dict."""
        msg = Message(
            topic="t", partition=0, offset=0, timestamp=0, key=None, value="v"
        )
        assert msg.headers == {}

    def test_default_headers_independent(self):
        """Default headers dicts should be independent per instance."""
        m1 = Message(topic="t", partition=0, offset=0, timestamp=0, key=None, value="a")
        m2 = Message(topic="t", partition=0, offset=0, timestamp=0, key=None, value="b")
        m1.headers["x"] = "y"
        assert "x" not in m2.headers

    def test_none_key(self):
        """Key can be None."""
        msg = Message(
            topic="t", partition=0, offset=0, timestamp=0, key=None, value="v"
        )
        assert msg.key is None

    def test_datetime_property(self):
        """Test the datetime property converts timestamp correctly."""
        ts_ms = 1700000000000  # 2023-11-14 22:13:20 UTC (approx)
        msg = Message(
            topic="t", partition=0, offset=0, timestamp=ts_ms, key=None, value="v"
        )
        dt = msg.datetime
        assert isinstance(dt, datetime)
        assert dt == datetime.fromtimestamp(ts_ms / 1000)

    def test_datetime_property_zero(self):
        """Timestamp of 0 should give epoch."""
        msg = Message(
            topic="t", partition=0, offset=0, timestamp=0, key=None, value="v"
        )
        assert msg.datetime == datetime.fromtimestamp(0)

    def test_repr(self):
        """Test custom __repr__."""
        msg = Message(
            topic="events", partition=1, offset=42, timestamp=0, key="k1", value="v"
        )
        r = repr(msg)
        assert "Message(" in r
        assert "topic='events'" in r
        assert "partition=1" in r
        assert "offset=42" in r
        assert "key='k1'" in r
        # value and headers should NOT be in repr
        assert "value=" not in r
        assert "headers=" not in r

    def test_repr_none_key(self):
        """Repr should show key=None."""
        msg = Message(
            topic="t", partition=0, offset=0, timestamp=0, key=None, value="v"
        )
        assert "key=None" in repr(msg)

    def test_value_types(self):
        """Value can be any type."""
        for val in ["string", 42, [1, 2, 3], {"a": 1}, None, b"bytes"]:
            msg = Message(
                topic="t", partition=0, offset=0, timestamp=0, key=None, value=val
            )
            assert msg.value == val


class TestRecord:
    """Tests for the Record dataclass."""

    def test_minimal_record(self):
        """Test Record with only value."""
        rec = Record(value="hello")
        assert rec.value == "hello"
        assert rec.key is None
        assert rec.headers == {}
        assert rec.partition is None
        assert rec.timestamp is None

    def test_full_record(self):
        """Test Record with all fields."""
        rec = Record(
            value={"data": 1},
            key="k",
            headers={"h": "v"},
            partition=3,
            timestamp=1700000000000,
        )
        assert rec.value == {"data": 1}
        assert rec.key == "k"
        assert rec.headers == {"h": "v"}
        assert rec.partition == 3
        assert rec.timestamp == 1700000000000

    def test_default_headers_independent(self):
        """Default headers should be independent per instance."""
        r1 = Record(value="a")
        r2 = Record(value="b")
        r1.headers["x"] = "y"
        assert "x" not in r2.headers


class TestTypesTopicConfig:
    """Tests for types.TopicConfig (different from admin.TopicConfig)."""

    def test_defaults(self):
        """Test default values."""
        config = TopicConfig()
        assert config.partitions == 1
        assert config.replication_factor == 1
        assert config.retention_ms is None
        assert config.retention_bytes is None
        assert config.segment_bytes is None
        assert config.cleanup_policy == "delete"
        assert config.compression_type == "none"

    def test_custom_config(self):
        """Test custom configuration."""
        config = TopicConfig(
            partitions=6,
            replication_factor=3,
            retention_ms=86400000,
            retention_bytes=1073741824,
            segment_bytes=536870912,
            cleanup_policy="compact",
            compression_type="zstd",
        )
        assert config.partitions == 6
        assert config.replication_factor == 3
        assert config.retention_ms == 86400000
        assert config.retention_bytes == 1073741824
        assert config.segment_bytes == 536870912
        assert config.cleanup_policy == "compact"
        assert config.compression_type == "zstd"


class TestTypesPartitionInfo:
    """Tests for types.PartitionInfo (has high_watermark and log_start_offset)."""

    def test_creation(self):
        """Test PartitionInfo creation."""
        info = PartitionInfo(
            id=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2, 3],
            high_watermark=1000,
        )
        assert info.id == 0
        assert info.leader == 1
        assert info.replicas == [1, 2, 3]
        assert info.isr == [1, 2, 3]
        assert info.high_watermark == 1000
        assert info.log_start_offset == 0  # default

    def test_custom_log_start_offset(self):
        """Test PartitionInfo with custom log_start_offset."""
        info = PartitionInfo(
            id=0, leader=1, replicas=[1], isr=[1],
            high_watermark=500, log_start_offset=100,
        )
        assert info.log_start_offset == 100


class TestTypesTopicInfo:
    """Tests for types.TopicInfo (has partitions list and config)."""

    def test_creation(self):
        """Test TopicInfo creation."""
        parts = [
            PartitionInfo(
                id=0, leader=1, replicas=[1, 2], isr=[1, 2], high_watermark=100
            ),
            PartitionInfo(
                id=1, leader=2, replicas=[1, 2], isr=[1, 2], high_watermark=200
            ),
        ]
        config = TopicConfig(partitions=2, replication_factor=2)
        info = TopicInfo(name="events", partitions=parts, config=config)
        assert info.name == "events"
        assert len(info.partitions) == 2

    def test_partition_count_property(self):
        """Test the partition_count property."""
        parts = [
            PartitionInfo(id=i, leader=0, replicas=[0], isr=[0], high_watermark=0)
            for i in range(5)
        ]
        info = TopicInfo(name="t", partitions=parts, config=TopicConfig())
        assert info.partition_count == 5

    def test_empty_partitions(self):
        """Test TopicInfo with no partitions."""
        info = TopicInfo(name="empty", partitions=[], config=TopicConfig())
        assert info.partition_count == 0


class TestOffsetInfo:
    """Tests for OffsetInfo dataclass."""

    def test_creation(self):
        """Test OffsetInfo creation."""
        info = OffsetInfo(
            topic="events",
            partition=0,
            current_offset=50,
            log_end_offset=100,
        )
        assert info.topic == "events"
        assert info.partition == 0
        assert info.current_offset == 50
        assert info.log_end_offset == 100

    def test_lag_property(self):
        """Test consumer lag calculation."""
        info = OffsetInfo(topic="t", partition=0, current_offset=30, log_end_offset=100)
        assert info.lag == 70

    def test_lag_zero(self):
        """Lag should be 0 when caught up."""
        info = OffsetInfo(
            topic="t", partition=0, current_offset=100, log_end_offset=100
        )
        assert info.lag == 0


class TestProduceResult:
    """Tests for ProduceResult dataclass."""

    def test_creation(self):
        """Test ProduceResult creation."""
        result = ProduceResult(
            topic="events", partition=2, offset=42, timestamp=1700000000000
        )
        assert result.topic == "events"
        assert result.partition == 2
        assert result.offset == 42
        assert result.timestamp == 1700000000000


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_creation(self):
        """Test QueryResult creation."""
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = QueryResult(
            columns=["id", "name"],
            rows=rows,
            row_count=2,
            execution_time_ms=15,
        )
        assert result.columns == ["id", "name"]
        assert result.rows == rows
        assert result.row_count == 2
        assert result.execution_time_ms == 15

    def test_len(self):
        """Test __len__ returns row_count."""
        result = QueryResult(columns=[], rows=[], row_count=5, execution_time_ms=0)
        assert len(result) == 5

    def test_iter(self):
        """Test __iter__ iterates over rows."""
        rows = [{"a": 1}, {"a": 2}]
        result = QueryResult(columns=["a"], rows=rows, row_count=2, execution_time_ms=0)
        assert list(result) == rows

    def test_empty_result(self):
        """Test empty query result."""
        result = QueryResult(columns=["id"], rows=[], row_count=0, execution_time_ms=1)
        assert len(result) == 0
        assert list(result) == []
