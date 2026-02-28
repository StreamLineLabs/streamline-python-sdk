"""
Type definitions for Streamline Python SDK.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional,Iterator
from datetime import datetime


@dataclass
class Message:
    """A message from a Streamline topic."""

    topic: str
    """Topic name."""

    partition: int
    """Partition number."""

    offset: int
    """Offset within the partition."""

    timestamp: int
    """Timestamp in milliseconds since epoch."""

    key: Optional[str]
    """Message key (optional)."""

    value: Any
    """Message value (deserialized from JSON if possible)."""

    headers: Dict[str, str] = field(default_factory=dict)
    """Message headers."""

    @property
    def datetime(self) -> datetime:
        """Get timestamp as datetime object."""
        return datetime.fromtimestamp(self.timestamp / 1000)

    def __repr__(self) -> str:
        return (
            f"Message(topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, key={self.key!r})"
        )


@dataclass
class Record:
    """A record to be produced to a topic."""

    value: Any
    """Message value (will be JSON serialized if dict/list)."""

    key: Optional[str] = None
    """Message key (optional)."""

    headers: Dict[str, str] = field(default_factory=dict)
    """Message headers."""

    partition: Optional[int] = None
    """Target partition (optional, uses key hash if not specified)."""

    timestamp: Optional[int] = None
    """Timestamp in milliseconds (optional, uses current time if not specified)."""


@dataclass
class TopicConfig:
    """Topic configuration."""

    partitions: int = 1
    """Number of partitions."""

    replication_factor: int = 1
    """Replication factor."""

    retention_ms: Optional[int] = None
    """Retention time in milliseconds."""

    retention_bytes: Optional[int] = None
    """Retention size in bytes."""

    segment_bytes: Optional[int] = None
    """Segment size in bytes."""

    cleanup_policy: str = "delete"
    """Cleanup policy: 'delete' or 'compact'."""

    compression_type: str = "none"
    """Compression type: 'none', 'gzip', 'snappy', 'lz4', 'zstd'."""


@dataclass
class PartitionInfo:
    """Partition information."""

    id: int
    """Partition ID."""

    leader: int
    """Leader broker ID."""

    replicas: List[int]
    """Replica broker IDs."""

    isr: List[int]
    """In-sync replica broker IDs."""

    high_watermark: int
    """High watermark (latest offset)."""

    log_start_offset: int = 0
    """Log start offset."""


@dataclass
class TopicInfo:
    """Topic information."""

    name: str
    """Topic name."""

    partitions: List[PartitionInfo]
    """Partition information."""

    config: TopicConfig
    """Topic configuration."""

    @property
    def partition_count(self) -> int:
        """Number of partitions."""
        return len(self.partitions)


@dataclass
class ConsumerGroupInfo:
    """Consumer group information."""

    group_id: str
    """Group ID."""

    state: str
    """Group state."""

    protocol_type: str
    """Protocol type."""

    protocol: str
    """Protocol name (assignment strategy)."""

    members: List["GroupMemberInfo"]
    """Group members."""


@dataclass
class GroupMemberInfo:
    """Consumer group member information."""

    member_id: str
    """Member ID."""

    client_id: str
    """Client ID."""

    client_host: str
    """Client host."""

    assignments: List[Dict[str, Any]]
    """Partition assignments."""


@dataclass
class OffsetInfo:
    """Offset information."""

    topic: str
    """Topic name."""

    partition: int
    """Partition number."""

    current_offset: int
    """Current committed offset."""

    log_end_offset: int
    """Log end offset."""

    @property
    def lag(self) -> int:
        """Consumer lag."""
        return self.log_end_offset - self.current_offset


@dataclass
class ProduceResult:
    """Result of a produce operation."""

    topic: str
    """Topic name."""

    partition: int
    """Partition written to."""

    offset: int
    """Offset of produced message."""

    timestamp: int
    """Timestamp in milliseconds."""


@dataclass
class QueryResult:
    """Result of a SQL query."""

    columns: List[str]
    """Column names."""

    rows: List[Dict[str, Any]]
    """Result rows."""

    row_count: int
    """Number of rows."""

    execution_time_ms: int
    """Execution time in milliseconds."""

    def __iter__(self) -> "Iterator[Dict[str, Any]]":
        return iter(self.rows)

    def __len__(self) -> int:
        return self.row_count
