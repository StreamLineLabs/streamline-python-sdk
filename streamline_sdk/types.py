"""
Type definitions for Streamline Python SDK.
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, NamedTuple


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

    key: str | None
    """Message key (optional)."""

    value: Any
    """Message value (deserialized from JSON if possible)."""

    headers: dict[str, str] = field(default_factory=dict)
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

    key: str | None = None
    """Message key (optional)."""

    headers: dict[str, str] = field(default_factory=dict)
    """Message headers."""

    partition: int | None = None
    """Target partition (optional, uses key hash if not specified)."""

    timestamp: int | None = None
    """Timestamp in milliseconds (optional, uses current time if not specified)."""


@dataclass(init=False)
class TopicConfig:
    """Canonical topic configuration.

    The admin-facing ``name``/``num_partitions``/``config`` shape is canonical.
    The older ``streamline_sdk.types`` retention fields and ``partitions`` alias
    remain available for import and source compatibility.
    """

    name: str
    num_partitions: int
    replication_factor: int
    config: dict[str, str]
    retention_ms: int | None
    retention_bytes: int | None
    segment_bytes: int | None
    cleanup_policy: str
    compression_type: str

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Dispatch to the canonical or legacy positional constructor shape.

        Two incompatible positional constructor shapes have existed for
        this class:

        * Canonical (admin-facing): ``TopicConfig(name, num_partitions,
          replication_factor, config)``, where ``name`` is a string.
        * Legacy (original ``streamline_sdk.types`` dataclass field order):
          ``TopicConfig(partitions, replication_factor, retention_ms,
          retention_bytes, segment_bytes, cleanup_policy,
          compression_type)``, where the first positional argument is the
          partition count (an ``int``), not a name.

        Because ``name`` is always a string and legacy-shape ``partitions``
        is always an int, the type of the first positional argument
        unambiguously identifies which shape is being used, and this
        constructor dispatches accordingly so both call styles keep
        working.
        """
        if args and not isinstance(args[0], str):
            legacy_fields = (
                "num_partitions",
                "replication_factor",
                "retention_ms",
                "retention_bytes",
                "segment_bytes",
                "cleanup_policy",
                "compression_type",
            )
            if len(args) > len(legacy_fields):
                raise TypeError(
                    "TopicConfig() takes at most "
                    f"{len(legacy_fields)} legacy positional arguments "
                    f"(partitions, replication_factor, retention_ms, "
                    f"retention_bytes, segment_bytes, cleanup_policy, "
                    f"compression_type) but {len(args)} were given"
                )
            legacy_kwargs = dict(zip(legacy_fields, args))
            overlap = sorted(set(legacy_kwargs) & set(kwargs))
            if overlap:
                raise TypeError(
                    "TopicConfig() got multiple values for argument(s): "
                    f"{', '.join(overlap)}"
                )
            warnings.warn(
                "TopicConfig(partitions, replication_factor, retention_ms, "
                "...) positional construction uses the deprecated legacy "
                "field order; use keyword arguments or "
                "TopicConfig(name=..., num_partitions=...) instead",
                DeprecationWarning,
                stacklevel=2,
            )
            self._init_canonical(**{**legacy_kwargs, **kwargs})
            return
        self._init_canonical(*args, **kwargs)

    def _init_canonical(
        self,
        name: str = "",
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
        *,
        partitions: int | None = None,
        retention_ms: int | None = None,
        retention_bytes: int | None = None,
        segment_bytes: int | None = None,
        cleanup_policy: str = "delete",
        compression_type: str = "none",
    ) -> None:
        if partitions is not None:
            if num_partitions != 1 and num_partitions != partitions:
                raise ValueError(
                    "num_partitions and deprecated partitions values disagree"
                )
            warnings.warn(
                "TopicConfig(partitions=...) is deprecated; use "
                "TopicConfig(num_partitions=...)",
                DeprecationWarning,
                stacklevel=3,
            )
            num_partitions = partitions

        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = dict(config or {})
        self.retention_ms = retention_ms
        self.retention_bytes = retention_bytes
        self.segment_bytes = segment_bytes
        self.cleanup_policy = cleanup_policy
        self.compression_type = compression_type

    @property
    def partitions(self) -> int:
        """Deprecated alias for :attr:`num_partitions`."""
        return self.num_partitions

    @partitions.setter
    def partitions(self, value: int) -> None:
        self.num_partitions = value


@dataclass
class PartitionInfo:
    """Partition information."""

    id: int
    """Partition ID."""

    leader: int
    """Leader broker ID."""

    replicas: list[int]
    """Replica broker IDs."""

    isr: list[int]
    """In-sync replica broker IDs."""

    high_watermark: int
    """High watermark (latest offset)."""

    log_start_offset: int = 0
    """Log start offset."""


@dataclass(init=False)
class TopicInfo:
    """Canonical topic information with compatibility for detailed metadata."""

    name: str
    partitions: int | list[PartitionInfo]
    replication_factor: int
    internal: bool
    config: TopicConfig | None

    def __init__(
        self,
        name: str,
        partitions: int | list[PartitionInfo],
        replication_factor: int | TopicConfig = 1,
        internal: bool = False,
        *,
        config: TopicConfig | None = None,
    ) -> None:
        if isinstance(replication_factor, TopicConfig):
            if config is not None:
                raise ValueError("TopicInfo config was provided twice")
            config = replication_factor
            replication_factor = config.replication_factor

        if isinstance(partitions, list):
            warnings.warn(
                "TopicInfo(partitions=[...], config=...) is deprecated; "
                "admin topic descriptions use an integer partition count",
                DeprecationWarning,
                stacklevel=2,
            )
            if config is not None and replication_factor == 1:
                replication_factor = config.replication_factor

        self.name = name
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.internal = internal
        self.config = config

    @property
    def partition_count(self) -> int:
        """Number of partitions."""
        if isinstance(self.partitions, int):
            return self.partitions
        return len(self.partitions)


@dataclass
class GroupMember:
    """Canonical consumer group member information."""

    member_id: str
    client_id: str
    host: str


@dataclass
class GroupMemberInfo:
    """Detailed consumer group member information retained for compatibility."""

    member_id: str
    """Member ID."""

    client_id: str
    """Client ID."""

    client_host: str
    """Client host."""

    assignments: list[dict[str, Any]]
    """Partition assignments."""


@dataclass(init=False)
class ConsumerGroupInfo:
    """Canonical consumer group information."""

    group_id: str
    state: str
    protocol_type: str
    protocol: str
    members: list[GroupMember | GroupMemberInfo]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Dispatch to the canonical or legacy positional constructor shape.

        Two incompatible positional constructor shapes have existed for
        this class:

        * Canonical: ``ConsumerGroupInfo(group_id, state, protocol,
          members)`` — 4 positional parameters.
        * Legacy (original ``streamline_sdk.types`` dataclass field order):
          ``ConsumerGroupInfo(group_id, state, protocol_type, protocol,
          members)`` — 5 positional, all-required fields.

        Both shapes have ``group_id``/``state`` as strings in the same
        first two positions, so they cannot be told apart by argument
        *type*. Instead, dispatch on positional argument *count*: the
        canonical constructor accepts at most 4 positional arguments, so
        exactly 5 positional arguments unambiguously means the legacy
        constructor shape is being used.
        """
        if len(args) == 5:
            legacy_fields = (
                "group_id",
                "state",
                "protocol_type",
                "protocol",
                "members",
            )
            overlap = sorted(set(legacy_fields) & set(kwargs))
            if overlap:
                raise TypeError(
                    "ConsumerGroupInfo() got multiple values for "
                    f"argument(s): {', '.join(overlap)}"
                )
            warnings.warn(
                "ConsumerGroupInfo(group_id, state, protocol_type, "
                "protocol, members) positional construction uses the "
                "deprecated legacy field order; use keyword arguments or "
                "ConsumerGroupInfo(group_id, state, protocol, members) "
                "instead",
                DeprecationWarning,
                stacklevel=2,
            )
            legacy_kwargs = dict(zip(legacy_fields, args))
            merged = {**legacy_kwargs, **kwargs}
            self._assign(
                group_id=merged["group_id"],
                state=merged["state"],
                protocol=merged.get("protocol", ""),
                members=merged.get("members"),
                protocol_type=merged.get("protocol_type", ""),
            )
            return
        self._init_canonical(*args, **kwargs)

    def _init_canonical(
        self,
        group_id: str,
        state: str,
        protocol: str = "",
        members: Sequence[GroupMember | GroupMemberInfo] | None = None,
        *,
        protocol_type: str = "",
    ) -> None:
        if protocol_type:
            warnings.warn(
                "ConsumerGroupInfo(protocol_type=...) is deprecated; "
                "use protocol for the negotiated assignment protocol",
                DeprecationWarning,
                stacklevel=3,
            )
        self._assign(
            group_id=group_id,
            state=state,
            protocol=protocol,
            members=members,
            protocol_type=protocol_type,
        )

    def _assign(
        self,
        *,
        group_id: str,
        state: str,
        protocol: str,
        members: Sequence[GroupMember | GroupMemberInfo] | None,
        protocol_type: str,
    ) -> None:
        self.group_id = group_id
        self.state = state
        self.protocol_type = protocol_type
        self.protocol = protocol
        self.members = list(members or [])


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

    columns: list[str]
    """Column names."""

    rows: list[dict[str, Any]]
    """Result rows."""

    row_count: int
    """Number of rows."""

    execution_time_ms: int
    """Execution time in milliseconds."""

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return self.row_count


class TopicPartition(NamedTuple):
    """Represents a specific topic-partition pair."""

    topic: str
    partition: int


class OffsetAndMetadata(NamedTuple):
    """Represents an offset with optional metadata."""

    offset: int
    metadata: str = ""
