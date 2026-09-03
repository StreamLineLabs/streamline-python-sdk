"""Compatibility tests for canonicalized admin model definitions."""

from __future__ import annotations

import pytest

import streamline_sdk.admin as admin_models
import streamline_sdk.types as shared_models
from streamline_sdk import TopicConfig as RootTopicConfig


def test_admin_and_types_imports_resolve_to_same_classes() -> None:
    assert admin_models.TopicConfig is shared_models.TopicConfig
    assert admin_models.TopicInfo is shared_models.TopicInfo
    assert admin_models.ConsumerGroupInfo is shared_models.ConsumerGroupInfo
    assert RootTopicConfig is shared_models.TopicConfig


def test_topic_config_preserves_admin_shape() -> None:
    config = shared_models.TopicConfig(
        name="orders",
        num_partitions=6,
        replication_factor=3,
        config={"cleanup.policy": "compact"},
    )

    assert config.name == "orders"
    assert config.num_partitions == 6
    assert config.partitions == 6
    assert config.replication_factor == 3
    assert config.config == {"cleanup.policy": "compact"}


def test_topic_config_preserves_deprecated_types_shape() -> None:
    with pytest.warns(DeprecationWarning, match="partitions"):
        config = shared_models.TopicConfig(
            partitions=6,
            replication_factor=3,
            retention_ms=86_400_000,
            cleanup_policy="compact",
        )

    assert config.num_partitions == 6
    assert config.partitions == 6
    assert config.retention_ms == 86_400_000
    assert config.cleanup_policy == "compact"


def test_topic_info_preserves_admin_and_detailed_shapes() -> None:
    admin_info = admin_models.TopicInfo(
        name="orders",
        partitions=6,
        replication_factor=3,
    )
    assert admin_info.partition_count == 6

    partitions = [
        shared_models.PartitionInfo(
            id=0,
            leader=1,
            replicas=[1],
            isr=[1],
            high_watermark=42,
        )
    ]
    with pytest.warns(DeprecationWarning, match="integer partition count"):
        detailed_info = shared_models.TopicInfo(
            name="orders",
            partitions=partitions,
            config=shared_models.TopicConfig(),
        )

    assert detailed_info.partition_count == 1
    assert detailed_info.config is not None


def test_consumer_group_info_preserves_protocol_type() -> None:
    member = shared_models.GroupMemberInfo(
        member_id="member-1",
        client_id="client-1",
        client_host="/127.0.0.1",
        assignments=[],
    )
    with pytest.warns(DeprecationWarning, match="protocol_type"):
        info = shared_models.ConsumerGroupInfo(
            group_id="workers",
            state="Stable",
            protocol_type="consumer",
            protocol="range",
            members=[member],
        )

    assert info.protocol_type == "consumer"
    assert info.protocol == "range"
    assert info.members == [member]


def test_topic_config_legacy_positional_constructor_dispatch() -> None:
    """The pre-canonicalization ``streamline_sdk.types.TopicConfig`` dataclass
    had positional field order ``(partitions, replication_factor,
    retention_ms, retention_bytes, segment_bytes, cleanup_policy,
    compression_type)`` — no ``name`` field existed. Old call sites that
    still construct it positionally must keep working and must not be
    silently misrouted into the new ``(name, num_partitions,
    replication_factor, config)`` positional shape.
    """
    with pytest.warns(DeprecationWarning, match="legacy field order"):
        config = shared_models.TopicConfig(
            6, 3, 86_400_000, 1_073_741_824, 512, "compact", "gzip"
        )

    assert config.name == ""
    assert config.num_partitions == 6
    assert config.partitions == 6
    assert config.replication_factor == 3
    assert config.retention_ms == 86_400_000
    assert config.retention_bytes == 1_073_741_824
    assert config.segment_bytes == 512
    assert config.cleanup_policy == "compact"
    assert config.compression_type == "gzip"


def test_topic_config_legacy_positional_partial_args() -> None:
    """A bare legacy positional int (matching the old single-field
    ``TopicConfig(partitions)`` call) must be treated as ``partitions``,
    never as ``name``."""
    with pytest.warns(DeprecationWarning, match="legacy field order"):
        config = shared_models.TopicConfig(6)

    assert config.num_partitions == 6
    assert config.replication_factor == 1
    assert config.name == ""


def test_topic_config_canonical_positional_constructor_still_works() -> None:
    """The new admin-facing positional shape (``name`` first, as a string)
    must not be affected by the legacy-positional dispatch."""
    config = shared_models.TopicConfig("orders", 6, 3, {"cleanup.policy": "compact"})

    assert config.name == "orders"
    assert config.num_partitions == 6
    assert config.replication_factor == 3
    assert config.config == {"cleanup.policy": "compact"}


def test_topic_config_legacy_positional_conflicting_keyword_raises() -> None:
    with pytest.raises(TypeError, match="multiple values"):
        shared_models.TopicConfig(6, 3, replication_factor=9)


def test_topic_config_legacy_positional_too_many_args_raises() -> None:
    with pytest.raises(TypeError, match="at most"):
        shared_models.TopicConfig(6, 3, None, None, None, "delete", "none", "extra")


def test_consumer_group_info_legacy_positional_constructor_dispatch() -> None:
    """The pre-canonicalization ``ConsumerGroupInfo`` dataclass had
    positional field order ``(group_id, state, protocol_type, protocol,
    members)``. Old call sites that still construct it positionally with
    all 5 fields must keep working and must not be silently misrouted into
    the new ``(group_id, state, protocol, members)`` 4-positional shape
    (which would otherwise put ``protocol_type`` into ``protocol`` and
    ``protocol`` into ``members``).
    """
    member = shared_models.GroupMemberInfo(
        member_id="member-1",
        client_id="client-1",
        client_host="/127.0.0.1",
        assignments=[],
    )
    with pytest.warns(DeprecationWarning, match="legacy field order"):
        info = shared_models.ConsumerGroupInfo(
            "workers", "Stable", "consumer", "range", [member]
        )

    assert info.group_id == "workers"
    assert info.state == "Stable"
    assert info.protocol_type == "consumer"
    assert info.protocol == "range"
    assert info.members == [member]


def test_consumer_group_info_canonical_positional_constructor_still_works() -> None:
    info = shared_models.ConsumerGroupInfo("workers", "Stable", "range", [])

    assert info.group_id == "workers"
    assert info.state == "Stable"
    assert info.protocol == "range"
    assert info.protocol_type == ""
    assert info.members == []


def test_consumer_group_info_legacy_positional_conflicting_keyword_raises() -> None:
    with pytest.raises(TypeError, match="multiple values"):
        shared_models.ConsumerGroupInfo(
            "workers", "Stable", "consumer", "range", [], protocol="range2"
        )
