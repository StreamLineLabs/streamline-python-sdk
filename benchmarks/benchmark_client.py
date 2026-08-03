"""Benchmarks for Streamline Python SDK."""


from datetime import datetime

from streamline_sdk import StreamlineClient
from streamline_sdk.client import ClientConfig, ConsumerConfig, ProducerConfig
from streamline_sdk.consumer import ConsumerRecord
from streamline_sdk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
    ConsumerError,
    ProducerError,
    SerializationError,
    StreamlineError,
    TimeoutError,
    TopicError,
)
from streamline_sdk.producer import ProducerRecord, RecordMetadata
from streamline_sdk.retry import RetryConfig
from streamline_sdk.types import (
    ConsumerGroupInfo,
    GroupMemberInfo,
    Message,
    OffsetInfo,
    PartitionInfo,
    ProduceResult,
    QueryResult,
    Record,
    TopicConfig,
    TopicInfo,
)

# ---------------------------------------------------------------------------
# Group: client_construction
# ---------------------------------------------------------------------------


class TestClientConstruction:
    """Benchmarks for client and config object construction."""

    def test_client_config_defaults(self, benchmark):
        """Benchmark ClientConfig creation with all defaults."""
        benchmark.group = "client_construction"
        benchmark(ClientConfig)

    def test_client_config_custom(self, benchmark):
        """Benchmark ClientConfig creation with custom values."""
        benchmark.group = "client_construction"
        benchmark(
            ClientConfig,
            bootstrap_servers="broker1:9092,broker2:9092,broker3:9092",
            client_id="bench-client",
            request_timeout_ms=60000,
            metadata_max_age_ms=600000,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="bench-user",
            sasl_password="bench-pass",
        )

    def test_streamline_client_default(self, benchmark):
        """Benchmark StreamlineClient construction with defaults."""
        benchmark.group = "client_construction"
        benchmark(StreamlineClient)

    def test_streamline_client_custom(self, benchmark):
        """Benchmark StreamlineClient construction with custom configs."""
        benchmark.group = "client_construction"

        def create():
            return StreamlineClient(
                bootstrap_servers="broker1:9092,broker2:9092",
                client_id="bench-client",
                producer_config=ProducerConfig(acks=1, compression_type="gzip"),
                consumer_config=ConsumerConfig(
                    group_id="bench-group", auto_offset_reset="earliest"
                ),
            )

        benchmark(create)

    def test_streamline_client_with_config_object(self, benchmark):
        """Benchmark StreamlineClient construction from a ClientConfig object."""
        benchmark.group = "client_construction"
        cfg = ClientConfig(
            bootstrap_servers="broker:9092",
            client_id="obj-client",
        )
        benchmark(StreamlineClient, client_config=cfg)


# ---------------------------------------------------------------------------
# Group: producer_config
# ---------------------------------------------------------------------------


class TestProducerConfiguration:
    """Benchmarks for producer configuration creation."""

    def test_producer_config_defaults(self, benchmark):
        """Benchmark ProducerConfig with all defaults."""
        benchmark.group = "producer_config"
        benchmark(ProducerConfig)

    def test_producer_config_custom(self, benchmark):
        """Benchmark ProducerConfig with custom values."""
        benchmark.group = "producer_config"
        benchmark(
            ProducerConfig,
            acks=1,
            compression_type="zstd",
            batch_size=32768,
            linger_ms=50,
            max_request_size=2097152,
            enable_idempotence=True,
            retries=5,
        )

    def test_producer_record_minimal(self, benchmark):
        """Benchmark ProducerRecord with minimal fields."""
        benchmark.group = "producer_config"
        benchmark(ProducerRecord, topic="bench-topic", value=b"hello")

    def test_producer_record_full(self, benchmark, sample_headers):
        """Benchmark ProducerRecord with all fields populated."""
        benchmark.group = "producer_config"
        benchmark(
            ProducerRecord,
            topic="bench-topic",
            value=b"benchmark-payload-data",
            key=b"bench-key",
            partition=3,
            timestamp_ms=1700000000000,
            headers=sample_headers,
        )

    def test_record_metadata_creation(self, benchmark):
        """Benchmark RecordMetadata construction."""
        benchmark.group = "producer_config"
        now = datetime.now()
        benchmark(
            RecordMetadata,
            topic="bench-topic",
            partition=0,
            offset=42,
            timestamp=now,
            serialized_key_size=9,
            serialized_value_size=22,
        )


# ---------------------------------------------------------------------------
# Group: consumer_config
# ---------------------------------------------------------------------------


class TestConsumerConfiguration:
    """Benchmarks for consumer configuration creation."""

    def test_consumer_config_defaults(self, benchmark):
        """Benchmark ConsumerConfig with all defaults."""
        benchmark.group = "consumer_config"
        benchmark(ConsumerConfig)

    def test_consumer_config_custom(self, benchmark):
        """Benchmark ConsumerConfig with custom values."""
        benchmark.group = "consumer_config"
        benchmark(
            ConsumerConfig,
            group_id="bench-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            auto_commit_interval_ms=10000,
            max_poll_records=1000,
            session_timeout_ms=60000,
            heartbeat_interval_ms=5000,
            isolation_level="read_committed",
        )

    def test_consumer_record_creation(self, benchmark):
        """Benchmark ConsumerRecord construction."""
        benchmark.group = "consumer_config"
        now = datetime.now()
        benchmark(
            ConsumerRecord,
            topic="bench-topic",
            partition=1,
            offset=100,
            key=b"key",
            value=b"value",
            timestamp=now,
            headers={"trace": b"abc"},
        )


# ---------------------------------------------------------------------------
# Group: types
# ---------------------------------------------------------------------------


class TestTypeConstruction:
    """Benchmarks for SDK type construction and properties."""

    def test_message_creation(self, benchmark):
        """Benchmark Message dataclass creation."""
        benchmark.group = "types"
        benchmark(
            Message,
            topic="events",
            partition=2,
            offset=100,
            timestamp=1700000000000,
            key="user-42",
            value={"event": "click", "page": "/home"},
            headers={"trace": "abc-123"},
        )

    def test_message_datetime_property(self, benchmark):
        """Benchmark Message.datetime computed property."""
        benchmark.group = "types"
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            timestamp=1700000000000,
            key=None,
            value="v",
        )
        benchmark(lambda: msg.datetime)

    def test_message_repr(self, benchmark):
        """Benchmark Message.__repr__."""
        benchmark.group = "types"
        msg = Message(
            topic="events", partition=1, offset=42, timestamp=0, key="k1", value="v"
        )
        benchmark(repr, msg)

    def test_record_creation(self, benchmark):
        """Benchmark Record dataclass creation."""
        benchmark.group = "types"
        benchmark(
            Record,
            value={"data": [1, 2, 3]},
            key="key-1",
            headers={"h": "v"},
            partition=0,
            timestamp=1700000000000,
        )

    def test_topic_config_creation(self, benchmark):
        """Benchmark types.TopicConfig creation."""
        benchmark.group = "types"
        benchmark(
            TopicConfig,
            partitions=12,
            replication_factor=3,
            retention_ms=86400000,
            cleanup_policy="compact",
            compression_type="zstd",
        )

    def test_partition_info_creation(self, benchmark):
        """Benchmark types.PartitionInfo creation."""
        benchmark.group = "types"
        benchmark(
            PartitionInfo,
            id=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2, 3],
            high_watermark=5000,
            log_start_offset=100,
        )

    def test_topic_info_creation(self, benchmark):
        """Benchmark types.TopicInfo creation with multiple partitions."""
        benchmark.group = "types"
        parts = [
            PartitionInfo(
                id=i, leader=0, replicas=[0, 1], isr=[0, 1], high_watermark=1000
            )
            for i in range(6)
        ]
        cfg = TopicConfig(partitions=6, replication_factor=2)
        benchmark(TopicInfo, name="bench-topic", partitions=parts, config=cfg)

    def test_topic_info_partition_count(self, benchmark):
        """Benchmark TopicInfo.partition_count property."""
        benchmark.group = "types"
        parts = [
            PartitionInfo(id=i, leader=0, replicas=[0], isr=[0], high_watermark=0)
            for i in range(12)
        ]
        info = TopicInfo(name="t", partitions=parts, config=TopicConfig())
        benchmark(lambda: info.partition_count)

    def test_offset_info_lag(self, benchmark):
        """Benchmark OffsetInfo.lag property."""
        benchmark.group = "types"
        info = OffsetInfo(
            topic="t", partition=0, current_offset=500, log_end_offset=1000
        )
        benchmark(lambda: info.lag)

    def test_produce_result_creation(self, benchmark):
        """Benchmark ProduceResult creation."""
        benchmark.group = "types"
        benchmark(
            ProduceResult,
            topic="events",
            partition=2,
            offset=42,
            timestamp=1700000000000,
        )

    def test_query_result_creation(self, benchmark):
        """Benchmark QueryResult creation."""
        benchmark.group = "types"
        rows = [{"id": i, "name": f"item-{i}"} for i in range(20)]
        benchmark(
            QueryResult,
            columns=["id", "name"],
            rows=rows,
            row_count=20,
            execution_time_ms=5,
        )

    def test_query_result_iteration(self, benchmark):
        """Benchmark iterating over QueryResult rows."""
        benchmark.group = "types"
        rows = [{"id": i, "val": i * 10} for i in range(50)]
        result = QueryResult(
            columns=["id", "val"], rows=rows, row_count=50, execution_time_ms=1
        )
        benchmark(lambda: list(result))

    def test_consumer_group_info_creation(self, benchmark):
        """Benchmark ConsumerGroupInfo with members."""
        benchmark.group = "types"
        members = [
            GroupMemberInfo(
                member_id=f"member-{i}",
                client_id=f"client-{i}",
                client_host=f"host-{i}",
                assignments=[{"topic": "t", "partitions": [i]}],
            )
            for i in range(5)
        ]
        benchmark(
            ConsumerGroupInfo,
            group_id="bench-group",
            state="Stable",
            protocol_type="consumer",
            protocol="range",
            members=members,
        )


# ---------------------------------------------------------------------------
# Group: exceptions
# ---------------------------------------------------------------------------


class TestExceptions:
    """Benchmarks for exception creation and error handling."""

    def test_streamline_error(self, benchmark):
        """Benchmark base StreamlineError creation."""
        benchmark.group = "exceptions"
        benchmark(StreamlineError, "something went wrong")

    def test_connection_error(self, benchmark):
        """Benchmark ConnectionError creation."""
        benchmark.group = "exceptions"
        benchmark(ConnectionError, "connection refused")

    def test_producer_error(self, benchmark):
        """Benchmark ProducerError creation."""
        benchmark.group = "exceptions"
        benchmark(ProducerError, "send failed")

    def test_consumer_error(self, benchmark):
        """Benchmark ConsumerError creation."""
        benchmark.group = "exceptions"
        benchmark(ConsumerError, "poll failed")

    def test_topic_error(self, benchmark):
        """Benchmark TopicError creation."""
        benchmark.group = "exceptions"
        benchmark(TopicError, "topic not found")

    def test_authentication_error(self, benchmark):
        """Benchmark AuthenticationError creation."""
        benchmark.group = "exceptions"
        benchmark(AuthenticationError, "invalid credentials")

    def test_authorization_error(self, benchmark):
        """Benchmark AuthorizationError creation."""
        benchmark.group = "exceptions"
        benchmark(AuthorizationError, "access denied")

    def test_serialization_error(self, benchmark):
        """Benchmark SerializationError creation."""
        benchmark.group = "exceptions"
        benchmark(SerializationError, "invalid json")

    def test_timeout_error(self, benchmark):
        """Benchmark TimeoutError creation."""
        benchmark.group = "exceptions"
        benchmark(TimeoutError, "request timed out")

    def test_exception_isinstance_check(self, benchmark):
        """Benchmark isinstance check against exception hierarchy."""
        benchmark.group = "exceptions"
        err = ProducerError("test")
        benchmark(isinstance, err, StreamlineError)

    def test_exception_raise_and_catch(self, benchmark):
        """Benchmark raising and catching a StreamlineError."""
        benchmark.group = "exceptions"

        def raise_and_catch():
            try:
                raise ProducerError("bench error")
            except StreamlineError:
                pass

        benchmark(raise_and_catch)


# ---------------------------------------------------------------------------
# Group: retry
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    """Benchmarks for retry policy configuration and evaluation."""

    def test_retry_config_defaults(self, benchmark):
        """Benchmark RetryConfig creation with defaults."""
        benchmark.group = "retry"
        benchmark(RetryConfig)

    def test_retry_config_custom(self, benchmark):
        """Benchmark RetryConfig creation with custom values."""
        benchmark.group = "retry"
        benchmark(
            RetryConfig,
            max_retries=10,
            initial_backoff_ms=50,
            max_backoff_ms=30000,
            backoff_multiplier=3.0,
            retryable_exceptions=[ConnectionError, TimeoutError, ProducerError],
        )

    def test_retry_config_exception_check(self, benchmark):
        """Benchmark checking if an exception is retryable."""
        benchmark.group = "retry"
        cfg = RetryConfig()
        err = ConnectionError("lost connection")
        benchmark(isinstance, err, cfg.retryable_exceptions)

    def test_retry_config_non_retryable_check(self, benchmark):
        """Benchmark checking a non-retryable exception."""
        benchmark.group = "retry"
        cfg = RetryConfig()
        err = ProducerError("bad message")
        benchmark(isinstance, err, cfg.retryable_exceptions)

    def test_backoff_calculation(self, benchmark):
        """Benchmark backoff calculation logic (simulated)."""
        benchmark.group = "retry"

        def compute_backoff(initial_ms, multiplier, max_ms, attempt):
            backoff = initial_ms
            for _ in range(attempt):
                backoff = min(int(backoff * multiplier), max_ms)
            return backoff

        benchmark(compute_backoff, 100, 2.0, 10000, 5)


# ---------------------------------------------------------------------------
# Group: serialization
# ---------------------------------------------------------------------------


class TestSerialization:
    """Benchmarks for type serialization and deserialization patterns."""

    def test_message_to_dict(self, benchmark):
        """Benchmark converting a Message to a dict via dataclasses.asdict."""
        benchmark.group = "serialization"
        from dataclasses import asdict

        msg = Message(
            topic="events",
            partition=0,
            offset=42,
            timestamp=1700000000000,
            key="k",
            value={"data": "test"},
            headers={"h": "v"},
        )
        benchmark(asdict, msg)

    def test_record_to_dict(self, benchmark):
        """Benchmark converting a Record to a dict."""
        benchmark.group = "serialization"
        from dataclasses import asdict

        rec = Record(value={"data": [1, 2, 3]}, key="k", headers={"h": "v"})
        benchmark(asdict, rec)

    def test_topic_config_to_dict(self, benchmark):
        """Benchmark converting TopicConfig to a dict."""
        benchmark.group = "serialization"
        from dataclasses import asdict

        cfg = TopicConfig(
            partitions=6,
            replication_factor=3,
            retention_ms=86400000,
            cleanup_policy="compact",
        )
        benchmark(asdict, cfg)

    def test_dict_to_message(self, benchmark):
        """Benchmark constructing a Message from a dict (deserialization)."""
        benchmark.group = "serialization"
        data = {
            "topic": "events",
            "partition": 0,
            "offset": 42,
            "timestamp": 1700000000000,
            "key": "k",
            "value": {"data": "test"},
            "headers": {"h": "v"},
        }
        benchmark(lambda: Message(**data))

    def test_dict_to_record(self, benchmark):
        """Benchmark constructing a Record from a dict."""
        benchmark.group = "serialization"
        data = {"value": "hello", "key": "k", "headers": {"h": "v"}}
        benchmark(lambda: Record(**data))

    def test_produce_result_to_dict(self, benchmark):
        """Benchmark converting ProduceResult to a dict."""
        benchmark.group = "serialization"
        from dataclasses import asdict

        result = ProduceResult(
            topic="events", partition=2, offset=42, timestamp=1700000000000
        )
        benchmark(asdict, result)

    def test_query_result_to_dict(self, benchmark):
        """Benchmark converting QueryResult to a dict."""
        benchmark.group = "serialization"
        from dataclasses import asdict

        rows = [{"id": i, "name": f"item-{i}"} for i in range(10)]
        result = QueryResult(
            columns=["id", "name"], rows=rows, row_count=10, execution_time_ms=5
        )
        benchmark(asdict, result)
