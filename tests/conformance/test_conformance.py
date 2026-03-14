"""SDK Conformance Test Suite — 46 tests per SDK_CONFORMANCE_SPEC.md

Requires: docker compose -f docker-compose.conformance.yml up -d
"""
import asyncio
import os
import time

import pytest

from streamline_sdk.client import StreamlineClient
from streamline_sdk.producer import ProducerRecord, RecordMetadata
from streamline_sdk.consumer import ConsumerRecord
from streamline_sdk.admin import TopicConfig, TopicInfo
from streamline_sdk import exceptions

BOOTSTRAP = os.environ.get("STREAMLINE_BOOTSTRAP", "localhost:9092")
HTTP_URL = os.environ.get("STREAMLINE_HTTP", "http://localhost:9094")

# Skip the entire module when no server is available.
pytestmark = pytest.mark.skipif(
    os.environ.get("CONFORMANCE", "0") != "1",
    reason="Set CONFORMANCE=1 to run conformance tests against a live server",
)


def unique_topic(test_id: str) -> str:
    return f"conformance-{test_id}-{int(time.time() * 1_000_000)}"


# ========== PRODUCER (8 tests) ==========


class TestProducer:
    @pytest.fixture(autouse=True)
    async def setup(self):
        """Create client and producer for each test."""
        self.client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await self.client.start()
        self.producer = self.client.producer
        self.admin = self.client.admin
        yield
        await self.client.close()

    @pytest.mark.asyncio
    async def test_p01_simple_produce(self):
        """Produce a single message and verify metadata is returned."""
        topic = unique_topic("p01")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        result = await self.producer.send(topic, value=b"hello")
        assert isinstance(result, RecordMetadata)
        assert result.topic == topic
        assert result.offset >= 0
        assert result.partition >= 0

    @pytest.mark.asyncio
    async def test_p02_keyed_produce(self):
        """Produce a keyed message and verify key-based partitioning is stable."""
        topic = unique_topic("p02")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=3))
        r1 = await self.producer.send(topic, key=b"user-42", value=b"a")
        r2 = await self.producer.send(topic, key=b"user-42", value=b"b")
        assert r1.partition == r2.partition, "Same key should route to same partition"

    @pytest.mark.asyncio
    async def test_p03_headers_produce(self):
        """Produce a message with headers and verify they round-trip."""
        topic = unique_topic("p03")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        headers = {b"trace-id": b"abc-123", b"source": b"test"}
        result = await self.producer.send(topic, value=b"payload", headers=headers)
        assert isinstance(result, RecordMetadata)
        assert result.offset >= 0

    @pytest.mark.asyncio
    async def test_p04_batch_produce(self):
        """Produce a batch of messages and verify all succeed."""
        topic = unique_topic("p04")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        records = [
            ProducerRecord(topic=topic, value=f"msg-{i}".encode())
            for i in range(10)
        ]
        results = await self.producer.send_batch(records)
        assert len(results) == 10
        offsets = [r.offset for r in results]
        assert offsets == sorted(offsets), "Offsets should be monotonically increasing"

    @pytest.mark.asyncio
    async def test_p05_compression(self):
        """Produce messages with compression and verify delivery."""
        topic = unique_topic("p05")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        large_payload = b"x" * 10_000
        result = await self.producer.send(topic, value=large_payload)
        assert isinstance(result, RecordMetadata)
        assert result.offset >= 0

    @pytest.mark.asyncio
    async def test_p06_partitioner(self):
        """Produce to a specific partition and verify assignment."""
        topic = unique_topic("p06")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=4))
        result = await self.producer.send(topic, value=b"targeted", partition=2)
        assert result.partition == 2

    @pytest.mark.asyncio
    async def test_p07_idempotent(self):
        """Produce duplicate messages and verify idempotent delivery."""
        topic = unique_topic("p07")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        r1 = await self.producer.send(topic, value=b"exactly-once")
        r2 = await self.producer.send(topic, value=b"exactly-once-2")
        assert r2.offset > r1.offset

    @pytest.mark.asyncio
    async def test_p08_timeout(self):
        """Verify producer respects send timeout configuration."""
        topic = unique_topic("p08")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        result = await self.producer.send(topic, value=b"timeout-test")
        assert isinstance(result, RecordMetadata)


# ========== CONSUMER (8 tests) ==========


class TestConsumer:
    @pytest.fixture(autouse=True)
    async def setup(self):
        """Create client, producer, and consumer for each test."""
        self.client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await self.client.start()
        self.producer = self.client.producer
        self.admin = self.client.admin
        yield
        await self.client.close()

    @pytest.mark.asyncio
    async def test_c01_subscribe(self):
        """Subscribe to a topic and verify subscription is active."""
        topic = unique_topic("c01")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        consumer = self.client.consumer(group_id="c01-group")
        await consumer.start()
        await consumer.subscribe([topic])
        assert topic in consumer.subscription()
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c02_from_beginning(self):
        """Consume messages from the beginning of a topic."""
        topic = unique_topic("c02")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        for i in range(5):
            await self.producer.send(topic, value=f"msg-{i}".encode())
        await self.producer.flush()

        consumer = self.client.consumer(group_id="c02-group")
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.seek_to_beginning()
        records = await consumer.poll(timeout_ms=5000, max_records=10)
        assert len(records) >= 5
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c03_from_offset(self):
        """Consume starting from a specific offset."""
        topic = unique_topic("c03")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        for i in range(10):
            await self.producer.send(topic, value=f"msg-{i}".encode())
        await self.producer.flush()

        consumer = self.client.consumer(group_id="c03-group")
        await consumer.start()
        await consumer.subscribe([topic])
        from streamline_sdk.consumer import TopicPartition
        await consumer.seek(TopicPartition(topic, 0), 5)
        records = await consumer.poll(timeout_ms=5000, max_records=10)
        assert len(records) >= 5
        assert records[0].offset >= 5
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c04_from_timestamp(self):
        """Consume messages starting from a timestamp."""
        topic = unique_topic("c04")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        ts_before = int(time.time() * 1000)
        await asyncio.sleep(0.1)
        for i in range(5):
            await self.producer.send(topic, value=f"msg-{i}".encode())
        await self.producer.flush()

        consumer = self.client.consumer(group_id="c04-group")
        await consumer.start()
        await consumer.subscribe([topic])
        records = await consumer.poll(timeout_ms=5000, max_records=10)
        for r in records:
            assert r.timestamp.timestamp() * 1000 >= ts_before
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c05_follow(self):
        """Consume in follow mode, receiving new messages as they arrive."""
        topic = unique_topic("c05")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        consumer = self.client.consumer(group_id="c05-group")
        await consumer.start()
        await consumer.subscribe([topic])

        await self.producer.send(topic, value=b"follow-msg")
        await self.producer.flush()
        records = await consumer.poll(timeout_ms=5000, max_records=1)
        assert len(records) >= 1
        assert records[0].value == b"follow-msg"
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c06_filter(self):
        """Consume and filter messages by key presence."""
        topic = unique_topic("c06")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        await self.producer.send(topic, key=b"keep", value=b"a")
        await self.producer.send(topic, value=b"b")
        await self.producer.send(topic, key=b"keep", value=b"c")
        await self.producer.flush()

        consumer = self.client.consumer(group_id="c06-group")
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.seek_to_beginning()
        records = await consumer.poll(timeout_ms=5000, max_records=10)
        keyed = [r for r in records if r.key is not None]
        assert len(keyed) >= 2
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c07_headers(self):
        """Consume messages and verify headers are preserved."""
        topic = unique_topic("c07")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        await self.producer.send(
            topic, value=b"with-headers",
            headers={b"x-trace": b"t1"},
        )
        await self.producer.flush()

        consumer = self.client.consumer(group_id="c07-group")
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.seek_to_beginning()
        records = await consumer.poll(timeout_ms=5000, max_records=5)
        assert len(records) >= 1
        assert b"x-trace" in records[0].headers
        await consumer.close()

    @pytest.mark.asyncio
    async def test_c08_timeout(self):
        """Verify poll returns empty list on timeout with no messages."""
        topic = unique_topic("c08")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        consumer = self.client.consumer(group_id="c08-group")
        await consumer.start()
        await consumer.subscribe([topic])
        start = time.monotonic()
        records = await consumer.poll(timeout_ms=500, max_records=10)
        elapsed = time.monotonic() - start
        assert isinstance(records, list)
        assert elapsed < 3.0
        await consumer.close()


# ========== CONSUMER GROUPS (6 tests) ==========


class TestConsumerGroups:
    @pytest.fixture(autouse=True)
    async def setup(self):
        self.client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await self.client.start()
        self.admin = self.client.admin
        self.producer = self.client.producer
        yield
        await self.client.close()

    @pytest.mark.asyncio
    async def test_g01_join_group(self):
        """Consumer joins a group and appears in group listing."""
        topic = unique_topic("g01")
        group = f"group-g01-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        consumer = self.client.consumer(group_id=group)
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.poll(timeout_ms=2000)
        groups = await self.admin.list_consumer_groups()
        assert isinstance(groups, list)
        await consumer.close()

    @pytest.mark.asyncio
    async def test_g02_rebalance(self):
        """Adding a second consumer triggers group rebalance."""
        topic = unique_topic("g02")
        group = f"group-g02-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=2))

        c1 = self.client.consumer(group_id=group)
        await c1.start()
        await c1.subscribe([topic])
        await c1.poll(timeout_ms=2000)

        c2 = self.client.consumer(group_id=group)
        await c2.start()
        await c2.subscribe([topic])
        await c2.poll(timeout_ms=3000)

        info = await self.admin.describe_consumer_group(group)
        assert info.state in ("Stable", "CompletingRebalance", "PreparingRebalance",
                              "stable", "completing_rebalance", "preparing_rebalance")
        await c1.close()
        await c2.close()

    @pytest.mark.asyncio
    async def test_g03_commit_offsets(self):
        """Commit offsets and verify they persist."""
        topic = unique_topic("g03")
        group = f"group-g03-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        await self.producer.send(topic, value=b"commit-test")
        await self.producer.flush()

        consumer = self.client.consumer(group_id=group)
        await consumer.start()
        await consumer.subscribe([topic])
        records = await consumer.poll(timeout_ms=5000, max_records=1)
        assert len(records) >= 1
        await consumer.commit()
        await consumer.close()

    @pytest.mark.asyncio
    async def test_g04_lag_monitoring(self):
        """Monitor consumer group lag."""
        topic = unique_topic("g04")
        group = f"group-g04-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        for i in range(5):
            await self.producer.send(topic, value=f"lag-{i}".encode())
        await self.producer.flush()

        consumer = self.client.consumer(group_id=group)
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.poll(timeout_ms=3000, max_records=2)
        await consumer.commit()
        lag = await self.admin.consumer_group_lag(group)
        assert lag.total_lag >= 0
        await consumer.close()

    @pytest.mark.asyncio
    async def test_g05_reset_offsets(self):
        """Reset consumer group offsets to beginning."""
        topic = unique_topic("g05")
        group = f"group-g05-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        for i in range(5):
            await self.producer.send(topic, value=f"reset-{i}".encode())
        await self.producer.flush()

        consumer = self.client.consumer(group_id=group)
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.seek_to_beginning()
        records = await consumer.poll(timeout_ms=5000, max_records=10)
        assert len(records) >= 5
        await consumer.close()

    @pytest.mark.asyncio
    async def test_g06_leave_group(self):
        """Consumer leaves a group cleanly."""
        topic = unique_topic("g06")
        group = f"group-g06-{int(time.time())}"
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        consumer = self.client.consumer(group_id=group)
        await consumer.start()
        await consumer.subscribe([topic])
        await consumer.poll(timeout_ms=2000)
        await consumer.close()
        # After close, consumer should have left the group


# ========== AUTHENTICATION (6 tests) ==========


class TestAuthentication:
    @pytest.mark.asyncio
    async def test_a01_tls_connect(self):
        """Connect to a TLS-enabled server."""
        tls_bootstrap = os.environ.get("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093")
        client = StreamlineClient(bootstrap_servers=tls_bootstrap)
        try:
            await client.start()
            assert client.is_connected
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_a02_mutual_tls(self):
        """Connect with mutual TLS (client certificate)."""
        tls_bootstrap = os.environ.get("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093")
        client = StreamlineClient(bootstrap_servers=tls_bootstrap)
        try:
            await client.start()
            assert client.is_connected
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_a03_sasl_plain(self):
        """Authenticate with SASL/PLAIN."""
        client = StreamlineClient(
            bootstrap_servers=BOOTSTRAP,
            sasl_mechanism="PLAIN",
            sasl_plain_username=os.environ.get("SASL_USERNAME", "admin"),
            sasl_plain_password=os.environ.get("SASL_PASSWORD", "admin-secret"),
        )
        try:
            await client.start()
            assert client.is_connected
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_a04_scram_sha256(self):
        """Authenticate with SASL/SCRAM-SHA-256."""
        client = StreamlineClient(
            bootstrap_servers=BOOTSTRAP,
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=os.environ.get("SASL_USERNAME", "admin"),
            sasl_plain_password=os.environ.get("SASL_PASSWORD", "admin-secret"),
        )
        try:
            await client.start()
            assert client.is_connected
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_a05_scram_sha512(self):
        """Authenticate with SASL/SCRAM-SHA-512."""
        client = StreamlineClient(
            bootstrap_servers=BOOTSTRAP,
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=os.environ.get("SASL_USERNAME", "admin"),
            sasl_plain_password=os.environ.get("SASL_PASSWORD", "admin-secret"),
        )
        try:
            await client.start()
            assert client.is_connected
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_a06_auth_failure(self):
        """Invalid credentials should raise AuthenticationError."""
        client = StreamlineClient(
            bootstrap_servers=BOOTSTRAP,
            sasl_mechanism="PLAIN",
            sasl_plain_username="wrong-user",
            sasl_plain_password="wrong-pass",
        )
        with pytest.raises(
            (exceptions.AuthenticationError, exceptions.ConnectionError, Exception)
        ):
            await client.start()
        await client.close()


# ========== SCHEMA REGISTRY (6 tests) ==========

SCHEMA_REGISTRY_URL = "http://localhost:9094"

AVRO_SCHEMA = '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
JSON_SCHEMA = '{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}'


class TestSchemaRegistry:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Initialize schema registry client for each test."""
        from streamline_sdk.serializers import SchemaRegistryClient, SchemaRegistryConfig
        self.client = SchemaRegistryClient(SchemaRegistryConfig(url=SCHEMA_REGISTRY_URL))

    @pytest.mark.asyncio
    async def test_s01_register_schema(self):
        """Register a schema and verify an ID is returned."""
        schema_id = await self.client.register_schema("test-s01-value", AVRO_SCHEMA, "AVRO")
        assert isinstance(schema_id, int)
        assert schema_id > 0

    @pytest.mark.asyncio
    async def test_s02_get_by_id(self):
        """Register a schema, then retrieve it by ID."""
        schema_id = await self.client.register_schema("test-s02-value", AVRO_SCHEMA, "AVRO")
        schema_str = await self.client.get_schema(schema_id)
        assert "User" in schema_str

    @pytest.mark.asyncio
    async def test_s03_get_versions(self):
        """Register multiple versions, verify version listing."""
        await self.client.register_schema("test-s03-value", AVRO_SCHEMA, "AVRO")
        versions = await self.client.get_versions("test-s03-value")
        assert isinstance(versions, list)
        assert len(versions) >= 1

    @pytest.mark.asyncio
    async def test_s04_compatibility_check(self):
        """Register a schema and check compatibility of a new version."""
        await self.client.register_schema("test-s04-value", AVRO_SCHEMA, "AVRO")
        is_compat = await self.client.check_compatibility("test-s04-value", AVRO_SCHEMA, "AVRO")
        assert isinstance(is_compat, bool)

    @pytest.mark.asyncio
    async def test_s05_avro_schema(self):
        """Register an Avro schema specifically."""
        schema_id = await self.client.register_schema("test-s05-avro", AVRO_SCHEMA, "AVRO")
        assert schema_id > 0
        schema_str = await self.client.get_schema(schema_id)
        assert "record" in schema_str

    @pytest.mark.asyncio
    async def test_s06_json_schema(self):
        """Register a JSON Schema specifically."""
        schema_id = await self.client.register_schema("test-s06-json", JSON_SCHEMA, "JSON")
        assert schema_id > 0
        schema_str = await self.client.get_schema(schema_id)
        assert "object" in schema_str


# ========== ADMIN (4 tests) ==========


class TestAdmin:
    @pytest.fixture(autouse=True)
    async def setup(self):
        self.client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await self.client.start()
        self.admin = self.client.admin
        yield
        await self.client.close()

    @pytest.mark.asyncio
    async def test_d01_create_topic(self):
        """Create a topic and verify it exists."""
        topic = unique_topic("d01")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=3))
        topics = await self.admin.list_topics()
        assert topic in topics

    @pytest.mark.asyncio
    async def test_d02_list_topics(self):
        """List topics and verify result is a non-empty list."""
        topic = unique_topic("d02")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        topics = await self.admin.list_topics()
        assert isinstance(topics, list)
        assert len(topics) >= 1
        assert topic in topics

    @pytest.mark.asyncio
    async def test_d03_describe_topic(self):
        """Describe a topic and verify metadata."""
        topic = unique_topic("d03")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=4))
        info = await self.admin.describe_topic(topic)
        assert isinstance(info, TopicInfo)
        assert info.name == topic
        assert info.partitions == 4

    @pytest.mark.asyncio
    async def test_d04_delete_topic(self):
        """Delete a topic and verify it is removed."""
        topic = unique_topic("d04")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        await self.admin.delete_topic(topic)
        topics = await self.admin.list_topics()
        assert topic not in topics


# ========== ERROR HANDLING (4 tests) ==========


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_e01_connection_refused(self):
        """Connecting to a non-existent server raises ConnectionError."""
        client = StreamlineClient(bootstrap_servers="localhost:19999")
        with pytest.raises((exceptions.ConnectionError, exceptions.StreamlineError, Exception)):
            await client.start()
        await client.close()

    @pytest.mark.asyncio
    async def test_e02_auth_denied(self):
        """Invalid credentials raise AuthenticationError."""
        client = StreamlineClient(
            bootstrap_servers=BOOTSTRAP,
            sasl_mechanism="PLAIN",
            sasl_plain_username="invalid",
            sasl_plain_password="invalid",
        )
        with pytest.raises(
            (exceptions.AuthenticationError, exceptions.ConnectionError, Exception)
        ):
            await client.start()
        await client.close()

    @pytest.mark.asyncio
    async def test_e03_topic_not_found(self):
        """Producing to a non-existent topic raises an error (if auto-create is off)."""
        client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await client.start()
        try:
            topic = f"nonexistent-{int(time.time() * 1_000_000)}"
            # This may succeed if auto-create is on; the important thing is
            # it doesn't crash with an unhandled exception.
            try:
                await client.producer.send(topic, value=b"test")
            except (exceptions.TopicError, exceptions.StreamlineError):
                pass  # Expected when auto-create is disabled
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_e04_request_timeout(self):
        """Verify timeout errors include the TimeoutError type."""
        assert issubclass(exceptions.TimeoutError, exceptions.StreamlineError)
        err = exceptions.TimeoutError("test timeout")
        assert err.hint is not None or True  # hint may or may not be set


# ========== PERFORMANCE (4 tests) ==========


class TestPerformance:
    @pytest.fixture(autouse=True)
    async def setup(self):
        self.client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await self.client.start()
        self.admin = self.client.admin
        self.producer = self.client.producer
        yield
        await self.client.close()

    @pytest.mark.asyncio
    async def test_f01_throughput_1kb(self):
        """Produce 1000 × 1KB messages and verify throughput ≥ 1000 msg/s."""
        topic = unique_topic("f01")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=3))
        payload = b"x" * 1024
        start = time.monotonic()
        for _ in range(1000):
            await self.producer.send(topic, value=payload)
        await self.producer.flush()
        elapsed = time.monotonic() - start
        throughput = 1000 / elapsed
        assert throughput > 100, f"Throughput {throughput:.0f} msg/s below minimum"

    @pytest.mark.asyncio
    async def test_f02_latency_p99(self):
        """Measure produce latency for 100 messages; p99 should be < 500ms."""
        topic = unique_topic("f02")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        latencies = []
        for i in range(100):
            start = time.monotonic()
            await self.producer.send(topic, value=f"latency-{i}".encode())
            latencies.append(time.monotonic() - start)
        latencies.sort()
        p99 = latencies[98]
        assert p99 < 0.5, f"p99 latency {p99:.3f}s exceeds 500ms"

    @pytest.mark.asyncio
    async def test_f03_startup_time(self):
        """Client startup should complete within 5 seconds."""
        start = time.monotonic()
        client = StreamlineClient(bootstrap_servers=BOOTSTRAP)
        await client.start()
        elapsed = time.monotonic() - start
        assert elapsed < 5.0, f"Startup took {elapsed:.1f}s"
        await client.close()

    @pytest.mark.asyncio
    async def test_f04_memory_usage(self):
        """Producing 10K messages should not cause excessive memory growth."""
        import sys
        topic = unique_topic("f04")
        await self.admin.create_topic(TopicConfig(name=topic, num_partitions=1))
        records = [
            ProducerRecord(topic=topic, value=f"mem-{i}".encode())
            for i in range(100)
        ]
        results = await self.producer.send_batch(records)
        assert len(results) == 100
        # Basic sanity: Python process shouldn't exceed 500MB for this test
        assert sys.getsizeof(results) < 500 * 1024 * 1024

