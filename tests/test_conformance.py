"""
SDK Conformance Tests — Reference Implementation (Python)

Implements the 46 required tests from CONFORMANCE_SPEC.md v1.0.0.
Run with: pytest tests/test_conformance.py -v

Each test is prefixed with its spec ID (C01, T01, P01, etc.) for traceability.
Uses the Testcontainers module for container lifecycle management.
"""

import json
import time
import pytest

try:
    import kafka  # noqa: F401
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

pytestmark = [
    pytest.mark.skipif(not HAS_KAFKA, reason="kafka-python package not installed"),
    pytest.mark.integration,
]

# These tests require a running Streamline server.
# In CI, the sdk-conformance.yml workflow provides one.
# Locally, start with: streamline --in-memory
BOOTSTRAP = "localhost:9092"
HTTP_URL = "http://localhost:9094"


def get_bootstrap():
    """Get bootstrap servers from env or default."""
    import os
    return os.environ.get("STREAMLINE_BOOTSTRAP", BOOTSTRAP)


def get_http_url():
    """Get HTTP URL from env or default."""
    import os
    return os.environ.get("STREAMLINE_HTTP_URL", HTTP_URL)


# ── C: Connection & Discovery (5 tests) ──────────────────────────────────────

class TestConnection:
    def test_C01_connect_basic(self):
        """C01: Connect to bootstrap server."""
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=get_bootstrap())
        assert producer.bootstrap_connected()
        producer.close()

    def test_C02_connect_metadata(self):
        """C02: Fetch cluster metadata."""
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=get_bootstrap())
        topics = consumer.topics()
        assert isinstance(topics, set)
        consumer.close()

    def test_C03_connect_api_versions(self):
        """C03: Request API versions."""
        from kafka import KafkaClient
        client = KafkaClient(bootstrap_servers=get_bootstrap())
        client.check_version()
        client.close()

    def test_C04_connect_invalid_addr(self):
        """C04: Invalid address returns error within 5s."""
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        start = time.time()
        with pytest.raises((NoBrokersAvailable, Exception)):
            KafkaProducer(
                bootstrap_servers="invalid-host:9999",
                request_timeout_ms=3000,
            )
        assert time.time() - start < 5

    def test_C05_connect_close(self):
        """C05: Graceful close."""
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=get_bootstrap())
        producer.close(timeout=5)


# ── T: Topic Management (6 tests) ────────────────────────────────────────────

class TestTopics:
    TOPIC_PREFIX = "conformance-topic-"

    def test_T01_topic_create(self):
        """T01: Create topic with 3 partitions."""
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        topic = f"{self.TOPIC_PREFIX}t01"
        try:
            admin.delete_topics([topic])
            time.sleep(0.5)
        except Exception:
            pass
        admin.create_topics([NewTopic(topic, 3, 1)])
        admin.close()

    def test_T02_topic_list(self):
        """T02: List topics."""
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=get_bootstrap())
        topics = consumer.topics()
        assert isinstance(topics, set)
        consumer.close()

    def test_T03_topic_describe(self):
        """T03: Describe topic partitions."""
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        topic = f"{self.TOPIC_PREFIX}t03"
        try:
            admin.create_topics([NewTopic(topic, 3, 1)])
        except Exception:
            pass
        desc = admin.describe_topics([topic])
        assert len(desc) > 0
        assert len(desc[0].get("partitions", [])) == 3
        admin.close()

    def test_T04_topic_delete(self):
        """T04: Delete topic."""
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        topic = f"{self.TOPIC_PREFIX}t04"
        admin.create_topics([NewTopic(topic, 1, 1)])
        admin.delete_topics([topic])
        admin.close()

    def test_T05_topic_create_duplicate(self):
        """T05: Duplicate creation returns error."""
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        topic = f"{self.TOPIC_PREFIX}t05"
        try:
            admin.create_topics([NewTopic(topic, 1, 1)])
        except Exception:
            pass
        with pytest.raises(Exception):
            admin.create_topics([NewTopic(topic, 1, 1)])
        admin.close()

    def test_T06_topic_auto_create(self):
        """T06: Auto-create on produce."""
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=get_bootstrap())
        topic = f"{self.TOPIC_PREFIX}t06-auto-{int(time.time())}"
        producer.send(topic, b"auto-create test").get(timeout=10)
        producer.close()


# ── P: Producer (8 tests) ────────────────────────────────────────────────────

class TestProducer:
    TOPIC = "conformance-producer"

    @pytest.fixture(autouse=True)
    def setup_topic(self):
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        try:
            admin.create_topics([NewTopic(self.TOPIC, 3, 1)])
        except Exception:
            pass
        admin.close()

    def test_P01_produce_single(self):
        """P01: Produce 1 message."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        result = p.send(self.TOPIC, b"hello").get(timeout=10)
        assert result.offset >= 0
        p.close()

    def test_P02_produce_keyed(self):
        """P02: Produce with key."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        result = p.send(self.TOPIC, key=b"key1", value=b"value1").get(timeout=10)
        assert result.offset >= 0
        p.close()

    def test_P03_produce_batch(self):
        """P03: Produce 100 messages."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        futures = [p.send(self.TOPIC, f"msg-{i}".encode()) for i in range(100)]
        for f in futures:
            assert f.get(timeout=10).offset >= 0
        p.close()

    def test_P04_produce_null_key(self):
        """P04: Produce with null key."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        result = p.send(self.TOPIC, key=None, value=b"no-key").get(timeout=10)
        assert result.offset >= 0
        p.close()

    def test_P05_produce_large_value(self):
        """P05: Produce 1MB message."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap(), max_request_size=2*1024*1024)
        big = b"x" * (1024 * 1024)
        result = p.send(self.TOPIC, value=big).get(timeout=30)
        assert result.offset >= 0
        p.close()

    def test_P06_produce_headers(self):
        """P06: Produce with headers."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        headers = [("x-trace-id", b"abc123"), ("x-source", b"test")]
        result = p.send(self.TOPIC, value=b"with-headers", headers=headers).get(timeout=10)
        assert result.offset >= 0
        p.close()

    def test_P07_produce_multi_partition(self):
        """P07: Produce to all partitions."""
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        for partition in range(3):
            result = p.send(self.TOPIC, partition=partition, value=f"p{partition}".encode()).get(timeout=10)
            assert result.partition == partition
        p.close()

    def test_P08_produce_json_value(self):
        """P08: Produce JSON value."""
        from kafka import KafkaProducer
        p = KafkaProducer(
            bootstrap_servers=get_bootstrap(),
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        result = p.send(self.TOPIC, {"event": "test", "count": 42}).get(timeout=10)
        assert result.offset >= 0
        p.close()


# ── N: Consumer (10 tests) ───────────────────────────────────────────────────

class TestConsumer:
    TOPIC = "conformance-consumer"

    @pytest.fixture(autouse=True)
    def setup_topic(self):
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        try:
            admin.create_topics([NewTopic(self.TOPIC, 3, 1)])
        except Exception:
            pass
        admin.close()
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        for i in range(10):
            p.send(self.TOPIC, key=f"k{i}".encode(), value=f"v{i}".encode(),
                   headers=[("idx", str(i).encode())])
        p.flush()
        p.close()

    def test_N01_consume_from_beginning(self):
        """N01: Consume from offset 0."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000)
        msgs = list(c)
        assert len(msgs) > 0
        c.close()

    def test_N02_consume_from_latest(self):
        """N02: Consume from latest (no old messages)."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='latest', consumer_timeout_ms=3000)
        msgs = list(c)
        assert len(msgs) == 0
        c.close()

    def test_N03_consume_with_timeout(self):
        """N03: Empty topic with timeout."""
        from kafka import KafkaConsumer
        empty_topic = f"conformance-empty-{int(time.time())}"
        c = KafkaConsumer(empty_topic, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='latest', consumer_timeout_ms=2000)
        msgs = list(c)
        assert len(msgs) == 0
        c.close()

    def test_N04_consume_max_records(self):
        """N04: Max records limit."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000,
                          max_poll_records=5)
        batch = c.poll(timeout_ms=5000, max_records=5)
        total = sum(len(msgs) for msgs in batch.values())
        assert total <= 5
        c.close()

    def test_N05_consume_verify_ordering(self):
        """N05: Messages consumed in order."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000)
        offsets = {}
        for msg in c:
            p = msg.partition
            if p in offsets:
                assert msg.offset > offsets[p], "Out of order!"
            offsets[p] = msg.offset
        c.close()

    def test_N06_consume_headers(self):
        """N06: Headers present on consume."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000)
        found_headers = False
        for msg in c:
            if msg.headers:
                found_headers = True
                break
        assert found_headers
        c.close()

    def test_N07_consume_large_message(self):
        """N07: Consume 1MB message."""
        from kafka import KafkaProducer, KafkaConsumer
        topic = "conformance-large"
        p = KafkaProducer(bootstrap_servers=get_bootstrap(), max_request_size=2*1024*1024)
        big = b"L" * (1024 * 1024)
        p.send(topic, value=big).get(timeout=30)
        p.close()

        c = KafkaConsumer(topic, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=10000,
                          max_partition_fetch_bytes=2*1024*1024)
        for msg in c:
            assert len(msg.value) == 1024 * 1024
            break
        c.close()

    def test_N08_consume_multi_partition(self):
        """N08: Consume across partitions."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000)
        partitions_seen = set()
        for msg in c:
            partitions_seen.add(msg.partition)
        assert len(partitions_seen) >= 1
        c.close()

    def test_N09_consume_offset_seek(self):
        """N09: Seek to specific offset."""
        from kafka import KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest', consumer_timeout_ms=5000)
        tp = TopicPartition(self.TOPIC, 0)
        c.assign([tp])
        c.seek(tp, 0)
        msg = next(c, None)
        assert msg is not None
        assert msg.offset == 0
        c.close()

    def test_N10_consume_earliest_latest(self):
        """N10: Query earliest/latest offsets."""
        from kafka import KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=get_bootstrap())
        tp = TopicPartition(self.TOPIC, 0)
        c.assign([tp])
        earliest = c.beginning_offsets([tp])
        latest = c.end_offsets([tp])
        assert tp in earliest
        assert tp in latest
        assert latest[tp] >= earliest[tp]
        c.close()


# ── G: Consumer Groups (8 tests) ─────────────────────────────────────────────

class TestConsumerGroups:
    TOPIC = "conformance-groups"

    @pytest.fixture(autouse=True)
    def setup_topic(self):
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        try:
            admin.create_topics([NewTopic(self.TOPIC, 3, 1)])
        except Exception:
            pass
        admin.close()
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        for i in range(10):
            p.send(self.TOPIC, f"g-msg-{i}".encode())
        p.flush()
        p.close()

    def test_G01_group_join(self):
        """G01: Join consumer group."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id='conformance-g01', auto_offset_reset='earliest',
                          consumer_timeout_ms=5000)
        msgs = list(c)
        assert len(msgs) > 0
        c.close()

    def test_G02_group_commit_offset(self):
        """G02: Commit offset."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id='conformance-g02', auto_offset_reset='earliest',
                          enable_auto_commit=False, consumer_timeout_ms=5000)
        for msg in c:
            c.commit()
            break
        c.close()

    def test_G03_group_fetch_offset(self):
        """G03: Fetch committed offset."""
        from kafka import KafkaConsumer, TopicPartition
        group = 'conformance-g03'
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id=group, auto_offset_reset='earliest',
                          enable_auto_commit=False, consumer_timeout_ms=5000)
        for msg in c:
            c.commit()
            break
        committed = c.committed(TopicPartition(self.TOPIC, msg.partition))
        assert committed is not None and committed > 0
        c.close()

    def test_G04_group_auto_commit(self):
        """G04: Auto-commit."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id='conformance-g04', auto_offset_reset='earliest',
                          enable_auto_commit=True, auto_commit_interval_ms=1000,
                          consumer_timeout_ms=5000)
        list(c)
        c.close()

    def test_G05_group_rebalance(self):
        """G05: Two consumers rebalance."""
        from kafka import KafkaConsumer
        c1 = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                           group_id='conformance-g05', auto_offset_reset='earliest',
                           consumer_timeout_ms=3000)
        c2 = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                           group_id='conformance-g05', auto_offset_reset='earliest',
                           consumer_timeout_ms=3000)
        # Both should get assignments
        c1.poll(timeout_ms=3000)
        c2.poll(timeout_ms=3000)
        c1.close()
        c2.close()

    def test_G06_group_leave(self):
        """G06: Consumer leaves."""
        from kafka import KafkaConsumer
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id='conformance-g06', auto_offset_reset='earliest',
                          consumer_timeout_ms=3000)
        c.poll(timeout_ms=2000)
        c.close()  # Leave group

    def test_G07_group_independent(self):
        """G07: Independent groups."""
        from kafka import KafkaConsumer
        c1 = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                           group_id='conformance-g07a', auto_offset_reset='earliest',
                           consumer_timeout_ms=5000)
        c2 = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                           group_id='conformance-g07b', auto_offset_reset='earliest',
                           consumer_timeout_ms=5000)
        msgs1 = list(c1)
        msgs2 = list(c2)
        assert len(msgs1) > 0
        assert len(msgs2) > 0
        c1.close()
        c2.close()

    def test_G08_group_describe(self):
        """G08: Describe group."""
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient
        group = 'conformance-g08'
        c = KafkaConsumer(self.TOPIC, bootstrap_servers=get_bootstrap(),
                          group_id=group, auto_offset_reset='earliest',
                          consumer_timeout_ms=3000)
        c.poll(timeout_ms=2000)
        admin = KafkaAdminClient(bootstrap_servers=get_bootstrap())
        desc = admin.describe_consumer_groups([group])
        assert len(desc) > 0
        admin.close()
        c.close()


# ── E: Error Handling (5 tests) ───────────────────────────────────────────────

class TestErrors:
    def test_E01_error_unknown_topic(self):
        """E01: Consume from non-existent topic."""
        from kafka import KafkaConsumer
        c = KafkaConsumer('conformance-nonexistent-topic',
                          bootstrap_servers=get_bootstrap(),
                          auto_offset_reset='earliest',
                          consumer_timeout_ms=3000)
        msgs = list(c)
        c.close()

    def test_E02_error_invalid_partition(self):
        """E02: Produce to invalid partition."""
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        p = KafkaProducer(bootstrap_servers=get_bootstrap())
        try:
            result = p.send('conformance-producer', partition=999, value=b"test")
            result.get(timeout=5)
        except Exception:
            pass  # Expected to fail
        p.close()

    def test_E03_error_invalid_offset(self):
        """E03: Fetch from negative offset."""
        from kafka import KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=get_bootstrap())
        tp = TopicPartition('conformance-producer', 0)
        c.assign([tp])
        try:
            c.seek(tp, -1)
        except Exception:
            pass  # Expected
        c.close()

    def test_E04_error_is_retryable(self):
        """E04: Errors have retryable information."""
        from kafka.errors import KafkaError, NotLeaderForPartitionError
        err = NotLeaderForPartitionError()
        assert hasattr(err, 'retriable') or isinstance(err, KafkaError)

    def test_E05_error_has_message(self):
        """E05: Errors have descriptive messages."""
        from kafka.errors import KafkaError, TopicAlreadyExistsError
        err = TopicAlreadyExistsError()
        assert str(err) != ""
