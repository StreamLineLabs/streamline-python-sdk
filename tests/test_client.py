"""Tests for StreamlineClient."""

from streamline_sdk import (
    ProducerRecord,
    StreamlineClient,
    TopicConfig,
)
from streamline_sdk.client import ClientConfig, ConsumerConfig, ProducerConfig


class TestClientConfig:
    """Tests for client configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ClientConfig()
        assert config.bootstrap_servers == "localhost:9092"
        assert config.client_id == "streamline-python-client"
        assert config.request_timeout_ms == 30000

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ClientConfig(
            bootstrap_servers="broker1:9092,broker2:9092",
            client_id="my-client",
            request_timeout_ms=60000,
        )
        assert config.bootstrap_servers == "broker1:9092,broker2:9092"
        assert config.client_id == "my-client"
        assert config.request_timeout_ms == 60000

    def test_sasl_config(self):
        """Test SASL configuration."""
        config = ClientConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="pass",
        )
        assert config.security_protocol == "SASL_PLAINTEXT"
        assert config.sasl_mechanism == "SCRAM-SHA-256"
        assert config.sasl_username == "user"


class TestProducerConfig:
    """Tests for producer configuration."""

    def test_default_producer_config(self):
        """Test default producer configuration."""
        config = ProducerConfig()
        assert config.acks == "all"
        assert config.compression_type == "none"
        assert config.batch_size == 16384
        assert config.enable_idempotence is False

    def test_custom_producer_config(self):
        """Test custom producer configuration."""
        config = ProducerConfig(
            acks=1,
            compression_type="gzip",
            enable_idempotence=True,
        )
        assert config.acks == 1
        assert config.compression_type == "gzip"
        assert config.enable_idempotence is True


class TestConsumerConfig:
    """Tests for consumer configuration."""

    def test_default_consumer_config(self):
        """Test default consumer configuration."""
        config = ConsumerConfig()
        assert config.group_id is None
        assert config.auto_offset_reset == "latest"
        assert config.enable_auto_commit is True

    def test_custom_consumer_config(self):
        """Test custom consumer configuration."""
        config = ConsumerConfig(
            group_id="my-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        assert config.group_id == "my-group"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False


class TestProducerRecord:
    """Tests for ProducerRecord."""

    def test_basic_record(self):
        """Test basic record creation."""
        record = ProducerRecord(topic="test-topic", value=b"test-value")
        assert record.topic == "test-topic"
        assert record.value == b"test-value"
        assert record.key is None
        assert record.partition == -1

    def test_full_record(self):
        """Test record with all fields."""
        record = ProducerRecord(
            topic="test-topic",
            value=b"test-value",
            key=b"test-key",
            partition=2,
            timestamp_ms=1234567890,
            headers={"header1": b"value1"},
        )
        assert record.topic == "test-topic"
        assert record.value == b"test-value"
        assert record.key == b"test-key"
        assert record.partition == 2
        assert record.timestamp_ms == 1234567890
        assert record.headers == {"header1": b"value1"}


class TestTopicConfig:
    """Tests for TopicConfig."""

    def test_basic_topic_config(self):
        """Test basic topic configuration."""
        config = TopicConfig(name="test-topic")
        assert config.name == "test-topic"
        assert config.num_partitions == 1
        assert config.replication_factor == 1
        assert config.config == {}

    def test_full_topic_config(self):
        """Test topic configuration with all fields."""
        config = TopicConfig(
            name="test-topic",
            num_partitions=6,
            replication_factor=3,
            config={"retention.ms": "86400000"},
        )
        assert config.name == "test-topic"
        assert config.num_partitions == 6
        assert config.replication_factor == 3
        assert config.config["retention.ms"] == "86400000"


class TestStreamlineClient:
    """Tests for StreamlineClient initialization."""

    def test_client_initialization(self):
        """Test client initialization."""
        client = StreamlineClient(bootstrap_servers="localhost:9092")
        assert client.bootstrap_servers == "localhost:9092"
        assert client.is_connected is False

    def test_client_with_configs(self):
        """Test client with custom configs."""
        client = StreamlineClient(
            bootstrap_servers="broker:9092",
            client_id="test-client",
            producer_config=ProducerConfig(acks=1),
            consumer_config=ConsumerConfig(group_id="test-group"),
        )
        assert client.bootstrap_servers == "broker:9092"
