"""
Integration tests for StreamlineContainer.
"""

import pytest
import requests
from streamline_testcontainers import StreamlineContainer


@pytest.fixture(scope="module")
def streamline():
    """Provide a running Streamline container for tests."""
    with StreamlineContainer().with_debug_logging() as container:
        yield container


class TestStreamlineContainer:
    """Tests for StreamlineContainer."""

    def test_container_starts(self, streamline):
        """Container should start and be running."""
        assert streamline.get_wrapped_container() is not None

    def test_bootstrap_servers(self, streamline):
        """Should return valid bootstrap servers string."""
        servers = streamline.get_bootstrap_servers()
        assert servers is not None
        assert ":" in servers
        print(f"Bootstrap servers: {servers}")

    def test_http_url(self, streamline):
        """Should return valid HTTP URL."""
        url = streamline.get_http_url()
        assert url.startswith("http://")
        print(f"HTTP URL: {url}")

    def test_health_check(self, streamline):
        """Health endpoint should return 200."""
        health_url = streamline.get_health_url()
        response = requests.get(health_url, timeout=5)
        assert response.status_code == 200

    def test_metrics_endpoint(self, streamline):
        """Metrics endpoint should return data."""
        metrics_url = streamline.get_metrics_url()
        response = requests.get(metrics_url, timeout=5)
        assert response.status_code == 200
        assert len(response.text) > 0


class TestKafkaIntegration:
    """Tests for Kafka client integration."""

    def test_produce_and_consume(self, streamline):
        """Should be able to produce and consume messages."""
        pytest.importorskip("kafka")
        from kafka import KafkaConsumer, KafkaProducer

        topic = "test-topic"
        message = b"Hello, Streamline!"

        # Produce
        producer = KafkaProducer(
            bootstrap_servers=streamline.get_bootstrap_servers(),
            api_version=(2, 0, 0),
        )
        future = producer.send(topic, message)
        future.get(timeout=10)
        producer.close()

        # Consume
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=streamline.get_bootstrap_servers(),
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            api_version=(2, 0, 0),
        )

        messages = list(consumer)
        consumer.close()

        assert len(messages) >= 1
        assert messages[0].value == message
