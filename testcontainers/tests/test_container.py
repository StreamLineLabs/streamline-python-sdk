"""
Integration tests for StreamlineContainer.
"""

import os

import pytest
import requests
from streamline_testcontainers import StreamlineContainer

#: An explicit, digest-pinned image reference the operator has verified is
#: reachable and runs a working Streamline server. There is intentionally
#: no built-in default (see ``StreamlineContainer.__init__``): the
#: container-starting tests below are skipped, with a clear reason, unless
#: this is set — they must never silently pass against a fabricated or
#: unreachable image reference.
TESTCONTAINERS_IMAGE_ENV_VAR = "STREAMLINE_TESTCONTAINERS_IMAGE"
_LIVE_IMAGE = os.environ.get(TESTCONTAINERS_IMAGE_ENV_VAR)
_VALID_DIGEST_IMAGE = "ghcr.io/streamlinelabs/streamline@sha256:" + "a" * 64


class TestStreamlineContainerConfiguration:
    """Self-contained tests for container configuration helpers."""

    def test_with_log_level_validates_and_sets_environment(self, monkeypatch):
        container = object.__new__(StreamlineContainer)
        calls = []
        monkeypatch.setattr(
            container,
            "with_env",
            lambda name, value: calls.append((name, value)),
        )

        assert container.with_log_level("WARN") is container
        assert calls == [("STREAMLINE_LOG_LEVEL", "warn")]

        with pytest.raises(ValueError, match="log_level"):
            container.with_log_level("verbose")

    def test_create_topic_quotes_cli_argument(self, monkeypatch):
        container = object.__new__(StreamlineContainer)
        commands = []
        monkeypatch.setattr(
            container,
            "exec",
            lambda command: commands.append(command) or (0, ""),
        )

        container.create_topic("orders; echo unsafe", partitions=3)
        assert commands == [
            "streamline-cli topics create 'orders; echo unsafe' --partitions 3"
        ]

        with pytest.raises(ValueError, match="greater than zero"):
            container.create_topic("orders", partitions=0)


class TestStreamlineContainerRequiresDigestPinnedImage:
    """Regression tests: no image ever silently "just works" by default."""

    def test_image_argument_is_required(self):
        with pytest.raises(TypeError):
            StreamlineContainer()  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "image",
        [
            "ghcr.io/streamlinelabs/streamline:0.3.0",
            "ghcr.io/streamlinelabs/streamline:latest",
            "ghcr.io/streamlinelabs/streamline",
        ],
    )
    def test_mutable_tag_is_rejected(self, image):
        with pytest.raises(ValueError, match="pinned by digest"):
            StreamlineContainer(image)

    def test_empty_image_is_rejected(self):
        with pytest.raises(ValueError, match="non-empty"):
            StreamlineContainer("")

    @pytest.mark.parametrize(
        "image",
        [
            "ghcr.io/streamlinelabs/streamline@sha256:deadbeef",  # too short
            "ghcr.io/streamlinelabs/streamline@sha256:" + "z" * 64,  # non-hex
        ],
    )
    def test_malformed_digest_is_rejected(self, image):
        with pytest.raises(ValueError, match="malformed sha256 digest"):
            StreamlineContainer(image)

    def test_digest_pinned_image_is_accepted_at_construction_time(self):
        # Construction alone must not touch Docker; it only validates and
        # records configuration, so this must succeed without a daemon.
        container = StreamlineContainer(_VALID_DIGEST_IMAGE)
        assert container is not None

    def test_classmethod_factories_require_an_explicit_image(self):
        with pytest.raises(TypeError):
            StreamlineContainer.as_kafka_replacement()  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            StreamlineContainer.for_testing()  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            StreamlineContainer.with_pre_configured_topics(  # type: ignore[call-arg]
                topics={"orders": 3}
            )

    def test_classmethod_factories_accept_and_validate_image(self):
        with pytest.raises(ValueError, match="pinned by digest"):
            StreamlineContainer.as_kafka_replacement(
                "ghcr.io/streamlinelabs/streamline:0.3.0"
            )
        container = StreamlineContainer.as_kafka_replacement(_VALID_DIGEST_IMAGE)
        assert container is not None


@pytest.fixture(scope="module")
def streamline():
    """Provide a running Streamline container for tests.

    Skipped unless ``STREAMLINE_TESTCONTAINERS_IMAGE`` names an explicit,
    digest-pinned image the operator has verified works, since there is no
    built-in default image this SDK can trust to exist and run correctly.
    """
    if not _LIVE_IMAGE:
        pytest.skip(
            f"set {TESTCONTAINERS_IMAGE_ENV_VAR} to a digest-pinned "
            "Streamline image (e.g. 'ghcr.io/streamlinelabs/streamline"
            "@sha256:...') to run container-starting tests"
        )
    with StreamlineContainer(_LIVE_IMAGE).with_debug_logging() as container:
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
