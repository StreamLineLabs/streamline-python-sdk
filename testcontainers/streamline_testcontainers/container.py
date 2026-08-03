"""
Streamline container implementation for Testcontainers.
"""

from __future__ import annotations

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class StreamlineContainer(DockerContainer):
    """
    Testcontainers container for Streamline - The Redis of Streaming.

    Streamline is a Kafka-compatible streaming platform that provides a lightweight,
    single-binary alternative to Apache Kafka.

    Example:
        >>> from streamline_testcontainers import StreamlineContainer
        >>> from kafka import KafkaProducer
        >>>
        >>> with StreamlineContainer() as streamline:
        ...     producer = KafkaProducer(
        ...         bootstrap_servers=streamline.get_bootstrap_servers()
        ...     )
        ...     producer.send("my-topic", b"Hello, Streamline!")
        ...     producer.flush()

    Attributes:
        KAFKA_PORT: The Kafka protocol port (9092)
        HTTP_PORT: The HTTP API port (9094)
    """

    KAFKA_PORT = 9092
    HTTP_PORT = 9094

    def __init__(
        self,
        image: str = "ghcr.io/streamlinelabs/streamline:latest",
        log_level: str = "info",
        **kwargs,
    ):
        """
        Initialize a Streamline container.

        Args:
            image: Docker image to use (default: streamline/streamline:latest)
            log_level: Log level - trace, debug, info, warn, error (default: info)
            **kwargs: Additional arguments passed to DockerContainer
        """
        super().__init__(image, **kwargs)

        self.with_exposed_ports(self.KAFKA_PORT, self.HTTP_PORT)
        self.with_env("STREAMLINE_LISTEN_ADDR", f"0.0.0.0:{self.KAFKA_PORT}")
        self.with_env("STREAMLINE_HTTP_ADDR", f"0.0.0.0:{self.HTTP_PORT}")
        self.with_env("STREAMLINE_LOG_LEVEL", log_level)

    def start(self) -> StreamlineContainer:
        """
        Start the container and wait for it to be ready.

        Returns:
            self for method chaining
        """
        super().start()
        self._wait_for_ready()
        return self

    def _wait_for_ready(self) -> None:
        """Wait for the container to be ready to accept connections."""
        wait_for_logs(self, "Server started", timeout=30)

    def get_bootstrap_servers(self) -> str:
        """
        Get the Kafka bootstrap servers connection string.

        Use this value for the `bootstrap_servers` parameter in Kafka clients.

        Returns:
            Bootstrap servers string in format "host:port"
        """
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.KAFKA_PORT)
        return f"{host}:{port}"

    def get_http_url(self) -> str:
        """
        Get the HTTP API base URL.

        Returns:
            HTTP API base URL
        """
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"

    def get_health_url(self) -> str:
        """
        Get the health check endpoint URL.

        Returns:
            Health endpoint URL
        """
        return f"{self.get_http_url()}/health"

    def get_metrics_url(self) -> str:
        """
        Get the Prometheus metrics endpoint URL.

        Returns:
            Metrics endpoint URL
        """
        return f"{self.get_http_url()}/metrics"

    def create_topic(self, name: str, partitions: int = 1) -> None:
        """
        Create a topic with the specified name and partitions.

        Note: Streamline supports auto-topic creation, so topics are created
        automatically when a producer first writes to them. Use this method
        if you need to pre-create topics with specific configurations.

        Args:
            name: Topic name
            partitions: Number of partitions (default: 1)

        Raises:
            RuntimeError: If topic creation fails
        """
        exit_code, output = self.exec(
            f"streamline-cli topics create {name} --partitions {partitions}"
        )
        if exit_code != 0:
            raise RuntimeError(f"Failed to create topic '{name}': {output}")

    def with_debug_logging(self) -> StreamlineContainer:
        """
        Enable debug logging.

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_LOG_LEVEL", "debug")
        return self

    def with_trace_logging(self) -> StreamlineContainer:
        """
        Enable trace logging.

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_LOG_LEVEL", "trace")
        return self

    def with_in_memory(self) -> StreamlineContainer:
        """
        Enable in-memory storage mode (no disk persistence).

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_IN_MEMORY", "true")
        return self

    def with_playground(self) -> StreamlineContainer:
        """
        Enable playground mode (pre-loaded demo topics).

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_PLAYGROUND", "true")
        return self

    def create_topics(self, topics: dict[str, int]) -> None:
        """
        Create multiple topics at once.

        Args:
            topics: Dictionary of topic name to partition count

        Raises:
            RuntimeError: If any topic creation fails
        """
        for name, partitions in topics.items():
            self.create_topic(name, partitions)

    def produce_message(self, topic: str, value: str, key: str | None = None) -> None:
        """
        Produce a single message to a topic.

        Args:
            topic: Topic name
            value: Message value
            key: Optional message key

        Raises:
            RuntimeError: If message production fails
        """
        cmd = f'streamline-cli produce {topic} -m "{value}"'
        if key:
            cmd += f' -k "{key}"'
        exit_code, output = self.exec(cmd)
        if exit_code != 0:
            raise RuntimeError(f"Failed to produce message: {output}")

    def get_info_url(self) -> str:
        """
        Get the server info endpoint URL.

        Returns:
            Info endpoint URL
        """
        return f"{self.get_http_url()}/info"

    def wait_for_topics(self, topics: list[str], timeout: float = 10.0) -> None:
        """
        Wait until all specified topics exist.

        Args:
            topics: List of topic names to wait for
            timeout: Maximum time to wait in seconds (default: 10)

        Raises:
            TimeoutError: If topics don't appear within the timeout
        """
        import time
        start = time.time()
        while time.time() - start < timeout:
            all_exist = True
            for topic in topics:
                exit_code, _ = self.exec(
                    f"streamline-cli topics describe {topic}"
                )
                if exit_code != 0:
                    all_exist = False
                    break
            if all_exist:
                return
            time.sleep(0.2)
        raise TimeoutError(f"Topics {topics} not available within {timeout}s")

    def assert_topic_exists(self, topic: str) -> None:
        """
        Assert that a topic exists.

        Args:
            topic: Topic name to verify

        Raises:
            AssertionError: If the topic does not exist
        """
        exit_code, _ = self.exec(
            f"streamline-cli topics describe {topic}"
        )
        assert exit_code == 0, f"Topic '{topic}' does not exist"

    def assert_healthy(self) -> None:
        """
        Assert that the container is healthy via the health endpoint.

        Raises:
            AssertionError: If the health check fails
        """
        import urllib.request
        try:
            resp = urllib.request.urlopen(self.get_health_url(), timeout=5)
            assert resp.status == 200, f"Health check returned status {resp.status}"
        except Exception as e:
            raise AssertionError(f"Health check failed: {e}")

    # -------------------------------------------------------------------------
    # Enhanced capabilities: batch produce, consumer groups, migration helpers
    # -------------------------------------------------------------------------

    def produce_messages(self, topic: str, messages: list[str]) -> None:
        """
        Produce a batch of messages to a topic.

        Args:
            topic: Topic name
            messages: List of message values
        """
        for msg in messages:
            self.produce_message(topic, msg)

    def produce_keyed_messages(self, topic: str, messages: dict[str, str]) -> None:
        """
        Produce a batch of keyed messages to a topic.

        Args:
            topic: Topic name
            messages: Dictionary of key to value
        """
        for key, value in messages.items():
            self.produce_message(topic, value, key=key)

    def list_consumer_groups(self) -> list[str]:
        """
        List consumer groups.

        Returns:
            List of consumer group IDs
        """
        exit_code, output = self.exec(
            "streamline-cli groups list --format json"
        )
        if exit_code != 0:
            raise RuntimeError(f"Failed to list consumer groups: {output}")
        groups = []
        for line in output.strip().split("\n"):
            cleaned = line.strip().strip('[],"')
            if cleaned:
                groups.append(cleaned)
        return groups

    def assert_consumer_group_exists(self, group_id: str) -> None:
        """
        Assert that a consumer group exists.

        Args:
            group_id: Consumer group ID

        Raises:
            AssertionError: If the group does not exist
        """
        exit_code, _ = self.exec(
            f"streamline-cli groups describe {group_id}"
        )
        assert exit_code == 0, f"Consumer group '{group_id}' does not exist"

    def get_partition_count(self, topic: str) -> int:
        """
        Get the partition count for a topic.

        Args:
            topic: Topic name

        Returns:
            Number of partitions
        """
        exit_code, output = self.exec(
            f"streamline-cli topics describe {topic} --format json"
        )
        if exit_code != 0:
            raise RuntimeError(f"Topic '{topic}' not found")
        import json
        try:
            data = json.loads(output)
            return data.get("partitions", 1) if isinstance(data, dict) else 1
        except json.JSONDecodeError:
            return 1

    def assert_partition_count(self, topic: str, expected: int) -> None:
        """
        Assert that a topic has the expected partition count.

        Args:
            topic: Topic name
            expected: Expected number of partitions

        Raises:
            AssertionError: If partition count doesn't match
        """
        actual = self.get_partition_count(topic)
        assert actual == expected, (
            f"Expected {expected} partitions for topic '{topic}', got {actual}"
        )

    def get_cluster_info(self) -> dict:
        """
        Get cluster information from the HTTP API.

        Returns:
            Cluster info as a dictionary
        """
        import json
        import urllib.request
        resp = urllib.request.urlopen(self.get_info_url(), timeout=5)
        return json.loads(resp.read().decode())

    def with_authentication(self, username: str, password: str) -> StreamlineContainer:
        """
        Enable SASL/PLAIN authentication.

        Args:
            username: Admin username
            password: Admin password

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_AUTH_ENABLED", "true")
        self.with_env("STREAMLINE_AUTH_DEFAULT_USER", username)
        self.with_env("STREAMLINE_AUTH_DEFAULT_PASSWORD", password)
        return self

    def with_auto_create_topics(
        self, default_partitions: int = 1
    ) -> StreamlineContainer:
        """
        Enable auto-topic creation with a default partition count.

        Args:
            default_partitions: Default partition count for auto-created topics

        Returns:
            self for method chaining
        """
        self.with_env("STREAMLINE_AUTO_CREATE_TOPICS", "true")
        self.with_env("STREAMLINE_DEFAULT_PARTITIONS", str(default_partitions))
        return self

    @classmethod
    def as_kafka_replacement(cls) -> StreamlineContainer:
        """
        Create a Streamline container configured as a drop-in Kafka replacement.

        Useful for migrating from Kafka-based tests. Simply replace::

            # Before (Kafka):
            container = KafkaContainer("confluentinc/cp-kafka:7.4.0")

            # After (Streamline):
            container = StreamlineContainer.as_kafka_replacement()

        Returns:
            A new StreamlineContainer configured for Kafka compatibility
        """
        return cls().with_in_memory().with_auto_create_topics(1)

    @classmethod
    def with_pre_configured_topics(
        cls, topics: dict[str, int]
    ) -> StreamlineContainer:
        """
        Create a container pre-configured with topics.

        Args:
            topics: Dictionary of topic name to partition count

        Returns:
            A new StreamlineContainer with pre-configured topics
        """
        container = cls().with_in_memory()
        for name, partitions in topics.items():
            container.with_env(f"STREAMLINE_AUTO_TOPIC_{name}", str(partitions))
        return container

    def with_ephemeral(self) -> StreamlineContainer:
        """
        Enable ephemeral mode: in-memory, auto-cleanup, fastest startup.

        The server will auto-shutdown after the idle timeout if no clients
        are connected.

        Returns:
            This container instance for method chaining
        """
        self.with_env("STREAMLINE_EPHEMERAL", "true")
        self.with_env("STREAMLINE_IN_MEMORY", "true")
        return self

    def with_ephemeral_idle_timeout(self, seconds: int) -> StreamlineContainer:
        """
        Set the idle timeout before ephemeral server auto-shuts down.

        Args:
            seconds: Seconds to wait with zero connections before shutdown

        Returns:
            This container instance for method chaining
        """
        self.with_env("STREAMLINE_EPHEMERAL_IDLE_TIMEOUT", str(seconds))
        return self

    def with_ephemeral_auto_topics(self, topic_specs: str) -> StreamlineContainer:
        """
        Auto-create topics on startup in ephemeral mode.

        Args:
            topic_specs: Comma-separated "name:partitions" specs
                         e.g., "orders:3,events:6,logs:1"

        Returns:
            This container instance for method chaining
        """
        self.with_env("STREAMLINE_EPHEMERAL_AUTO_TOPICS", topic_specs)
        return self

    @classmethod
    def for_testing(cls) -> StreamlineContainer:
        """
        Create a container optimized for CI/CD testing.

        Ephemeral mode, in-memory, auto-create topics, minimal logging.

        Example::

            @pytest.fixture(scope="session")
            def streamline():
                with StreamlineContainer.for_testing() as container:
                    yield container

        Returns:
            A new StreamlineContainer optimized for testing
        """
        return (
            cls()
            .with_ephemeral()
            .with_auto_create_topics(3)
            .with_log_level("warn")
        )
