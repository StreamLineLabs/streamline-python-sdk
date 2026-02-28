"""Main client for Streamline SDK."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Optional,Any

from .producer import Producer
from .consumer import Consumer
from .admin import Admin
from .exceptions import ConnectionError


@dataclass
class ClientConfig:
    """Configuration for StreamlineClient.

    Attributes:
        bootstrap_servers: Comma-separated list of broker addresses.
        client_id: Client identifier for the connection.
        request_timeout_ms: Request timeout in milliseconds.
        metadata_max_age_ms: How often to refresh metadata in milliseconds.
        security_protocol: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
        sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
        sasl_username: SASL username.
        sasl_password: SASL password.
        ssl_cafile: Path to CA certificate.
        ssl_certfile: Path to client certificate.
        ssl_keyfile: Path to client key.
    """

    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    client_id: str = "streamline-python-client"
    request_timeout_ms: int = 30000
    metadata_max_age_ms: int = 300000
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None


@dataclass
class ProducerConfig:
    """Producer-specific configuration.

    Attributes:
        acks: Number of acknowledgments (0, 1, or 'all').
        compression_type: Compression codec (none, gzip, snappy, lz4, zstd).
        batch_size: Maximum batch size in bytes.
        linger_ms: Time to wait for batch to fill in milliseconds.
        max_request_size: Maximum request size in bytes.
        enable_idempotence: Enable exactly-once semantics.
        retries: Number of retries on failure.
    """

    acks: str | int = "all"
    compression_type: str = "none"
    batch_size: int = 16384
    linger_ms: int = 0
    max_request_size: int = 1048576
    enable_idempotence: bool = False
    retries: int = 3


@dataclass
class ConsumerConfig:
    """Consumer-specific configuration.

    Attributes:
        group_id: Consumer group identifier.
        auto_offset_reset: Where to start consuming (earliest, latest).
        enable_auto_commit: Enable automatic offset commits.
        auto_commit_interval_ms: Auto-commit interval in milliseconds.
        max_poll_records: Maximum records per poll.
        session_timeout_ms: Session timeout in milliseconds.
        heartbeat_interval_ms: Heartbeat interval in milliseconds.
        isolation_level: Transaction isolation (read_uncommitted, read_committed).
    """

    group_id: Optional[str] = None
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    isolation_level: str = "read_uncommitted"


class StreamlineClient:
    """Main client for interacting with Streamline.

    This client provides access to producer, consumer, and admin functionality.
    It should be used as an async context manager.

    Example:
        async with StreamlineClient(bootstrap_servers="localhost:9092") as client:
            await client.producer.send("topic", value=b"message")
    """

    def __init__(
        self,
        bootstrap_servers: str = os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092"),
        *,
        client_id: str = "streamline-python-client",
        client_config: Optional[ClientConfig] = None,
        producer_config: Optional[ProducerConfig] = None,
        consumer_config: Optional[ConsumerConfig] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the client.

        Args:
            bootstrap_servers: Comma-separated list of broker addresses.
            client_id: Client identifier.
            client_config: Full client configuration (overrides other args).
            producer_config: Producer configuration.
            consumer_config: Consumer configuration.
            **kwargs: Additional arguments passed to ClientConfig.
        """
        if client_config is not None:
            self._config = client_config
        else:
            self._config = ClientConfig(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                **kwargs,
            )

        self._producer_config = producer_config or ProducerConfig()
        self._consumer_config = consumer_config or ConsumerConfig()

        self._producer: Optional[Producer] = None
        self._admin: Optional[Admin] = None
        self._started = False
        self._closed = False

    @property
    def producer(self) -> Producer:
        """Get the producer instance.

        Returns:
            Producer instance for sending messages.

        Raises:
            ConnectionError: If client is not started.
        """
        if self._producer is None:
            raise ConnectionError("Client not started. Use 'async with' context manager.")
        return self._producer

    @property
    def admin(self) -> Admin:
        """Get the admin client instance.

        Returns:
            Admin instance for administrative operations.

        Raises:
            ConnectionError: If client is not started.
        """
        if self._admin is None:
            raise ConnectionError("Client not started. Use 'async with' context manager.")
        return self._admin

    def consumer(
        self,
        group_id: Optional[str] = None,
        **kwargs:Any,
    ) -> Consumer:
        """Create a new consumer.

        Args:
            group_id: Consumer group ID (overrides config).
            **kwargs: Additional consumer configuration.

        Returns:
            Consumer instance.
        """
        config = ConsumerConfig(
            group_id=group_id or self._consumer_config.group_id,
            auto_offset_reset=kwargs.get(
                "auto_offset_reset", self._consumer_config.auto_offset_reset
            ),
            enable_auto_commit=kwargs.get(
                "enable_auto_commit", self._consumer_config.enable_auto_commit
            ),
            auto_commit_interval_ms=kwargs.get(
                "auto_commit_interval_ms", self._consumer_config.auto_commit_interval_ms
            ),
            max_poll_records=kwargs.get(
                "max_poll_records", self._consumer_config.max_poll_records
            ),
            session_timeout_ms=kwargs.get(
                "session_timeout_ms", self._consumer_config.session_timeout_ms
            ),
            heartbeat_interval_ms=kwargs.get(
                "heartbeat_interval_ms", self._consumer_config.heartbeat_interval_ms
            ),
            isolation_level=kwargs.get(
                "isolation_level", self._consumer_config.isolation_level
            ),
        )
        return Consumer(self._config, config)

    async def start(self) -> None:
        """Start the client and establish connections.

        This is called automatically when using the async context manager.
        """
        if self._started:
            return

        try:
            self._producer = Producer(self._config, self._producer_config)
            await self._producer.start()

            self._admin = Admin(self._config)
            await self._admin.start()

            self._started = True
        except Exception as e:
            await self.close()
            raise ConnectionError(f"Failed to start client: {e}") from e

    async def close(self) -> None:
        """Close the client and release resources.

        This is called automatically when using the async context manager.
        """
        if self._closed:
            return

        self._closed = True

        if self._producer is not None:
            await self._producer.close()
            self._producer = None

        if self._admin is not None:
            await self._admin.close()
            self._admin = None

        self._started = False

    async def __aenter__(self) -> "StreamlineClient":
        """Enter the async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Exit the async context manager."""
        await self.close()

    @property
    def bootstrap_servers(self) -> str:
        """Get the bootstrap servers."""
        return self._config.bootstrap_servers

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._started and not self._closed
