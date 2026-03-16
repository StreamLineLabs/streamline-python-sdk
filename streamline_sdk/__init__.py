"""
Streamline Python SDK - Official Python client for Streamline.

Example usage:

    from streamline_sdk import StreamlineClient

    async def main():
        async with StreamlineClient(bootstrap_servers="localhost:9092") as client:
            # Produce a message
            await client.producer.send("my-topic", value=b"Hello, World!")

            # Consume messages
            async for message in client.consumer.subscribe("my-topic"):
                print(f"Received: {message.value}")

    asyncio.run(main())
"""

from .client import StreamlineClient
from .producer import Producer, ProducerRecord, RecordMetadata
from .consumer import Consumer, ConsumerRecord
from .admin import Admin, TopicConfig, TopicInfo, PartitionInfo
from .admin import ClusterInfo, BrokerInfo, ConsumerLag, ConsumerGroupLag, InspectedMessage, MetricPoint
from .exceptions import (
    StreamlineError,
    ConnectionError,
    ProducerError,
    ConsumerError,
    TopicError,
)
from .retry import RetryConfig, retry_async, with_retry
from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpen, CircuitState
from .telemetry import StreamlineTracing
from .metrics import ClientMetrics, MetricsSnapshot
from .query import QueryClient, QueryResult
from .ai import AIClient
from .serializers import (
    SchemaRegistryClient,
    SchemaRegistryConfig,
    AvroSerializer,
    JsonSchemaSerializer,
)
from .schema_producer import SchemaProducer, SchemaConsumer, DeserializedRecord
from .traced import TracedProducer, TracedConsumer

__version__ = "0.2.0"

__all__ = [
    # Main client
    "StreamlineClient",
    # Producer
    "Producer",
    "ProducerRecord",
    "RecordMetadata",
    # Consumer
    "Consumer",
    "ConsumerRecord",
    # Admin
    "Admin",
    "TopicConfig",
    "TopicInfo",
    "PartitionInfo",
    # Exceptions
    "StreamlineError",
    "ConnectionError",
    "ProducerError",
    "ConsumerError",
    "TopicError",
    # Retry
    "RetryConfig",
    "retry_async",
    "with_retry",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpen",
    "CircuitState",
    # Telemetry
    "StreamlineTracing",
    # Metrics
    "ClientMetrics",
    "MetricsSnapshot",
    # Query
    "QueryClient",
    "QueryResult",
    # AI
    "AIClient",
    # Schema Registry
    "SchemaRegistryClient",
    "SchemaRegistryConfig",
    "AvroSerializer",
    "JsonSchemaSerializer",
    # Schema-aware wrappers
    "SchemaProducer",
    "SchemaConsumer",
    "DeserializedRecord",
    # Traced wrappers
    "TracedProducer",
    "TracedConsumer",
]
