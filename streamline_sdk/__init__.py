"""
Streamline Python SDK - Official Python client for Streamline.

Example usage:

    from streamline_sdk import StreamlineClient

    async def main():
        async with StreamlineClient(bootstrap_servers="localhost:9092") as client:
            # Produce a message
            await client.producer.send("my-topic", value=b"Hello, World!")

            # Consume messages
            async with client.consumer(group_id="example") as consumer:
                await consumer.subscribe(["my-topic"])
                async for message in consumer:
                    print(f"Received: {message.value}")

    asyncio.run(main())
"""

from __future__ import annotations

from .admin import (
    Admin,
    BranchInfo,
    BrokerInfo,
    ClusterInfo,
    ConsumerGroupLag,
    ConsumerLag,
    InspectedMessage,
    MetricPoint,
    PartitionInfo,
    TopicConfig,
    TopicInfo,
)
from .ai import AIClient
from .attestation import (
    ATTEST_HEADER,
    AttestationError,
    Attestor,
    SignedAttestation,
)
from .branches_admin import (
    BranchAdminClient,
    BranchAdminError,
    BranchMessage,
    BranchView,
)
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
    CircuitState,
)
from .client import StreamlineClient

# ``SearchHit`` is intentionally not re-exported from ``.consumer``: the public
# ``streamline_sdk.SearchHit`` is the HTTP search client's dataclass imported
# from ``.search`` below. Use ``streamline_sdk.consumer.SearchHit`` for the
# wire-protocol variant.
from .consumer import Consumer, ConsumerRecord
from .contracts import (
    ContractsClient,
    ContractsError,
    ValidationError,
    ValidationResult,
)
from .exceptions import (
    ConfigurationError,
    ConnectionError,
    ConsumerError,
    ProducerError,
    StreamlineError,
    TopicError,
)
from .memory import (
    MemoryClient,
    MemoryError,
    RecalledMemory,
    WrittenEntry,
)
from .metrics import ClientMetrics, MetricsSnapshot
from .producer import Producer, ProducerRecord, RecordMetadata
from .query import QueryClient, QueryResult
from .retry import RetryConfig, retry_async, with_retry
from .schema_producer import DeserializedRecord, SchemaConsumer, SchemaProducer
from .search import (
    SearchClient,
    SearchError,
    SearchHit,
    SearchResult,
)
from .serializers import (
    AvroSerializer,
    JsonSchemaSerializer,
    SchemaRegistryClient,
    SchemaRegistryConfig,
)
from .telemetry import StreamlineTracing
from .traced import TracedConsumer, TracedProducer
from .validation import validate_topic_name
from .verifier import (
    StreamlineVerifier,
)
from .verifier import (
    VerificationResult as AttestationVerificationResult,
)

__version__ = "0.4.0"

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
    "BranchInfo",
    "BrokerInfo",
    "ClusterInfo",
    "ConsumerGroupLag",
    "ConsumerLag",
    "InspectedMessage",
    "MetricPoint",
    # Exceptions
    "StreamlineError",
    "ConnectionError",
    "ProducerError",
    "ConsumerError",
    "TopicError",
    "ConfigurationError",
    # Retry
    "RetryConfig",
    "retry_async",
    "with_retry",
    # Validation
    "validate_topic_name",
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
    # Branches admin (M5 P1)
    "BranchAdminClient",
    "BranchAdminError",
    "BranchMessage",
    "BranchView",
    # Contracts validate (M2)
    "ContractsClient",
    "ContractsError",
    "ValidationError",
    "ValidationResult",
    # Attestation (M4)
    "ATTEST_HEADER",
    "Attestor",
    "AttestationError",
    "SignedAttestation",
    # Local attestation verifier
    "StreamlineVerifier",
    "AttestationVerificationResult",
    # Semantic search (M2)
    "SearchClient",
    "SearchError",
    "SearchHit",
    "SearchResult",
    # Agent Memory (M1)
    "MemoryClient",
    "MemoryError",
    "RecalledMemory",
    "WrittenEntry",
]
