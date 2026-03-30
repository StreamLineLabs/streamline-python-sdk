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
from .consumer import Consumer, ConsumerRecord, SearchHit
from .admin import Admin, TopicConfig, TopicInfo, PartitionInfo
from .admin import ClusterInfo, BrokerInfo, ConsumerLag, ConsumerGroupLag, InspectedMessage, MetricPoint, BranchInfo
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
from .branches_admin import (
    BranchAdminClient,
    BranchAdminError,
    BranchMessage,
    BranchView,
)
from .contracts import (
    ContractsClient,
    ContractsError,
    ValidationError,
    ValidationResult,
)
from .attestation import (
    ATTEST_HEADER,
    Attestor,
    AttestationError,
    SignedAttestation,
)
from .verifier import (
    StreamlineVerifier,
    VerificationResult as AttestationVerificationResult,
)
from .search import (
    SearchClient,
    SearchError,
    SearchHit,
    SearchResult,
)
from .memory import (
    MemoryClient,
    MemoryError,
    RecalledMemory,
    WrittenEntry,
)

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
    "BranchInfo",
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
