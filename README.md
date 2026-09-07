# Streamline Python SDK

Official Python client for [Streamline](https://github.com/streamlinelabs/streamline) — The Redis of Streaming.

[![CI](https://github.com/streamlinelabs/streamline-python-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-python-sdk/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline-python-sdk?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline-python-sdk)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9--3.14-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/streamline-sdk)](https://pypi.org/project/streamline-sdk/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/python)

## Installation

```bash
pip install streamline-sdk
```

## Quick Start

<!-- snippet-source: examples/readme_quickstart.py -->
```python
from __future__ import annotations

import asyncio

from streamline_sdk import StreamlineClient


async def main() -> None:
    async with StreamlineClient("localhost:9092") as client:
        metadata = await client.producer.send(
            "events",
            key=b"user-42",
            value=b"Hello, Streamline!",
        )
        print(f"wrote partition={metadata.partition} offset={metadata.offset}")

        async with client.consumer(group_id="quickstart") as consumer:
            await consumer.subscribe(["events"])
            for message in await consumer.poll(timeout_ms=1_000):
                print(message.value)


if __name__ == "__main__":
    asyncio.run(main())
```

## Features

- Kafka protocol compatible
- Producer and consumer APIs
- Consumer group support
- Admin client (topic management, consumer groups)
- SQL query support
- Async support (async/await native)
- Type hints throughout
- Compression (LZ4, Zstd, Snappy, Gzip)
- TLS/mTLS and SASL authentication (PLAIN, SCRAM-SHA-256/512)
- Automatic reconnection with exponential backoff
- Optional OpenTelemetry tracing for produce/consume operations

## OpenTelemetry Tracing

The SDK supports optional distributed tracing via OpenTelemetry. Install with the
`telemetry` extra:

```bash
pip install streamline-sdk[telemetry]
```

When `opentelemetry-api` is not installed, the tracing layer is a zero-overhead no-op.

### Usage

```python
from streamline_sdk import StreamlineTracing

tracing = StreamlineTracing()

# As an async context manager
async with tracing.trace_produce("orders", headers={}):
    await producer.send("orders", value=b"order-data")

# As a decorator
@tracing.traced_consume("events")
async def handle(records):
    for record in records:
        process(record)

# Trace individual record processing with context propagation
async for record in consumer:
    async with tracing.trace_process(
        record.topic, record.partition, record.offset, record.headers
    ):
        process(record)
```

### Span Conventions

| Attribute | Value |
|-----------|-------|
| Span name | `{topic} {operation}` (e.g., "orders produce") |
| `messaging.system` | `streamline` |
| `messaging.destination.name` | Topic name |
| `messaging.operation` | `produce`, `consume`, or `process` |
| Span kind | `PRODUCER` for produce, `CONSUMER` for consume |

Trace context is automatically propagated through message headers using the
W3C TraceContext format.

## Testing

### Unit Tests

The default run is self-contained — it never contacts a Streamline server:

```bash
pip install -e ".[dev]"
pytest tests/
```

### Integration Tests

Requires a running Streamline server. Server-dependent tests are marked
`integration` (or `conformance`) and are skipped unless explicitly enabled
via `STREAMLINE_INTEGRATION=1` (or `CONFORMANCE=1`):

```bash
docker compose -f docker-compose.test.yml up -d
STREAMLINE_INTEGRATION=1 pytest tests/ -m integration
# or simply:
make integration-test
```

CI runs conformance separately with `CONFORMANCE=1` and an enforcement flag
that fails if no selected conformance test reaches its call phase. The bundled
server fixture currently exposes plaintext Kafka and HTTP ports only; TLS,
mTLS, and SASL conformance require externally supplied endpoints and
certificate/credential environment variables described in
[`AUDIT.md`](AUDIT.md).

## Testcontainers

For integration testing, use the bundled testcontainers module (source-only;
not published to PyPI — see
[`testcontainers/README.md`](testcontainers/README.md)). It requires an
explicit, digest-pinned image reference; there is no default image:

```python
import pytest

from streamline_sdk import StreamlineClient
from streamline_testcontainers import StreamlineContainer

IMAGE = "ghcr.io/streamlinelabs/streamline@sha256:<digest-you-verified>"


@pytest.mark.asyncio
async def test_with_streamline() -> None:
    with StreamlineContainer(IMAGE) as streamline:
        async with StreamlineClient(
            streamline.get_bootstrap_servers()
        ) as client:
            await client.producer.send("events", value=b"test")
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `StreamlineClient(bootstrap_servers)` | Create a new client |
| `client.producer` | Get the producer instance |
| `client.admin` | Get the admin client |
| `client.consumer(group_id, **kwargs)` | Create a consumer |
| `await client.start()` | Connect to the cluster |
| `await client.close()` | Close all connections |
| `client.is_connected` | Check connection status |

### Producer

| Method | Description |
|--------|-------------|
| `await producer.send(topic, value, key=None)` | Send a message |
| `await producer.send_record(record)` | Send a ProducerRecord |
| `await producer.send_batch(records)` | Send a batch of messages |
| `await producer.flush()` | Flush buffered messages |
| `await producer.close()` | Close the producer |

### Transactions

<!-- snippet-source: examples/readme_transactions.py -->
```python
from __future__ import annotations

import asyncio

from streamline_sdk import StreamlineClient


async def main() -> None:
    async with StreamlineClient("localhost:9092") as client:
        producer = client.producer
        await producer.begin_transaction()
        try:
            await producer.send("orders", key=b"k1", value=b"v1")
            await producer.send("orders", key=b"k2", value=b"v2")
            await producer.commit_transaction()
        except Exception:
            # commit_transaction() exits buffering mode *before* replaying the
            # buffered sends (see its docstring), so if it fails partway
            # through that replay, the transaction is already over. Calling
            # abort_transaction() unconditionally here would raise
            # "RuntimeError: No transaction in progress" and mask the real
            # commit failure. Guard with in_transaction so we only abort
            # when buffering mode is still active (e.g. begin_transaction()
            # succeeded but a send() before commit raised).
            if producer.in_transaction:
                await producer.abort_transaction()
            raise


if __name__ == "__main__":
    asyncio.run(main())
```

> **Note:** Transactions use client-side buffering — messages are collected while buffering is
> active and are replayed as ordinary, independent sends when `commit_transaction()` is called.
> **This is not atomic.** The Streamline broker has no cross-message transactional coordinator,
> so if the connection fails partway through the replay, earlier messages may already be
> delivered while later ones are not: **partial delivery is possible**. Because
> `commit_transaction()` exits buffering mode before replaying, always guard a
> fallback `abort_transaction()` call with `producer.in_transaction` (as shown
> above) so it cannot mask the original commit failure by raising its own
> `RuntimeError: No transaction in progress`.

### Consumer

| Method | Description |
|--------|-------------|
| `await consumer.subscribe(topics)` | Subscribe to topics |
| `await consumer.unsubscribe()` | Unsubscribe from all topics |
| `await consumer.poll(timeout_ms)` | Poll for messages |
| `await consumer.commit(offsets)` | Commit offsets |
| `await consumer.seek(partition, offset)` | Seek to a specific offset |
| `await consumer.seek_to_beginning(partitions)` | Seek to start |
| `await consumer.seek_to_end(partitions)` | Seek to end |
| `consumer.assignment()` | Get assigned partitions |
| `consumer.subscription()` | Get subscribed topics |

### Admin

| Method | Description |
|--------|-------------|
| `await admin.create_topic(config)` | Create a topic |
| `await admin.delete_topic(name)` | Delete a topic |
| `await admin.list_topics()` | List all topics |
| `await admin.describe_topic(name)` | Get topic information |
| `await admin.list_consumer_groups()` | List consumer groups |
| `await admin.describe_consumer_group(group_id)` | Get group information |
| `await admin.cluster_info()` | Cluster overview (brokers, controller) |
| `await admin.consumer_group_lag(group_id)` | Consumer group lag monitoring |
| `await admin.consumer_group_topic_lag(group_id, topic)` | Topic-scoped lag |
| `await admin.inspect_messages(topic, partition, offset, limit)` | Browse messages |
| `await admin.latest_messages(topic, count)` | Get latest messages |
| `await admin.metrics_history()` | Server metrics history |

```python
# StreamlineClient starts and owns its Admin instance.
cluster = await client.admin.cluster_info()
print(f"Cluster: {cluster.cluster_id}, Brokers: {len(cluster.brokers)}")

lag = await client.admin.consumer_group_lag("my-group")
print(f"Total lag: {lag.total_lag}")
for partition in lag.partitions:
    print(f"  {partition.topic}:{partition.partition} lag={partition.lag}")

messages = await client.admin.inspect_messages("events", partition=0, limit=10)
for message in messages:
    print(f"offset={message.offset} value={message.value}")

metrics = await client.admin.metrics_history()
```

## Requirements

- Python 3.9 through 3.14
- Streamline server 0.4.0 or later

## Error Handling

```python
from streamline_sdk import StreamlineClient, StreamlineError

async with StreamlineClient("localhost:9092") as client:
    try:
        await client.producer.send("my-topic", key=b"key", value=b"value")
    except StreamlineError as error:
        print(error)
        if error.hint:
            print(f"Hint: {error.hint}")
```

## Configuration Reference

### Client

| Parameter | Default | Description |
|---|---|---|
| `bootstrap_servers` | `localhost:9092` | Comma-separated broker addresses |
| `client_id` | `streamline-python-client` | Client identifier for server-side logging |

### Producer

| Parameter | Default | Description |
|---|---|---|
| `batch_size` | `16384` | Maximum batch size in bytes |
| `linger_ms` | `0` | Time to wait before sending a batch (ms) |
| `compression_type` | `none` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `acks` | `all` | Acknowledgments: `0` (none), `1` (leader), `all` (all replicas) |
| `retries` | `3` | Retries on transient failures |
| `enable_idempotence` | `False` | Enable exactly-once semantics |

### Consumer

| Parameter | Default | Description |
|---|---|---|
| `group_id` | *(required)* | Consumer group identifier |
| `auto_offset_reset` | `latest` | Start position: `earliest`, `latest` |
| `enable_auto_commit` | `True` | Automatically commit offsets |
| `auto_commit_interval_ms` | `5000` | Auto-commit interval (ms) |
| `max_poll_records` | `500` | Maximum records per poll |
| `session_timeout_ms` | `30000` | Session timeout (ms) |

### Security

| Parameter | Default | Description |
|---|---|---|
| `security_protocol` | `PLAINTEXT` | Protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | — | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl_username` | — | SASL username |
| `sasl_password` | — | SASL password |
| `ssl_cafile` | — | Path to CA certificate file |
| `ssl_certfile` | — | Path to an mTLS client certificate |
| `ssl_keyfile` | — | Path to the matching mTLS client key |

## Circuit Breaker

Protect your application from cascading failures when the Streamline server is unresponsive:

```python
from streamline_sdk import CircuitBreaker, CircuitBreakerConfig

cb = CircuitBreaker(
    CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=2,
        open_timeout_s=30.0,
    )
)

if cb.allow():
    try:
        await client.producer.send("events", key=b"key", value=b"value")
        cb.record_success()
    except Exception:
        cb.record_failure()
        raise
```

When the circuit is open, `allow()` returns `False` and operations should be skipped or rejected. See the [Circuit Breaker guide](https://streamlinelabs.dev/docs/features/circuit-breaker) for details.

## Examples

The [`examples/`](examples/) directory contains runnable examples:

| Example | Description |
|---------|-------------|
| [basic_usage.py](examples/basic_usage.py) | Produce, consume, and admin operations |
| [query_usage.py](examples/query_usage.py) | SQL analytics with the embedded query engine |
| [schema_registry.py](examples/schema_registry.py) | Schema registration and validation |
| [circuit_breaker.py](examples/circuit_breaker.py) | Resilient production with circuit breaker |
| [security.py](examples/security.py) | TLS and SASL authentication |

Run any example:

```bash
python examples/basic_usage.py
```

## Moonshot Features

> ⚠️ **Experimental** — These features require Streamline server 0.3.0+ with moonshot feature flags enabled.

### Semantic Search

Query topics by meaning instead of offset. Requires a topic created with `semantic.embed=true`.

```python
results = await consumer.search("logs.app", "payment failure", k=10)
for hit in results:
    print(f"[p{hit.partition}] offset={hit.offset} score={hit.score:.2f}")
```

### Attestation Verification

Verify cryptographic provenance attestations attached to records by data contracts.

```python
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from streamline_sdk import StreamlineVerifier

public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)
verifier = StreamlineVerifier(public_key)
result = verifier.verify(record)
print(f"Verified: {result.verified}, Producer: {result.producer_id}")
```

### Agent Memory (MCP)

Use Streamline as persistent memory for AI agents via the MCP protocol.

```python
from streamline_sdk import MemoryClient

memory = MemoryClient("http://localhost:9094")
await memory.remember(
    agent_id="assistant",
    kind="observation",
    content="user prefers dark mode",
    tags=["preferences"],
)
results = await memory.recall(
    agent_id="assistant",
    query="user preferences",
    k=5,
)
```

### Branched Streams

Create topic branches for replay, A/B testing, or counterfactual analysis.

```python
from streamline_sdk import BranchAdminClient

branches = BranchAdminClient("http://localhost:9094")
branch = await branches.create("events", "experiment-v2")
await branches.append(branch.id, role="user", text="replay this event")
messages = await branches.messages(branch.id)
```

## Contributing

Contributions are welcome! Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0
<!-- refactor: de37c839 -->
<!-- fix: 380469bb -->

## Security

To report a security vulnerability, please email **security@streamline.dev**.
Do **not** open a public issue.

See the [Security Policy](https://github.com/streamlinelabs/streamline/blob/main/SECURITY.md) for details.
