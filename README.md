# Streamline Python SDK

Official Python client for [Streamline](https://github.com/streamlinelabs/streamline) — The Redis of Streaming.

[![CI](https://github.com/streamlinelabs/streamline-python-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-python-sdk/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline-python-sdk?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline-python-sdk)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/streamline-sdk)](https://pypi.org/project/streamline-sdk/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/python)

## Installation

```bash
pip install streamline-sdk
```

## Quick Start

```python
from streamline_sdk import StreamlineClient

# Connect to Streamline
client = StreamlineClient("localhost:9092")

# Produce messages
producer = client.producer("my-topic")
producer.send("Hello, Streamline!")

# Consume messages
consumer = client.consumer("my-topic", group_id="my-group")
for message in consumer:
    print(message.value)
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
- Connection pooling with configurable pool size
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

```bash
pip install -e ".[dev]"
pytest tests/
```

### Integration Tests

Requires a running Streamline server:

```bash
docker compose -f docker-compose.test.yml up -d
pytest tests/ -m integration
```

## Testcontainers

For integration testing, use the bundled testcontainers module:

```python
from testcontainers.streamline import StreamlineContainer

with StreamlineContainer() as streamline:
    client = StreamlineClient(streamline.get_bootstrap_servers())
    # ... run tests
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
async with client.admin as admin:
    # Cluster overview
    cluster = await admin.cluster_info()
    print(f"Cluster: {cluster.cluster_id}, Brokers: {len(cluster.brokers)}")

    # Consumer group lag
    lag = await admin.consumer_group_lag("my-group")
    print(f"Total lag: {lag.total_lag}")
    for p in lag.partitions:
        print(f"  {p.topic}:{p.partition} lag={p.lag}")

    # Message inspection
    messages = await admin.inspect_messages("events", partition=0, limit=10)
    for m in messages:
        print(f"offset={m.offset} value={m.value}")

    # Server metrics
    metrics = await admin.metrics_history()
```

## Requirements

- Python 3.9 or later
- Streamline server 0.2.0 or later

## Error Handling

```python
from streamline import StreamlineClient, StreamlineError, TopicNotFoundError

async with StreamlineClient("localhost:9092") as client:
    try:
        await client.produce("my-topic", b"key", b"value")
    except TopicNotFoundError as e:
        print(f"Topic not found: {e}")
        print(f"Hint: {e.hint}")  # Actionable guidance
    except StreamlineError as e:
        if e.retryable:
            print(f"Retryable error: {e}")
        else:
            print(f"Fatal error: {e}")
```

## Configuration Reference

### Client

| Parameter | Default | Description |
|---|---|---|
| `bootstrap_servers` | `localhost:9092` | Comma-separated broker addresses |
| `client_id` | auto-generated | Client identifier for server-side logging |

### Producer

| Parameter | Default | Description |
|---|---|---|
| `batch_size` | `16384` | Maximum batch size in bytes |
| `linger_ms` | `0` | Time to wait before sending a batch (ms) |
| `compression_type` | `none` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `acks` | `1` | Acknowledgments: `0` (none), `1` (leader), `all` (all replicas) |
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
| `ssl_cafile` | — | Path to CA certificate file |

## Circuit Breaker

Protect your application from cascading failures when the Streamline server is unresponsive:

```python
from streamline.circuit_breaker import CircuitBreaker

cb = CircuitBreaker(
    failure_threshold=5,       # Open after 5 consecutive failures
    success_threshold=2,       # Close after 2 half-open successes
    open_timeout=30.0,         # 30s before probing
)

if cb.allow():
    try:
        await producer.send("events", b"key", b"value")
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

