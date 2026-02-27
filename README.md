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
- Async support
- Type hints throughout
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

## License

Apache-2.0
<!-- refactor: de37c839 -->
<!-- fix: 380469bb -->

## Security

To report a security vulnerability, please email **security@streamline.dev**.
Do **not** open a public issue.

See the [Security Policy](https://github.com/streamlinelabs/streamline/blob/main/SECURITY.md) for details.
