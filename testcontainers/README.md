# Testcontainers Streamline (Python)

[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](LICENSE)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline).

> **Source-only, unpublished:** this package is not published to PyPI (see
> "Installation" below for why) and CI only builds and validates it
> (`python -m build` + `twine check`) — it never runs `twine upload`. Use it
> from a source checkout.

## Features

- Kafka-compatible container for testing
- No ZooKeeper or KRaft required
- Built-in health checks

## Installation

There is no `testcontainers-streamline` package on PyPI. Install from a
source checkout of this repository instead:

```bash
pip install -e streamline-python-sdk/testcontainers
```

## Usage

### Basic Usage

`StreamlineContainer` requires an explicit, digest-pinned image reference
(`registry/repo@sha256:<digest>`) — there is no default image. A mutable
tag (including `:latest` or a version tag like `:0.3.0`) is rejected,
because this SDK cannot verify in advance that a given tag exists or
contains a working Streamline server; supply a digest you have verified
yourself.

```python
from streamline_testcontainers import StreamlineContainer
from kafka import KafkaProducer, KafkaConsumer

IMAGE = "ghcr.io/streamlinelabs/streamline@sha256:<digest-you-verified>"

# Using context manager (recommended)
with StreamlineContainer(IMAGE) as streamline:
    bootstrap_servers = streamline.get_bootstrap_servers()

    # Use with any Kafka client
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send("my-topic", b"Hello, Streamline!")
    producer.flush()
    producer.close()
```

### With pytest

```python
import pytest
from streamline_testcontainers import StreamlineContainer

IMAGE = "ghcr.io/streamlinelabs/streamline@sha256:<digest-you-verified>"

@pytest.fixture(scope="module")
def streamline():
    with StreamlineContainer(IMAGE) as container:
        yield container

def test_kafka_integration(streamline):
    bootstrap_servers = streamline.get_bootstrap_servers()
    # Your test code here
```

### With Debug Logging

```python
with StreamlineContainer(IMAGE).with_debug_logging() as streamline:
    # Container will output debug logs
    pass
```

### Create Topics

```python
with StreamlineContainer(IMAGE) as streamline:
    streamline.create_topic("my-topic", partitions=3)
```

### Access HTTP API

```python
import requests

with StreamlineContainer(IMAGE) as streamline:
    # Health check
    response = requests.get(streamline.get_health_url(), timeout=5)
    assert response.status_code == 200

    # Metrics
    metrics = requests.get(streamline.get_metrics_url(), timeout=5).text
```

## API Reference

### StreamlineContainer

| Method | Description |
|--------|-------------|
| `get_bootstrap_servers()` | Returns Kafka bootstrap servers string |
| `get_http_url()` | Returns HTTP API base URL |
| `get_health_url()` | Returns health check endpoint URL |
| `get_metrics_url()` | Returns Prometheus metrics URL |
| `create_topic(name, partitions)` | Creates a topic |
| `with_debug_logging()` | Enables debug logging |
| `with_trace_logging()` | Enables trace logging |
| `with_log_level(level)` | Sets trace/debug/info/warn/error logging |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## License

Apache-2.0
