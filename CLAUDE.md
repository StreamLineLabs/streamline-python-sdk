# CLAUDE.md — Streamline Python SDK

## Overview
Async Python SDK for [Streamline](https://github.com/streamlinelabs/streamline), built on `aiokafka`. Communicates with Streamline via the Kafka wire protocol on port 9092.

## Build & Test
```bash
pip install -e ".[dev]"     # Install with dev dependencies
pytest tests/               # Run tests
mypy .                      # Type check
ruff check .                # Lint
ruff format .               # Format
```

## Architecture
```
streamline_sdk/
├── __init__.py          # Public API exports
├── client.py            # StreamlineClient — main entry point
├── producer.py          # Producer with batching & compression
├── consumer.py          # Consumer with group coordination
├── admin.py             # Topic/group management
├── client.py            # Main client and configuration dataclasses
├── exceptions.py        # Exception hierarchy with hints & retryable flag
├── retry.py             # Retry logic with backoff
└── types.py             # Shared type definitions
```

## Coding Conventions
- **Async-first**: All I/O operations use `async/await`
- **Type annotations**: Full type hints required (`from __future__ import annotations`)
- **Dataclasses**: Use for configuration objects (`ClientConfig`, `ProducerConfig`, `ConsumerConfig`)
- **Error handling**: Raise `StreamlineError` subclasses with `.hint` and `.retryable` attributes
- **Context managers**: Use `async with StreamlineClient(...)` pattern for resource cleanup
- **Naming**: snake_case for functions/variables, PascalCase for classes

## Error Handling Pattern
```python
from streamline_sdk.exceptions import StreamlineError, ConnectionError

try:
    await client.producer.send("topic", value=b"value")
except ConnectionError as e:
    if e.retryable:
        # Retry logic
    print(e.hint)  # Actionable guidance
```

## Dependencies
- `aiokafka>=0.10.0` — Core Kafka protocol client
- Optional: `opentelemetry-api` for tracing (install with `pip install streamline-sdk[telemetry]`)

## Testing
- Unit tests: `tests/test_*.py` using `pytest` + `pytest-asyncio`
- Integration tests: Use `testcontainers/` with Docker Compose to spin up a real Streamline server
- Benchmarks: `benchmarks/` directory
