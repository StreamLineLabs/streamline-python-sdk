# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Fixed
- Python 3.9 support: every runtime module now carries
  `from __future__ import annotations`, so PEP 604 (`str | None`) and PEP 585
  (`list[str]`) annotations are no longer evaluated at import time. Previously
  `import streamline_sdk` raised `TypeError` on Python 3.9.
- `cryptography>=42.0.0` is now a declared runtime dependency. The package
  `__init__` re-exports `StreamlineVerifier`/`AttestationVerificationResult`
  from `streamline_sdk.verifier`, which imports `cryptography` at module scope,
  so a plain `pip install streamline-sdk` previously produced an unimportable
  package.
- `pytest tests/` is self-contained again: server-dependent tests are gated on
  `STREAMLINE_INTEGRATION=1` (marker `integration`) and `CONFORMANCE=1`
  (marker `conformance`) instead of failing/hanging against `localhost`.
  `make integration-test` and the `integration` workflow remain the runnable
  integration entry points.
- `ruff check .` and `mypy` pass again: modernised annotations, fixed unused
  imports/variables, and replaced `Any` leaks with explicit types and guards.
- `Producer.send_record()` now returns a `datetime` timestamp for buffered
  transactional records, matching `RecordMetadata.timestamp`.
- `examples/agent_memory/memory_demo.py` uses the real `MemoryClient` API
  instead of non-existent `StreamlineClient.memory_*` helpers.

### Added
- `search` extra (`pip install streamline-sdk[search]`) providing `aiohttp`,
  matching the hint already raised by `Consumer.search()`.
- `BrokerInfo`, `ClusterInfo`, `ConsumerLag`, `ConsumerGroupLag`,
  `InspectedMessage` and `MetricPoint` are now listed in `streamline_sdk.__all__`
  (they were already importable from the package).
- Regression tests for the packaging/import contract and for the test-suite
  gating (`tests/test_packaging_contract.py`, `tests/test_suite_gating.py`).

### Changed
- `mypy` is pinned to `<2.0` in the `dev` extra: mypy 2.x rejects
  `python_version = "3.9"`, which this package still targets.


## [0.3.0] - 2026-04-20

### Added
- Moonshot HTTP modules under `streamline_sdk`:
  - `branches_admin.BranchesClient` (M5)
  - `contracts.ContractsClient` (M4)
  - `attestation.AttestationClient` (M4)
  - `search.SearchClient` (M2)
  - `memory.MemoryClient` (M1)
- Each client is sync-friendly and uses `httpx` under the hood.

### Added
- Admin: `cluster_info()` — cluster overview including broker list
- Admin: `consumer_group_lag()` / `consumer_group_topic_lag()` — consumer group lag monitoring
- Admin: `inspect_messages()` / `latest_messages()` — message inspection by offset
- Admin: `metrics_history()` — server metrics history
- Model types: `ClusterInfo`, `BrokerInfo`, `ConsumerLag`, `ConsumerGroupLag`, `InspectedMessage`, `MetricPoint`
- New types exported from `streamline_sdk` package `__init__.py`

### Fixed
- AI client urllib fallback now has 30-second timeout (was missing, could hang indefinitely)
- Query client urllib fallback now has timeout to prevent event loop blocking

### Changed
- feat: add async context manager for producer lifecycle
- feat: add batch consumer with configurable prefetch (2026-03-05)
- refactor: improve type hints for public API (2026-03-06)
- **Changed**: update pyproject.toml dependencies
- **Changed**: simplify connection configuration dataclass
- **Testing**: add pytest fixtures for producer testing
- **Fixed**: resolve event loop conflict with aiokafka
- **Added**: add async context manager for consumer sessions

### Fixed
- Resolve asyncio event loop cleanup on shutdown
- Resolve event loop handling in producer close

### Changed
- Improve consumer group coordinator logic

### Performance
- Optimize message deserialization path
- Optimize message batching with memoryview


## [0.2.0] - 2026-02-18

### Added
- `StreamlineClient` with async context manager support
- `Producer` with async message sending and batching
- `Consumer` with async iteration and consumer group support
- `Admin` client for topic and group management
- Configuration via `StreamlineConfig` dataclass
- 8-type exception hierarchy with retryability flags
- Retry utilities with exponential backoff
- SASL authentication support (PLAIN, SCRAM)
- TLS/SSL connection support
- Testcontainers integration for testing

### Infrastructure
- CI pipeline with pytest, coverage reporting, and multi-Python matrix (3.9-3.12)
- CodeQL security scanning
- Release workflow with PyPI publishing
- Release drafter for automated release notes
- Dependabot for dependency updates
- CONTRIBUTING.md with development setup guide
- Security policy (SECURITY.md)
- EditorConfig for consistent formatting
- Ruff linter and formatter configuration
- MyPy type checking configuration
- Issue templates for bug reports and feature requests

## [0.1.0] - 2026-02-18

### Added
- Initial release of Streamline Python SDK
- Async-first design built on aiokafka
- Testcontainers support for integration testing
- Apache 2.0 license
- test: expand telemetry hook coverage for async producers
- test: add serializer roundtrip and edge case tests
