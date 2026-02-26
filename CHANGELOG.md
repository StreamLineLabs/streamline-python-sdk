# Changelog

All notable changes to this project will be documented in this file.
- feat: add async context manager for producer lifecycle

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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

