# Contributing to Streamline Python SDK

Thank you for your interest in contributing to the Streamline Python SDK! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m "Add my feature"`)
6. Push to your fork (`git push origin feature/my-feature`)
7. Open a Pull Request

## Prerequisites

- Python 3.9 or later
- pip

## Development Setup

```bash
# Clone your fork
git clone https://github.com/<your-username>/streamline-python-sdk.git
cd streamline-python-sdk

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with development dependencies
pip install -e ".[dev]"
```

## Running Tests

```bash
# Run all tests (self-contained — no server required)
pytest tests/ -v

# Run a specific test file
pytest tests/test_producer.py -v

# Run with coverage
pytest tests/ --cov=streamline --cov-report=term-missing
```

### Integration Tests

Integration tests require a running Streamline server. They are marked
`integration` and skipped by the default run unless `STREAMLINE_INTEGRATION=1`
is set:

```bash
# Start the server
docker compose -f docker-compose.test.yml up -d

# Run integration tests
STREAMLINE_INTEGRATION=1 pytest tests/ -v -m integration

# Stop the server
docker compose -f docker-compose.test.yml down
```

The conformance suite under `tests/conformance/` is marked `conformance` and
gated the same way with `CONFORMANCE=1`:

```bash
CONFORMANCE=1 pytest tests/conformance -v -m conformance
```

## Linting & Type Checking

```bash
# Lint with ruff
ruff check .

# Auto-fix lint issues
ruff check . --fix

# Type checking
mypy
```

## Code Style

- Follow PEP 8 and existing code patterns
- Use type hints for all public functions and methods
- Add docstrings for public APIs (Google style)
- Keep functions focused and short

## Pull Request Guidelines

- Write clear commit messages
- Add tests for new functionality
- Update documentation if needed
- Ensure all checks pass (`pytest`, `ruff check`, `mypy`)

## Reporting Issues

- Use the **Bug Report** or **Feature Request** issue templates
- Search existing issues before creating a new one
- Include reproduction steps for bugs

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](https://github.com/streamlinelabs/.github/blob/main/CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
<!-- docs: 93baf60d -->
<!-- chore: ae0af4a6 -->
