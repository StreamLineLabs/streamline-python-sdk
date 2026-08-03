"""Shared fixtures and configuration for the test suite.

The default ``pytest tests/`` run is self-contained: it never talks to a
Streamline server. Tests that need a live broker are marked ``integration``
or ``conformance`` and are skipped unless the corresponding environment
variable is set:

* ``STREAMLINE_INTEGRATION=1`` — enables ``@pytest.mark.integration`` tests
  (see ``make integration-test``).
* ``CONFORMANCE=1`` — enables ``@pytest.mark.conformance`` tests.
"""

from __future__ import annotations

import os

import pytest

STREAMLINE_BOOTSTRAP_SERVERS = os.environ.get(
    "STREAMLINE_BOOTSTRAP_SERVERS",
    os.environ.get("STREAMLINE_BOOTSTRAP", "localhost:9092"),
)
STREAMLINE_HTTP_URL = os.environ.get("STREAMLINE_HTTP_URL", "http://localhost:9094")

#: Environment variables that opt a run in to server-dependent tests.
INTEGRATION_ENV_VAR = "STREAMLINE_INTEGRATION"
CONFORMANCE_ENV_VAR = "CONFORMANCE"

_TRUTHY = {"1", "true", "yes", "on"}


def _env_enabled(name: str) -> bool:
    """Return True when ``name`` is set to a truthy value."""
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def integration_enabled() -> bool:
    """Return True when server-dependent integration tests are opted in."""
    return _env_enabled(INTEGRATION_ENV_VAR)


def conformance_enabled() -> bool:
    """Return True when server-dependent conformance tests are opted in."""
    return _env_enabled(CONFORMANCE_ENV_VAR)


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip server-dependent tests unless their environment gate is enabled.

    Gating here (rather than with a module-level ``skipif``) keeps the tests
    *collected* and selectable via ``-m integration`` / ``-m conformance``,
    while a bare ``pytest tests/`` never opens a socket against localhost.
    """
    gates = (
        (
            "integration",
            integration_enabled(),
            f"set {INTEGRATION_ENV_VAR}=1 and provide a running Streamline "
            "server to run integration tests",
        ),
        (
            "conformance",
            conformance_enabled(),
            f"set {CONFORMANCE_ENV_VAR}=1 and provide a running Streamline "
            "server to run conformance tests",
        ),
    )
    for marker_name, enabled, reason in gates:
        if enabled:
            continue
        skip_marker = pytest.mark.skip(reason=reason)
        for item in items:
            if item.get_closest_marker(marker_name) is not None:
                item.add_marker(skip_marker)


@pytest.fixture
def bootstrap_servers() -> str:
    """Return the Streamline Kafka bootstrap address."""
    return STREAMLINE_BOOTSTRAP_SERVERS


@pytest.fixture
def http_url() -> str:
    """Return the Streamline HTTP API URL."""
    return STREAMLINE_HTTP_URL
