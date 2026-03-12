"""Shared fixtures and configuration for integration tests."""

import os

import pytest

STREAMLINE_BOOTSTRAP_SERVERS = os.environ.get(
    "STREAMLINE_BOOTSTRAP_SERVERS",
    os.environ.get("STREAMLINE_BOOTSTRAP", "localhost:9092"),
)
STREAMLINE_HTTP_URL = os.environ.get("STREAMLINE_HTTP_URL", "http://localhost:9094")


@pytest.fixture
def bootstrap_servers():
    """Return the Streamline Kafka bootstrap address."""
    return STREAMLINE_BOOTSTRAP_SERVERS


@pytest.fixture
def http_url():
    """Return the Streamline HTTP API URL."""
    return STREAMLINE_HTTP_URL
