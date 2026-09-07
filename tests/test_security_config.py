"""Tests for shared TLS and SASL configuration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk._security import build_security_kwargs
from streamline_sdk.admin import Admin
from streamline_sdk.client import ClientConfig, ConsumerConfig, ProducerConfig
from streamline_sdk.consumer import Consumer
from streamline_sdk.exceptions import ConfigurationError
from streamline_sdk.producer import Producer


def test_tls_context_uses_all_configured_certificate_files() -> None:
    config = ClientConfig(
        security_protocol="SSL",
        ssl_cafile="ca.pem",
        ssl_certfile="client.pem",
        ssl_keyfile="client.key",
    )
    ssl_context = MagicMock(name="ssl_context")

    with patch(
        "streamline_sdk._security.create_ssl_context",
        return_value=ssl_context,
    ) as create_context:
        kwargs = build_security_kwargs(config)

    create_context.assert_called_once_with(
        cafile="ca.pem",
        certfile="client.pem",
        keyfile="client.key",
    )
    assert kwargs == {
        "security_protocol": "SSL",
        "ssl_context": ssl_context,
    }


def test_sasl_ssl_combines_credentials_and_tls_context() -> None:
    config = ClientConfig(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="service-account",
        sasl_password="secret",
        ssl_cafile="ca.pem",
    )
    ssl_context = MagicMock(name="ssl_context")

    with patch(
        "streamline_sdk._security.create_ssl_context",
        return_value=ssl_context,
    ):
        kwargs = build_security_kwargs(config)

    assert kwargs == {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": "service-account",
        "sasl_plain_password": "secret",
        "ssl_context": ssl_context,
    }


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (
            ClientConfig(sasl_mechanism="PLAIN"),
            "SASL settings require",
        ),
        (
            ClientConfig(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
            ),
            "requires both sasl_username and sasl_password",
        ),
        (
            ClientConfig(security_protocol="SSL", ssl_certfile="client.pem"),
            "requires both ssl_certfile and ssl_keyfile",
        ),
        (
            ClientConfig(ssl_cafile="ca.pem"),
            "TLS certificate settings require",
        ),
        (
            ClientConfig(security_protocol="UNKNOWN"),
            "Unsupported security_protocol",
        ),
    ],
)
def test_invalid_security_combinations_fail_early(
    config: ClientConfig, message: str
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        build_security_kwargs(config)


@pytest.mark.asyncio
async def test_producer_receives_built_ssl_context() -> None:
    ssl_context = MagicMock(name="ssl_context")
    producer = Producer(
        ClientConfig(security_protocol="SSL", ssl_cafile="ca.pem"),
        ProducerConfig(),
    )

    with patch(
        "streamline_sdk._security.create_ssl_context",
        return_value=ssl_context,
    ):
        with patch("streamline_sdk.producer.AIOKafkaProducer") as constructor:
            constructor.return_value.start = AsyncMock()
            await producer.start()

    assert constructor.call_args.kwargs["ssl_context"] is ssl_context
    assert constructor.call_args.kwargs["security_protocol"] == "SSL"


@pytest.mark.asyncio
async def test_consumer_receives_built_ssl_context() -> None:
    ssl_context = MagicMock(name="ssl_context")
    consumer = Consumer(
        ClientConfig(security_protocol="SSL", ssl_cafile="ca.pem"),
        ConsumerConfig(group_id="secure-group"),
    )

    with patch(
        "streamline_sdk._security.create_ssl_context",
        return_value=ssl_context,
    ):
        with patch("streamline_sdk.consumer.AIOKafkaConsumer") as constructor:
            constructor.return_value.start = AsyncMock()
            await consumer.start()

    assert constructor.call_args.kwargs["ssl_context"] is ssl_context
    assert constructor.call_args.kwargs["security_protocol"] == "SSL"


@pytest.mark.asyncio
async def test_admin_receives_built_ssl_context() -> None:
    ssl_context = MagicMock(name="ssl_context")
    admin = Admin(ClientConfig(security_protocol="SSL", ssl_cafile="ca.pem"))

    with patch(
        "streamline_sdk._security.create_ssl_context",
        return_value=ssl_context,
    ):
        with patch("streamline_sdk.admin.AIOKafkaAdminClient") as constructor:
            constructor.return_value.start = AsyncMock()
            await admin.start()

    assert constructor.call_args.kwargs["ssl_context"] is ssl_context
    assert constructor.call_args.kwargs["security_protocol"] == "SSL"
