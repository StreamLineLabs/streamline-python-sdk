"""Shared aiokafka security configuration."""

from __future__ import annotations

from typing import Any

from aiokafka.helpers import create_ssl_context

from .exceptions import ConfigurationError

_SECURITY_PROTOCOLS = {
    "PLAINTEXT",
    "SSL",
    "SASL_PLAINTEXT",
    "SASL_SSL",
}
_SASL_PROTOCOLS = {"SASL_PLAINTEXT", "SASL_SSL"}
_TLS_PROTOCOLS = {"SSL", "SASL_SSL"}


def build_security_kwargs(client_config: Any) -> dict[str, Any]:
    """Build validated security keyword arguments for aiokafka clients."""
    protocol = str(client_config.security_protocol).upper()
    if protocol not in _SECURITY_PROTOCOLS:
        supported = ", ".join(sorted(_SECURITY_PROTOCOLS))
        raise ConfigurationError(
            f"Unsupported security_protocol {protocol!r}; expected one of {supported}"
        )

    mechanism = client_config.sasl_mechanism
    username = client_config.sasl_username
    password = client_config.sasl_password
    has_sasl_values = any(
        value is not None for value in (mechanism, username, password)
    )

    if protocol in _SASL_PROTOCOLS:
        if not mechanism:
            raise ConfigurationError(
                f"{protocol} requires sasl_mechanism to be configured"
            )
        if username is None or password is None:
            raise ConfigurationError(
                f"{protocol} requires both sasl_username and sasl_password"
            )
    elif has_sasl_values:
        raise ConfigurationError(
            "SASL settings require security_protocol='SASL_PLAINTEXT' or 'SASL_SSL'"
        )

    ssl_cafile = client_config.ssl_cafile
    ssl_certfile = client_config.ssl_certfile
    ssl_keyfile = client_config.ssl_keyfile
    has_tls_files = any(
        value is not None for value in (ssl_cafile, ssl_certfile, ssl_keyfile)
    )

    if has_tls_files and protocol not in _TLS_PROTOCOLS:
        raise ConfigurationError(
            "TLS certificate settings require security_protocol='SSL' or 'SASL_SSL'"
        )
    if (ssl_certfile is None) != (ssl_keyfile is None):
        raise ConfigurationError(
            "Mutual TLS requires both ssl_certfile and ssl_keyfile"
        )

    kwargs: dict[str, Any] = {}
    if protocol != "PLAINTEXT":
        kwargs["security_protocol"] = protocol

    if protocol in _SASL_PROTOCOLS:
        kwargs.update(
            {
                "sasl_mechanism": mechanism,
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )

    if protocol in _TLS_PROTOCOLS:
        kwargs["ssl_context"] = create_ssl_context(
            cafile=ssl_cafile,
            certfile=ssl_certfile,
            keyfile=ssl_keyfile,
        )

    return kwargs
