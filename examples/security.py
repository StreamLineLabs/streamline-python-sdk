"""Security example for Streamline Python SDK.

Demonstrates TLS and SASL authentication configuration.

Run with:
    # SASL/PLAIN
    SASL_USERNAME=admin SASL_PASSWORD=admin-secret python examples/security.py

    # TLS
    TLS_ENABLED=1 CA_PATH=certs/ca.pem python examples/security.py
"""

import asyncio
import os

from streamline_sdk import StreamlineClient


async def sasl_plain_example():
    """Connect with SASL/PLAIN authentication."""
    print("SASL/PLAIN Authentication")
    print("-" * 40)

    client = StreamlineClient(
        bootstrap_servers=os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092"),
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ.get("SASL_USERNAME", "admin"),
        sasl_plain_password=os.environ.get("SASL_PASSWORD", "admin-secret"),
    )

    async with client:
        topics = await client.admin.list_topics()
        print(f"  Connected with SASL/PLAIN. Topics: {topics}")

        result = await client.producer.send(
            "secure-topic",
            value=b"authenticated message",
        )
        print(f"  Produced to partition={result.partition} offset={result.offset}")

    print("  Disconnected.\n")


async def sasl_scram_example():
    """Connect with SASL/SCRAM-SHA-256 authentication."""
    print("SASL/SCRAM-SHA-256 Authentication")
    print("-" * 40)

    client = StreamlineClient(
        bootstrap_servers=os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092"),
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=os.environ.get("SASL_USERNAME", "admin"),
        sasl_plain_password=os.environ.get("SASL_PASSWORD", "admin-secret"),
    )

    async with client:
        topics = await client.admin.list_topics()
        print(f"  Connected with SCRAM-SHA-256. Topics: {topics}")

    print("  Disconnected.\n")


async def tls_example():
    """Connect with TLS encryption."""
    print("TLS Encrypted Connection")
    print("-" * 40)

    ca_path = os.environ.get("CA_PATH", "certs/ca.pem")
    client_cert = os.environ.get("CLIENT_CERT_PATH")
    client_key = os.environ.get("CLIENT_KEY_PATH")

    client = StreamlineClient(
        bootstrap_servers=os.environ.get("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093"),
        ssl_cafile=ca_path,
        ssl_certfile=client_cert,
        ssl_keyfile=client_key,
    )

    async with client:
        topics = await client.admin.list_topics()
        print(f"  Connected with TLS. Topics: {topics}")
        if client_cert:
            print("  Mutual TLS (client certificate) enabled.")

    print("  Disconnected.\n")


async def main():
    print("Streamline Security Examples")
    print("=" * 40)
    print()

    mode = os.environ.get("SECURITY_MODE", "sasl_plain")

    if mode == "sasl_plain" or os.environ.get("SASL_USERNAME"):
        await sasl_plain_example()

    if mode == "scram":
        await sasl_scram_example()

    if mode == "tls" or os.environ.get("TLS_ENABLED"):
        await tls_example()

    if mode not in ("sasl_plain", "scram", "tls"):
        # Default: show SASL/PLAIN
        await sasl_plain_example()

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
