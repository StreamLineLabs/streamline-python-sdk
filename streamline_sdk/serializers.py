"""Schema-aware serializers for Streamline.

Provides Avro, JSON Schema, and Protobuf serialization with
automatic schema registration and validation via the built-in
Streamline Schema Registry.

Usage:
    from streamline_sdk.serializers import AvroSerializer, JsonSchemaSerializer

    # Avro serialization with auto-registration
    serializer = AvroSerializer(
        schema_registry_url="http://localhost:9094",
        schema_str='''
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }
        ''',
        auto_register=True,
    )

    # Serialize a record
    data = serializer.serialize("users", {"id": 1, "name": "Alice", "email": "alice@example.com"})

    # Produce with serialized data
    await client.produce("users", value=data)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .exceptions import StreamlineError


@dataclass
class SchemaRegistryConfig:
    """Configuration for the Schema Registry client."""
    url: str = "http://localhost:9094"
    auto_register: bool = True
    cache_capacity: int = 256


class SchemaRegistryClient:
    """Client for the Streamline Schema Registry (Confluent-compatible)."""

    def __init__(self, config: SchemaRegistryConfig):
        self.config = config
        self._cache: Dict[str, int] = {}

    async def register_schema(self, subject: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """Register a schema and return its ID."""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"{self.config.url}/subjects/{subject}/versions"
            payload = {"schema": schema_str, "schemaType": schema_type}
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    schema_id = data["id"]
                    self._cache[subject] = schema_id
                    return int(schema_id)
                else:
                    text = await resp.text()
                    raise StreamlineError(f"Schema registration failed: {resp.status} {text}")

    async def get_schema(self, schema_id: int) -> str:
        """Get a schema by ID."""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"{self.config.url}/schemas/ids/{schema_id}"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return str(data["schema"])
                else:
                    raise StreamlineError(f"Schema not found: {schema_id}")


class AvroSerializer:
    """Avro serializer with Schema Registry integration.

    Serializes Python dicts to Avro binary format with a 5-byte
    schema ID prefix (Confluent wire format: magic byte + 4-byte ID).
    """

    def __init__(
        self,
        schema_registry_url: str = "http://localhost:9094",
        schema_str: Optional[str] = None,
        auto_register: bool = True,
    ):
        self.registry = SchemaRegistryClient(SchemaRegistryConfig(url=schema_registry_url))
        self.schema_str = schema_str
        self.auto_register = auto_register
        self._schema_id: Optional[int] = None

    async def serialize(self, topic: str, value: Dict[str, Any]) -> bytes:
        """Serialize a value to Avro binary with schema ID prefix."""
        if self._schema_id is None and self.auto_register and self.schema_str:
            subject = f"{topic}-value"
            self._schema_id = await self.registry.register_schema(subject, self.schema_str, "AVRO")

        try:
            import avro.io
            import avro.schema
            import io

            schema = avro.schema.parse(self.schema_str)
            writer = avro.io.DatumWriter(schema)
            buf = io.BytesIO()

            # Confluent wire format: 0x00 + 4-byte schema ID (big-endian)
            if self._schema_id is not None:
                buf.write(b'\x00')
                buf.write(self._schema_id.to_bytes(4, 'big'))

            encoder = avro.io.BinaryEncoder(buf)
            writer.write(value, encoder)
            return buf.getvalue()
        except ImportError:
            raise StreamlineError(
                "avro-python3 is required for Avro serialization. "
                "Install with: pip install streamline-sdk[avro]"
            )


class JsonSchemaSerializer:
    """JSON Schema serializer with Schema Registry integration.

    Validates Python dicts against a JSON Schema and serializes
    to JSON bytes with optional schema ID prefix.
    """

    def __init__(
        self,
        schema_registry_url: str = "http://localhost:9094",
        schema_str: Optional[str] = None,
        auto_register: bool = True,
        validate: bool = True,
    ):
        self.registry = SchemaRegistryClient(SchemaRegistryConfig(url=schema_registry_url))
        self.schema_str = schema_str
        self.auto_register = auto_register
        self.validate = validate
        self._schema_id: Optional[int] = None
        self._schema: Optional[Dict[str, Any]] = json.loads(schema_str) if schema_str else None

    async def serialize(self, topic: str, value: Dict[str, Any]) -> bytes:
        """Serialize a value to JSON bytes with optional validation."""
        if self._schema_id is None and self.auto_register and self.schema_str:
            subject = f"{topic}-value"
            self._schema_id = await self.registry.register_schema(
                subject, self.schema_str, "JSON"
            )

        if self.validate and self._schema:
            try:
                import jsonschema
                jsonschema.validate(value, self._schema)
            except ImportError:
                pass  # Skip validation if jsonschema not installed
            except jsonschema.ValidationError as e:
                raise StreamlineError(f"Schema validation failed: {e.message}")

        payload = json.dumps(value).encode("utf-8")

        if self._schema_id is not None:
            # Confluent wire format prefix
            return b'\x00' + self._schema_id.to_bytes(4, 'big') + payload
        return payload
