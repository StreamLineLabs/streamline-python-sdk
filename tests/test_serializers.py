"""Tests for the serializers module."""

import json

import pytest

from streamline_sdk.serializers import (
    AvroSerializer,
    JsonSchemaSerializer,
    SchemaRegistryClient,
    SchemaRegistryConfig,
)
from streamline_sdk.exceptions import StreamlineError


class TestSchemaRegistryConfig:
    """Test the SchemaRegistryConfig dataclass."""

    def test_defaults(self):
        config = SchemaRegistryConfig()
        assert config.url == "http://localhost:9094"
        assert config.auto_register is True
        assert config.cache_capacity == 256

    def test_custom_values(self):
        config = SchemaRegistryConfig(
            url="http://registry:8081",
            auto_register=False,
            cache_capacity=1024,
        )
        assert config.url == "http://registry:8081"
        assert config.auto_register is False
        assert config.cache_capacity == 1024


class TestSchemaRegistryClient:
    """Test the SchemaRegistryClient construction and caching."""

    def test_construction(self):
        config = SchemaRegistryConfig(url="http://localhost:9094")
        client = SchemaRegistryClient(config)
        assert client.config.url == "http://localhost:9094"
        assert client._cache == {}

    def test_cache_starts_empty(self):
        config = SchemaRegistryConfig()
        client = SchemaRegistryClient(config)
        assert len(client._cache) == 0


class TestAvroSerializer:
    """Test the AvroSerializer construction and configuration."""

    def test_default_construction(self):
        serializer = AvroSerializer()
        assert serializer.schema_str is None
        assert serializer.auto_register is True
        assert serializer._schema_id is None

    def test_construction_with_schema(self):
        schema = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'
        serializer = AvroSerializer(
            schema_registry_url="http://registry:9094",
            schema_str=schema,
            auto_register=False,
        )
        assert serializer.schema_str == schema
        assert serializer.auto_register is False
        assert serializer.registry.config.url == "http://registry:9094"

    def test_schema_id_initially_none(self):
        serializer = AvroSerializer(schema_str='{"type":"string"}')
        assert serializer._schema_id is None


class TestJsonSchemaSerializer:
    """Test the JsonSchemaSerializer construction and validation."""

    def test_default_construction(self):
        serializer = JsonSchemaSerializer()
        assert serializer.schema_str is None
        assert serializer.auto_register is True
        assert serializer.validate is True
        assert serializer._schema is None
        assert serializer._schema_id is None

    def test_construction_with_schema(self):
        schema_str = '{"type":"object","properties":{"name":{"type":"string"}}}'
        serializer = JsonSchemaSerializer(schema_str=schema_str)
        assert serializer.schema_str == schema_str
        assert serializer._schema is not None
        assert serializer._schema["type"] == "object"

    def test_schema_parsed_on_construction(self):
        schema_str = '{"type":"object","required":["id"]}'
        serializer = JsonSchemaSerializer(schema_str=schema_str)
        assert "required" in serializer._schema
        assert "id" in serializer._schema["required"]

    def test_validate_disabled(self):
        serializer = JsonSchemaSerializer(
            schema_str='{"type":"string"}',
            validate=False,
        )
        assert serializer.validate is False

    @pytest.mark.asyncio
    async def test_serialize_without_schema_id(self):
        """Serialization without schema registration returns plain JSON."""
        serializer = JsonSchemaSerializer(validate=False)
        serializer.auto_register = False
        result = await serializer.serialize("test-topic", {"key": "value"})
        decoded = json.loads(result)
        assert decoded == {"key": "value"}

    @pytest.mark.asyncio
    async def test_serialize_with_preassigned_schema_id(self):
        """Serialization with a preassigned schema ID includes wire format prefix."""
        serializer = JsonSchemaSerializer(validate=False)
        serializer.auto_register = False
        serializer._schema_id = 42

        result = await serializer.serialize("test-topic", {"key": "value"})

        # Confluent wire format: 0x00 + 4-byte big-endian schema ID
        assert result[0:1] == b"\x00"
        schema_id = int.from_bytes(result[1:5], "big")
        assert schema_id == 42
        payload = json.loads(result[5:])
        assert payload == {"key": "value"}

    @pytest.mark.asyncio
    async def test_serialize_validation_passes(self):
        """Valid data passes schema validation."""
        schema = '{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}'
        serializer = JsonSchemaSerializer(schema_str=schema)
        serializer.auto_register = False

        try:
            import jsonschema  # noqa: F401
            result = await serializer.serialize("test", {"name": "Alice"})
            assert b"Alice" in result
        except ImportError:
            pytest.skip("jsonschema not installed")

    @pytest.mark.asyncio
    async def test_serialize_validation_fails(self):
        """Invalid data raises StreamlineError when validation is enabled."""
        schema = '{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}'
        serializer = JsonSchemaSerializer(schema_str=schema)
        serializer.auto_register = False

        try:
            import jsonschema  # noqa: F401
            with pytest.raises(StreamlineError, match="Schema validation failed"):
                await serializer.serialize("test", {"wrong_field": 123})
        except ImportError:
            pytest.skip("jsonschema not installed")

    @pytest.mark.asyncio
    async def test_wire_format_prefix_structure(self):
        """Wire format prefix is exactly 5 bytes: magic + 4-byte ID."""
        serializer = JsonSchemaSerializer(validate=False)
        serializer.auto_register = False
        serializer._schema_id = 256

        result = await serializer.serialize("t", {"x": 1})

        assert result[0:1] == b"\x00"  # magic byte
        assert len(result[1:5]) == 4  # 4-byte schema ID
        assert int.from_bytes(result[1:5], "big") == 256

    @pytest.mark.asyncio
    async def test_large_schema_id(self):
        """Schema IDs up to 2^31-1 are correctly encoded."""
        serializer = JsonSchemaSerializer(validate=False)
        serializer.auto_register = False
        serializer._schema_id = 2_147_483_647  # max signed 32-bit

        result = await serializer.serialize("t", {})
        schema_id = int.from_bytes(result[1:5], "big")
        assert schema_id == 2_147_483_647
