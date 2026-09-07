"""Schema Registry example for the Streamline Python SDK.

Demonstrates JSON schema registration and schema-aware produce/consume,
and compatibility checking.

Ensure a Streamline server is running at localhost:9092 with the
schema registry enabled on port 9094 before running:

    streamline --playground

Run with:
    python examples/schema_registry.py
"""

import asyncio
import json
import os

from streamline_sdk import StreamlineClient, TopicConfig
from streamline_sdk.schema_producer import SchemaConsumer, SchemaProducer
from streamline_sdk.serializers import (
    SchemaRegistryClient,
    SchemaRegistryConfig,
)

# JSON schema for a User record
USER_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
            "created_at": {"type": "string"},
        },
        "required": ["id", "name", "email", "created_at"],
    }
)

SUBJECT = "users-value"
TOPIC = "users"


async def main() -> None:
    """Demonstrate schema registry usage."""
    bootstrap_servers = os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.environ.get(
        "STREAMLINE_SCHEMA_REGISTRY_URL", "http://localhost:9094"
    )

    # === 1. Create a schema registry client ===
    print("Connecting to Schema Registry...")
    sr_config = SchemaRegistryConfig(url=schema_registry_url)
    schema_registry = SchemaRegistryClient(sr_config)

    async with StreamlineClient(bootstrap_servers=bootstrap_servers) as client:
        # Ensure the topic exists
        try:
            await client.admin.create_topic(
                TopicConfig(
                    name=TOPIC,
                    num_partitions=3,
                    replication_factor=1,
                )
            )
            print(f"Topic '{TOPIC}' created")
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # === 2. Register a JSON schema ===
        print("\n=== Registering Schema ===")
        schema_id = await schema_registry.register_schema(
            subject=SUBJECT,
            schema_str=USER_SCHEMA,
            schema_type="JSON",
        )
        print(f"Registered schema with id={schema_id} for subject={SUBJECT}")

        # Retrieve the schema back by id
        retrieved = await schema_registry.get_schema(schema_id)
        print(f"Retrieved schema: {retrieved}")

        # === 3. Check schema compatibility ===
        print("\n=== Checking Compatibility ===")
        compatible = await schema_registry.check_compatibility(
            subject=SUBJECT,
            schema_str=USER_SCHEMA,
            schema_type="JSON",
        )
        print(f"Schema compatible: {compatible}")

        # === 4. Produce messages with schema validation ===
        print("\n=== Producing Messages with Schema ===")
        schema_producer = SchemaProducer(
            producer=client.producer,
            schema_registry=schema_registry,
            subject=SUBJECT,
            schema=USER_SCHEMA,
            schema_type="JSON",
        )

        for i in range(5):
            user = {
                "id": i,
                "name": f"user-{i}",
                "email": f"user{i}@example.com",
                "created_at": "2025-01-15T10:00:00Z",
            }
            result = await schema_producer.send(
                TOPIC,
                value=user,
                key=f"user-{i}".encode(),
            )
            print(
                f"  Produced user-{i} to partition {result.partition} "
                f"at offset {result.offset}"
            )

        # === 5. Consume and deserialize with schema ===
        print("\n=== Consuming Messages with Schema ===")
        async with client.consumer(group_id="python-schema-group") as consumer:
            await consumer.subscribe([TOPIC])
            await consumer.seek_to_beginning()

            schema_consumer = SchemaConsumer(
                consumer=consumer,
                schema_registry=schema_registry,
            )
            messages = await schema_consumer.poll(
                timeout_ms=5000,
                max_records=10,
            )
            print(f"  Received {len(messages)} messages:")

            for msg in messages:
                print(
                    f"    Partition: {msg.partition}, "
                    f"Offset: {msg.offset}, "
                    f"Key: {msg.key!r}, "
                    f"Schema: {msg.schema_id}, "
                    f"User: {msg.value}"
                )

        print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
