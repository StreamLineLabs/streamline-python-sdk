"""Basic usage example for Streamline Python SDK."""

import asyncio
import os

from streamline_sdk import StreamlineClient, TopicConfig


async def main() -> None:
    """Demonstrate basic SDK usage."""
    print("Connecting to Streamline...")

    async with StreamlineClient(
        bootstrap_servers=os.environ.get(
            "STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092"
        )
    ) as client:
        # Create a topic
        print("Creating topic...")
        try:
            await client.admin.create_topic(
                TopicConfig(
                    name="example-topic",
                    num_partitions=3,
                    replication_factor=1,
                )
            )
            print("Topic created!")
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # Produce messages
        print("\nProducing messages...")
        for i in range(10):
            result = await client.producer.send(
                "example-topic",
                value=f"Hello, Streamline! Message {i}".encode(),
                key=f"key-{i}".encode(),
            )
            print(
                f"  Sent message {i} to partition {result.partition} "
                f"at offset {result.offset}"
            )

        # Produce with headers
        result = await client.producer.send(
            "example-topic",
            value=b"Message with headers",
            headers={
                "trace-id": b"abc123",
                "content-type": b"text/plain",
            },
        )
        print(f"  Sent message with headers to partition {result.partition}")

        # Consume messages
        print("\nConsuming messages...")
        async with client.consumer(group_id="example-group") as consumer:
            await consumer.subscribe(["example-topic"])

            # Seek to beginning to see all messages
            await consumer.seek_to_beginning()

            # Poll a batch of messages
            messages = await consumer.poll(timeout_ms=5000, max_records=20)
            print(f"  Received {len(messages)} messages:")

            for msg in messages:
                print(
                    f"    Topic: {msg.topic}, "
                    f"Partition: {msg.partition}, "
                    f"Offset: {msg.offset}, "
                    f"Key: {msg.key!r}, "
                    f"Value: {msg.value!r}"
                )
                if msg.headers:
                    print(f"    Headers: {msg.headers}")

        # Admin operations
        print("\nAdmin operations...")
        topics = await client.admin.list_topics()
        print(f"  Topics: {topics}")

        info = await client.admin.describe_topic("example-topic")
        print(
            f"  Topic info: {info.name}, "
            f"partitions={info.partitions}, "
            f"replication_factor={info.replication_factor}"
        )

        print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
