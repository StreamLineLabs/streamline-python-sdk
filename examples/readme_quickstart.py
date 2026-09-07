from __future__ import annotations

import asyncio

from streamline_sdk import StreamlineClient


async def main() -> None:
    async with StreamlineClient("localhost:9092") as client:
        metadata = await client.producer.send(
            "events",
            key=b"user-42",
            value=b"Hello, Streamline!",
        )
        print(f"wrote partition={metadata.partition} offset={metadata.offset}")

        async with client.consumer(group_id="quickstart") as consumer:
            await consumer.subscribe(["events"])
            for message in await consumer.poll(timeout_ms=1_000):
                print(message.value)


if __name__ == "__main__":
    asyncio.run(main())
