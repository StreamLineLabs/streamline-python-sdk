from __future__ import annotations

import asyncio

from streamline_sdk import StreamlineClient


async def main() -> None:
    async with StreamlineClient("localhost:9092") as client:
        producer = client.producer
        await producer.begin_transaction()
        try:
            await producer.send("orders", key=b"k1", value=b"v1")
            await producer.send("orders", key=b"k2", value=b"v2")
            await producer.commit_transaction()
        except Exception:
            # commit_transaction() exits buffering mode *before* replaying the
            # buffered sends (see its docstring), so if it fails partway
            # through that replay, the transaction is already over. Calling
            # abort_transaction() unconditionally here would raise
            # "RuntimeError: No transaction in progress" and mask the real
            # commit failure. Guard with in_transaction so we only abort
            # when buffering mode is still active (e.g. begin_transaction()
            # succeeded but a send() before commit raised).
            if producer.in_transaction:
                await producer.abort_transaction()
            raise


if __name__ == "__main__":
    asyncio.run(main())
