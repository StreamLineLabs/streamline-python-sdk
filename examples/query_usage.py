"""
Streamline SQL Query Example

Demonstrates using Streamline's embedded analytics engine (DuckDB)
to run SQL queries on streaming data.

Prerequisites:
    - Streamline server running
    - pip install streamline-sdk

Run:
    python query_usage.py
"""
import asyncio
import os
from streamline_sdk import StreamlineClient
from streamline_sdk.query import QueryClient


async def main():
    bootstrap = os.getenv("STREAMLINE_BOOTSTRAP", "localhost:9092")
    http_url = os.getenv("STREAMLINE_HTTP", "http://localhost:9094")

    # Produce sample data
    async with StreamlineClient(bootstrap) as client:
        await client.create_topic("events", partitions=1)
        for i in range(10):
            await client.produce("events", {
                "user": f"user-{i}",
                "action": "click",
                "value": i * 10,
            })
        print("Produced 10 events")

    # Query the data using SQL
    query_client = QueryClient(http_url)

    # Simple SELECT
    print("\n--- All events (limit 5) ---")
    result = await query_client.query(
        "SELECT * FROM topic('events') LIMIT 5"
    )
    print(f"Columns: {[c['name'] for c in result.columns]}")
    print(f"Rows: {result.rows_returned}")
    for row in result.rows:
        print(f"  {row}")

    # Aggregation query
    print("\n--- Count by action ---")
    result = await query_client.query(
        "SELECT action, COUNT(*) as cnt FROM topic('events') GROUP BY action"
    )
    for row in result.rows:
        print(f"  {row}")

    # Query with options
    print("\n--- With custom timeout and limit ---")
    result = await query_client.query(
        "SELECT * FROM topic('events') ORDER BY offset DESC",
        timeout_ms=5000,
        max_rows=3,
    )
    print(f"Returned {result.rows_returned} rows (truncated: {result.truncated})")
    print(f"Execution time: {result.execution_time_ms}ms")

    # Explain query plan
    print("\n--- Query plan ---")
    plan = await query_client.explain(
        "SELECT * FROM topic('events') WHERE value > 50"
    )
    print(plan)


if __name__ == "__main__":
    asyncio.run(main())
