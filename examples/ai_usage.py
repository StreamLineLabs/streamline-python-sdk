"""Streamline AI Module Usage Example.

Demonstrates vector embeddings, semantic search, anomaly detection,
and retrieval-augmented generation (RAG) capabilities.

Prerequisites:
    - Streamline server running with AI features enabled
    - pip install streamline-sdk

Run with:
    python examples/ai_usage.py
"""

import asyncio
import os

from streamline_sdk import StreamlineClient
from streamline_sdk.ai import AIClient


async def main():
    """Demonstrate AI module usage."""
    bootstrap = os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")
    http_url = os.environ.get("STREAMLINE_HTTP", "http://localhost:9094")

    ai = AIClient(http_url)

    async with StreamlineClient(bootstrap_servers=bootstrap) as client:
        # Ensure a topic exists for semantic search context
        try:
            from streamline_sdk import TopicConfig

            await client.admin.create_topic(
                TopicConfig(name="docs", num_partitions=1, replication_factor=1)
            )
        except Exception:
            pass  # topic may already exist

        # --- Vector Embeddings ---
        print("=== Vector Embeddings ===")
        texts = [
            "Streamline is a Kafka-compatible streaming platform",
            "Real-time data processing at the edge",
        ]
        result = await ai.embed(texts)
        print(f"Generated {len(result.embeddings)} embeddings")
        print(f"Dimensions: {len(result.embeddings[0])}")

        # --- Semantic Search ---
        print("\n=== Semantic Search ===")
        results = await ai.search("streaming performance", topic="docs", top_k=5)
        for r in results:
            print(f"  Score: {r.score:.3f} — {r.text[:80]}...")

        # --- Anomaly Detection ---
        print("\n=== Anomaly Detection ===")
        print("Monitoring 'metrics' topic for anomalies...")
        async for anomaly in ai.detect_anomalies("metrics", threshold=0.95):
            print(f"  Anomaly detected: {anomaly}")
            break  # Exit after first anomaly for demo

        # --- Retrieval-Augmented Generation ---
        print("\n=== RAG Query ===")
        answer = await ai.rag(
            query="How does Streamline handle backpressure?",
            context_topic="documentation",
        )
        print(f"Answer: {answer.text}")
        print(f"Sources: {len(answer.sources)} documents referenced")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
