"""Streamline Agent Memory Example.

Demonstrates the memory MCP tools (remember, recall) for building
agents with persistent, semantically searchable memory. Shows both
single-agent memory and multi-agent shared memory.

Prerequisites:
    - Streamline server running with memory features enabled
    - pip install streamline-sdk

Run with:
    python examples/agent_memory/memory_demo.py
"""

import asyncio
import os

from streamline_sdk import StreamlineClient


async def single_agent_memory(client: StreamlineClient) -> None:
    """Demonstrate remember/recall for a single agent."""
    print("=== Single Agent Memory ===")

    # Store architectural decisions
    await client.memory_remember(
        agent_id="demo-agent",
        content="We chose PostgreSQL for its JSONB support and mature ecosystem",
        kind="fact",
        importance=0.8,
        tags=["architecture", "database"],
    )

    await client.memory_remember(
        agent_id="demo-agent",
        content="Redis is used as a caching layer with a 15-minute TTL",
        kind="fact",
        importance=0.7,
        tags=["architecture", "caching"],
    )

    await client.memory_remember(
        agent_id="demo-agent",
        content="User requested dark mode support in the dashboard",
        kind="preference",
        importance=0.6,
        tags=["ui", "user-request"],
    )

    print("Stored 3 memories\n")

    # Recall by semantic similarity
    print("--- Recall: 'why did we pick our database?' ---")
    results = await client.memory_recall(
        agent_id="demo-agent",
        query="why did we pick our database?",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")

    print("\n--- Recall: 'caching strategy' ---")
    results = await client.memory_recall(
        agent_id="demo-agent",
        query="caching strategy",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")


async def multi_agent_shared_memory(client: StreamlineClient) -> None:
    """Demonstrate shared memory between multiple agents."""
    print("\n=== Multi-Agent Shared Memory ===")

    # Agent A stores a decision
    await client.memory_remember(
        agent_id="agent-a",
        namespace="team-shared",
        content="Deploy target is Kubernetes on AWS EKS",
        kind="fact",
        importance=0.9,
        tags=["infra", "deployment"],
    )
    print("Agent A stored deployment decision")

    # Agent B stores related context
    await client.memory_remember(
        agent_id="agent-b",
        namespace="team-shared",
        content="CI/CD pipeline uses GitHub Actions with OIDC auth to AWS",
        kind="fact",
        importance=0.8,
        tags=["infra", "ci-cd"],
    )
    print("Agent B stored CI/CD context")

    # Agent C recalls shared memories from the team namespace
    print("\n--- Agent C recalls 'deployment infrastructure' from shared namespace ---")
    results = await client.memory_recall(
        agent_id="agent-c",
        namespace="team-shared",
        query="deployment infrastructure",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")


async def main() -> None:
    """Run agent memory demos."""
    bootstrap = os.environ.get("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")
    http_url = os.environ.get("STREAMLINE_HTTP", "http://localhost:9094")

    async with StreamlineClient(
        bootstrap_servers=bootstrap, http_endpoint=http_url
    ) as client:
        await single_agent_memory(client)
        await multi_agent_shared_memory(client)

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
