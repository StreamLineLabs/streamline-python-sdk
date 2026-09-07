"""Streamline Agent Memory Example.

Demonstrates the agent memory API (remember, recall) for building
agents with persistent, semantically searchable memory. Shows both
single-agent memory and shared team memory.

Prerequisites:
    - Streamline server running with memory features enabled
    - pip install streamline-sdk

Run with:
    python examples/agent_memory/memory_demo.py
"""

from __future__ import annotations

import asyncio
import os

from streamline_sdk import MemoryClient


async def single_agent_memory(memory: MemoryClient) -> None:
    """Demonstrate remember/recall for a single agent."""
    print("=== Single Agent Memory ===")

    # Store architectural decisions
    await memory.remember(
        agent_id="demo-agent",
        content="We chose PostgreSQL for its JSONB support and mature ecosystem",
        kind="fact",
        importance=0.8,
        tags=["architecture", "database"],
    )

    await memory.remember(
        agent_id="demo-agent",
        content="Redis is used as a caching layer with a 15-minute TTL",
        kind="fact",
        importance=0.7,
        tags=["architecture", "caching"],
    )

    await memory.remember(
        agent_id="demo-agent",
        content="User requested dark mode support in the dashboard",
        kind="observation",
        importance=0.6,
        tags=["ui", "user-request"],
    )

    print("Stored 3 memories\n")

    # Recall by semantic similarity
    print("--- Recall: 'why did we pick our database?' ---")
    results = await memory.recall(
        agent_id="demo-agent",
        query="why did we pick our database?",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")

    print("\n--- Recall: 'caching strategy' ---")
    results = await memory.recall(
        agent_id="demo-agent",
        query="caching strategy",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")


async def shared_team_memory(memory: MemoryClient) -> None:
    """Demonstrate shared memory by writing under a common agent id."""
    print("\n=== Shared Team Memory ===")

    # Both agents write into the same logical memory space.
    await memory.remember(
        agent_id="team-shared",
        content="Deploy target is Kubernetes on AWS EKS",
        kind="fact",
        importance=0.9,
        tags=["infra", "deployment", "agent-a"],
    )
    print("Agent A stored deployment decision")

    await memory.remember(
        agent_id="team-shared",
        content="CI/CD pipeline uses GitHub Actions with OIDC auth to AWS",
        kind="fact",
        importance=0.8,
        tags=["infra", "ci-cd", "agent-b"],
    )
    print("Agent B stored CI/CD context")

    # A third agent recalls from the shared memory space.
    print("\n--- Agent C recalls 'deployment infrastructure' from shared memory ---")
    results = await memory.recall(
        agent_id="team-shared",
        query="deployment infrastructure",
        k=5,
    )
    for hit in results:
        print(f"  [{hit.tier}] score={hit.score:.2f}: {hit.content}")


async def main() -> None:
    """Run agent memory demos."""
    http_url = os.environ.get("STREAMLINE_HTTP_URL", "http://localhost:9094")

    memory = MemoryClient(http_url)
    await single_agent_memory(memory)
    await shared_team_memory(memory)

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
