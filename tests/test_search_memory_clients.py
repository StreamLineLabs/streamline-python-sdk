"""Unit tests for the search & memory HTTP clients.

Patches the SDK's ``_post`` async method so no broker is required.
"""

from __future__ import annotations

from typing import Any

import pytest

from streamline_sdk import (
    MemoryClient,
    MemoryError,
    RecalledMemory,
    SearchClient,
    SearchError,
    SearchHit,
    SearchResult,
    WrittenEntry,
)


# --------------------------------------------------------------------- helpers


def _patch_search(monkeypatch: pytest.MonkeyPatch, handler):
    async def _post(self, path: str, body: dict[str, Any]):
        return await handler(path, body)

    monkeypatch.setattr(SearchClient, "_post", _post)


def _patch_memory(monkeypatch: pytest.MonkeyPatch, handler):
    async def _post(self, path: str, body: dict[str, Any]):
        return await handler(path, body)

    monkeypatch.setattr(MemoryClient, "_post", _post)


# --------------------------------------------------------------------- search


@pytest.mark.asyncio
async def test_search_returns_hits(monkeypatch):
    captured: dict[str, Any] = {}

    async def handler(path, body):
        captured["path"] = path
        captured["body"] = body
        return {
            "hits": [
                {"partition": 0, "offset": 12, "score": 0.91, "value": "x"},
                {"partition": 1, "offset": 5, "score": 0.42},
            ],
            "took_ms": 7,
        }

    _patch_search(monkeypatch, handler)
    client = SearchClient("http://localhost:9094")
    result = await client.search("logs", "payment failure", k=5)

    assert captured["path"] == "/api/v1/topics/logs/search"
    assert captured["body"] == {"query": "payment failure", "k": 5}
    assert isinstance(result, SearchResult)
    assert result.took_ms == 7
    assert len(result.hits) == 2
    assert isinstance(result.hits[0], SearchHit)
    assert result.hits[0].score == pytest.approx(0.91)
    assert result.hits[0].value == "x"


@pytest.mark.asyncio
async def test_search_passes_filter(monkeypatch):
    captured: dict[str, Any] = {}

    async def handler(path, body):
        captured["body"] = body
        return {"hits": [], "took_ms": 1}

    _patch_search(monkeypatch, handler)
    client = SearchClient("http://localhost:9094")
    await client.search("t", "q", k=3, filter={"env": "prod"})
    assert captured["body"]["filter"] == {"env": "prod"}


@pytest.mark.asyncio
async def test_search_rejects_empty_query():
    client = SearchClient("http://localhost:9094")
    with pytest.raises(SearchError):
        await client.search("logs", "")


@pytest.mark.asyncio
async def test_search_rejects_bad_k():
    client = SearchClient("http://localhost:9094")
    with pytest.raises(SearchError):
        await client.search("logs", "q", k=0)
    with pytest.raises(SearchError):
        await client.search("logs", "q", k=2000)


@pytest.mark.asyncio
async def test_search_rejects_empty_topic():
    client = SearchClient("http://localhost:9094")
    with pytest.raises(SearchError):
        await client.search("", "q")


# --------------------------------------------------------------------- memory


@pytest.mark.asyncio
async def test_memory_remember_fact(monkeypatch):
    captured: dict[str, Any] = {}

    async def handler(path, body):
        captured["path"] = path
        captured["body"] = body
        return {
            "written": [
                {"topic": "__mem.alice.episodic", "offset": 0},
                {"topic": "__mem.alice.semantic", "offset": 0},
            ]
        }

    _patch_memory(monkeypatch, handler)
    client = MemoryClient("http://localhost:9094")
    written = await client.remember(
        agent_id="alice",
        kind="fact",
        content="vault path is prod/api",
        importance=0.9,
        tags=["secrets"],
    )
    assert captured["path"] == "/api/v1/memory/remember"
    assert captured["body"]["agent_id"] == "alice"
    assert captured["body"]["kind"] == "fact"
    assert captured["body"]["importance"] == pytest.approx(0.9)
    assert captured["body"]["tags"] == ["secrets"]
    assert len(written) == 2
    assert isinstance(written[0], WrittenEntry)


@pytest.mark.asyncio
async def test_memory_remember_procedure_requires_skill(monkeypatch):
    _patch_memory(monkeypatch, lambda p, b: None)  # never called
    client = MemoryClient("http://localhost:9094")
    with pytest.raises(MemoryError):
        await client.remember(agent_id="a", kind="procedure", content="step")


@pytest.mark.asyncio
async def test_memory_remember_procedure_with_skill(monkeypatch):
    captured: dict[str, Any] = {}

    async def handler(path, body):
        captured["body"] = body
        return {"written": [{"topic": "__mem.a.procedural", "offset": 0}]}

    _patch_memory(monkeypatch, handler)
    client = MemoryClient("http://localhost:9094")
    await client.remember(
        agent_id="a", kind="procedure", content="how to deploy", skill="deploy"
    )
    assert captured["body"]["skill"] == "deploy"


@pytest.mark.asyncio
async def test_memory_recall(monkeypatch):
    async def handler(path, body):
        assert path == "/api/v1/memory/recall"
        assert body == {
            "agent_id": "alice",
            "query": "vault",
            "k": 3,
            "min_hits": 0,
        }
        return {
            "hits": [
                {
                    "tier": "semantic",
                    "topic": "__mem.alice.semantic",
                    "offset": 7,
                    "content": "vault path is prod/api",
                    "score": 0.82,
                }
            ]
        }

    _patch_memory(monkeypatch, handler)
    client = MemoryClient("http://localhost:9094")
    hits = await client.recall(agent_id="alice", query="vault", k=3)
    assert len(hits) == 1
    assert isinstance(hits[0], RecalledMemory)
    assert hits[0].tier == "semantic"
    assert hits[0].score == pytest.approx(0.82)


@pytest.mark.asyncio
async def test_memory_validation():
    client = MemoryClient("http://localhost:9094")
    with pytest.raises(MemoryError):
        await client.remember(agent_id="", kind="fact", content="x")
    with pytest.raises(MemoryError):
        await client.remember(agent_id="a", kind="bogus", content="x")
    with pytest.raises(MemoryError):
        await client.remember(agent_id="a", kind="fact", content="x", importance=2.0)
    with pytest.raises(MemoryError):
        await client.recall(agent_id="a", query="", k=5)
    with pytest.raises(MemoryError):
        await client.recall(agent_id="a", query="q", k=0)
