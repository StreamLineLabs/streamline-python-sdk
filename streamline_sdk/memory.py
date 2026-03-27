"""HTTP client for the Agent Memory Fabric (M1 P1, Experimental).

Wraps the broker's ``/api/v1/memory/{remember,recall}`` endpoints.

Example:
    client = MemoryClient("http://localhost:9094")
    written = await client.remember(
        agent_id="agent-1",
        kind="fact",
        content="vault path is prod/api",
        importance=0.9,
    )
    hits = await client.recall(agent_id="agent-1", query="vault", k=5)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import aiohttp

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover - import guard
    _HAS_AIOHTTP = False

from .exceptions import StreamlineError


class MemoryError(StreamlineError):
    """Raised on memory API failures."""


# Allowed write kinds. Mirror Rust enum WriteKind.
_VALID_KINDS = {"observation", "fact", "procedure"}


@dataclass
class WrittenEntry:
    topic: str
    offset: int

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "WrittenEntry":
        return cls(topic=data["topic"], offset=int(data["offset"]))


@dataclass
class RecalledMemory:
    tier: str
    topic: str
    offset: int
    content: str
    score: float

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "RecalledMemory":
        return cls(
            tier=data.get("tier", ""),
            topic=data.get("topic", ""),
            offset=int(data.get("offset", 0)),
            content=data.get("content", ""),
            score=float(data.get("score", 0.0)),
        )


class MemoryClient:
    """Async client for the Agent Memory HTTP API.

    Args:
        http_url: Broker HTTP base URL (e.g. ``http://localhost:9094``).
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, http_url: str, *, timeout: float = 30.0) -> None:
        self.http_url = http_url.rstrip("/")
        self.timeout = timeout

    async def remember(
        self,
        *,
        agent_id: str,
        kind: str,
        content: str,
        importance: float = 0.5,
        tags: Optional[list[str]] = None,
        skill: Optional[str] = None,
    ) -> list[WrittenEntry]:
        """Write a memory for an agent.

        Args:
            agent_id: Logical agent identifier.
            kind: ``observation``, ``fact``, or ``procedure``.
            content: Memory content (free text).
            importance: 0.0-1.0; below 0.3 facts skip the semantic mirror.
            tags: Optional tags for filtering.
            skill: Required when ``kind="procedure"`` — names the skill.

        Returns:
            One :class:`WrittenEntry` per tier the write fanned out to.
        """
        if not agent_id:
            raise MemoryError("agent_id must not be empty")
        if not content:
            raise MemoryError("content must not be empty")
        kind_lower = kind.lower()
        if kind_lower not in _VALID_KINDS:
            raise MemoryError(
                f"kind must be one of {sorted(_VALID_KINDS)}, got {kind!r}"
            )
        if not 0.0 <= importance <= 1.0:
            raise MemoryError("importance must be in [0.0, 1.0]")
        if kind_lower == "procedure" and not skill:
            raise MemoryError("skill is required for kind='procedure'")

        body: dict[str, Any] = {
            "agent_id": agent_id,
            "kind": kind_lower,
            "content": content,
            "importance": importance,
            "tags": tags or [],
        }
        if kind_lower == "procedure":
            body["skill"] = skill

        data = await self._post("/api/v1/memory/remember", body)
        items = data.get("written", []) if isinstance(data, dict) else []
        return [WrittenEntry.from_json(e) for e in items]

    async def recall(
        self,
        *,
        agent_id: str,
        query: str,
        k: int = 10,
        min_hits: int = 0,
    ) -> list[RecalledMemory]:
        """Recall top-k memories for an agent matching ``query``."""
        if not agent_id:
            raise MemoryError("agent_id must not be empty")
        if not query:
            raise MemoryError("query must not be empty")
        if k <= 0 or k > 1000:
            raise MemoryError("k must be in [1, 1000]")

        body = {
            "agent_id": agent_id,
            "query": query,
            "k": k,
            "min_hits": min_hits,
        }
        data = await self._post("/api/v1/memory/recall", body)
        hits = data.get("hits", []) if isinstance(data, dict) else []
        return [RecalledMemory.from_json(h) for h in hits]

    # ------------------------------------------------------------------
    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.http_url}{path}"
        if _HAS_AIOHTTP:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as resp:
                    text = await resp.text()
                    self._check(resp.status, text, path)
                    return json.loads(text) if text else {}
        else:
            import urllib.error
            import urllib.request

            data = json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            def _sync() -> dict[str, Any]:
                try:
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        text = resp.read().decode("utf-8")
                        self._check(resp.status, text, path)
                        return json.loads(text) if text else {}
                except urllib.error.HTTPError as e:
                    text = e.read().decode("utf-8", errors="replace")
                    self._check(e.code, text, path)
                    return {}

            return await asyncio.to_thread(_sync)

    @staticmethod
    def _check(status: int, body: str, path: str) -> None:
        if status >= 400:
            raise MemoryError(
                f"memory request to {path} failed (HTTP {status}): {body}"
            )


__all__ = ["MemoryClient", "RecalledMemory", "WrittenEntry", "MemoryError"]
