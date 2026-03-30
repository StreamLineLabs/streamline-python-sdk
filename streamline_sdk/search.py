"""HTTP client for semantic-topic search (M2 P1, Experimental).

Wraps the broker's ``POST /api/v1/topics/{topic}/search`` endpoint.

Example:
    client = SearchClient("http://localhost:9094")
    hits = await client.search("logs", "payment failure", k=5)
    for h in hits:
        print(h.partition, h.offset, h.score)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Optional

try:
    import aiohttp

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover - import guard
    _HAS_AIOHTTP = False

from .exceptions import StreamlineError


class SearchError(StreamlineError):
    """Raised on search API failures."""


@dataclass
class SearchHit:
    partition: int
    offset: int
    score: float
    value: Optional[str] = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "SearchHit":
        return cls(
            partition=int(data.get("partition", 0)),
            offset=int(data.get("offset", 0)),
            score=float(data.get("score", 0.0)),
            value=data.get("value"),
        )


@dataclass
class SearchResult:
    hits: list[SearchHit]
    took_ms: int

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "SearchResult":
        return cls(
            hits=[SearchHit.from_json(h) for h in data.get("hits", [])],
            took_ms=int(data.get("took_ms", 0)),
        )


class SearchClient:
    """Async client for the semantic-search HTTP API.

    Args:
        http_url: Broker HTTP base URL (e.g. ``http://localhost:9094``).
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, http_url: str, *, timeout: float = 30.0) -> None:
        self.http_url = http_url.rstrip("/")
        self.timeout = timeout

    async def search(
        self,
        topic: str,
        query: str,
        *,
        k: int = 10,
        filter: Optional[dict[str, Any]] = None,
    ) -> SearchResult:
        if not topic:
            raise SearchError("topic must not be empty")
        if not query:
            raise SearchError("query must not be empty")
        if k <= 0 or k > 1000:
            raise SearchError("k must be in [1, 1000]")
        body: dict[str, Any] = {"query": query, "k": k}
        if filter is not None:
            body["filter"] = filter
        data = await self._post(f"/api/v1/topics/{topic}/search", body)
        return SearchResult.from_json(data)

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
            raise SearchError(
                f"search request to {path} failed (HTTP {status}): {body}"
            )


__all__ = ["SearchClient", "SearchHit", "SearchResult", "SearchError"]
