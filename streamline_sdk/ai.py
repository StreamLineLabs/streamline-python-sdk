"""AI-native streaming capabilities for Streamline.

Provides vector embeddings, semantic search, anomaly detection,
and RAG (Retrieval-Augmented Generation) through the Streamline AI API.
"""

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


@dataclass
class EmbeddingResult:
    """Result of an embedding operation."""
    vectors: list[list[float]]
    model: str
    usage: dict[str, int] = field(default_factory=dict)


@dataclass
class SearchResult:
    """A single semantic search result."""
    score: float
    offset: int
    value: Any
    topic: str = ""


@dataclass
class AnomalyAlert:
    """An anomaly detection alert."""
    field: str
    value: float
    z_score: float
    timestamp: int = 0
    topic: str = ""


@dataclass
class RAGResponse:
    """Response from a RAG query."""
    answer: str
    sources: list[dict[str, Any]] = field(default_factory=list)
    model: str = ""


class AIClient:
    """AI client for Streamline's AI-native streaming capabilities.

    Example:
        ai = AIClient("http://localhost:9094")

        # Embed text
        result = await ai.embed(["hello world", "streaming data"])

        # Semantic search
        results = await ai.search("user login events", topic="events", top_k=10)

        # Anomaly detection
        async for alert in ai.detect_anomalies("metrics"):
            print(f"Anomaly: {alert.field} = {alert.value}")

        # RAG
        answer = await ai.rag("What happened?", context_topic="incidents")
    """

    def __init__(self, base_url: str = "http://localhost:9094"):
        self.base_url = base_url.rstrip("/")

    async def embed(
        self,
        texts: list[str],
        model: str = "default",
    ) -> EmbeddingResult:
        """Generate vector embeddings for text inputs."""
        data = await self._post("/api/v1/ai/embed", {"texts": texts, "model": model})
        return EmbeddingResult(
            vectors=data.get("vectors", []),
            model=data.get("model", model),
            usage=data.get("usage", {}),
        )

    async def search(
        self,
        query: str,
        topic: str,
        top_k: int = 10,
    ) -> list[SearchResult]:
        """Perform semantic search across a topic."""
        data = await self._post("/api/v1/ai/search", {
            "query": query,
            "topic": topic,
            "top_k": top_k,
        })
        return [
            SearchResult(
                score=r.get("score", 0),
                offset=r.get("offset", 0),
                value=r.get("value"),
                topic=topic,
            )
            for r in data.get("results", [])
        ]

    async def detect_anomalies(
        self,
        topic: str,
        threshold: float = 2.0,
        window_size: int = 100,
    ) -> AsyncIterator[AnomalyAlert]:
        """Stream anomaly detection alerts (via SSE)."""
        if not HAS_AIOHTTP:
            raise ImportError("aiohttp required for streaming anomaly detection")

        payload = {
            "topic": topic,
            "config": {"threshold": threshold, "window_size": window_size},
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/api/v1/ai/anomalies/detect",
                json=payload,
            ) as resp:
                async for line in resp.content:
                    text = line.decode("utf-8").strip()
                    if text.startswith("data: "):
                        data = json.loads(text[6:])
                        yield AnomalyAlert(
                            field=data.get("field", ""),
                            value=data.get("value", 0),
                            z_score=data.get("z_score", 0),
                            timestamp=data.get("timestamp", 0),
                            topic=topic,
                        )

    async def rag(
        self,
        query: str,
        context_topic: str,
        model: str = "gpt-4",
        top_k: int = 5,
    ) -> RAGResponse:
        """Execute a RAG (Retrieval-Augmented Generation) query."""
        data = await self._post("/api/v1/ai/rag", {
            "query": query,
            "context_topic": context_topic,
            "model": model,
            "top_k": top_k,
        })
        return RAGResponse(
            answer=data.get("answer", ""),
            sources=data.get("sources", []),
            model=data.get("model", model),
        )

    async def _post(self, path: str, payload: dict) -> dict:
        """Make a POST request to the AI API."""
        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}{path}",
                    json=payload,
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"AI API error ({resp.status}): {text}")
                    return await resp.json()
        else:
            import urllib.request
            req = urllib.request.Request(
                f"{self.base_url}{path}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
