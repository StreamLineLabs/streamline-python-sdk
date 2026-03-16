"""Tests for the AI client module."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk.ai import (
    AIClient,
    AnomalyAlert,
    EmbeddingResult,
    RAGResponse,
    SearchResult,
)


class TestAIClientInit:
    """Test AIClient initialization."""

    def test_default_base_url(self):
        client = AIClient()
        assert client.base_url == "http://localhost:9094"

    def test_custom_base_url(self):
        client = AIClient("http://myhost:8080")
        assert client.base_url == "http://myhost:8080"

    def test_strips_trailing_slash(self):
        client = AIClient("http://myhost:8080/")
        assert client.base_url == "http://myhost:8080"


class TestAIClientEmbed:
    """Test the embed() method."""

    @pytest.fixture
    def client(self):
        return AIClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_embed_returns_embedding_result(self, client):
        mock_response = {
            "vectors": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            "model": "default",
            "usage": {"prompt_tokens": 5, "total_tokens": 5},
        }

        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.embed(["hello", "world"])

        assert isinstance(result, EmbeddingResult)
        assert result.vectors == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
        assert result.model == "default"
        assert result.usage == {"prompt_tokens": 5, "total_tokens": 5}
        mock_post.assert_awaited_once_with(
            "/api/v1/ai/embed",
            {"texts": ["hello", "world"], "model": "default"},
        )

    @pytest.mark.asyncio
    async def test_embed_with_custom_model(self, client):
        mock_response = {"vectors": [[0.1]], "model": "custom-model", "usage": {}}

        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.embed(["test"], model="custom-model")

        assert result.model == "custom-model"
        mock_post.assert_awaited_once_with(
            "/api/v1/ai/embed",
            {"texts": ["test"], "model": "custom-model"},
        )

    @pytest.mark.asyncio
    async def test_embed_empty_response_fields(self, client):
        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {}
            result = await client.embed(["test"])

        assert result.vectors == []
        assert result.model == "default"
        assert result.usage == {}


class TestAIClientSearch:
    """Test the search() method."""

    @pytest.fixture
    def client(self):
        return AIClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_search_returns_results(self, client):
        mock_response = {
            "results": [
                {"score": 0.95, "offset": 42, "value": "login event"},
                {"score": 0.87, "offset": 99, "value": "logout event"},
            ]
        }

        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            results = await client.search("login", topic="events", top_k=5)

        assert len(results) == 2
        assert all(isinstance(r, SearchResult) for r in results)
        assert results[0].score == 0.95
        assert results[0].offset == 42
        assert results[0].value == "login event"
        assert results[0].topic == "events"
        assert results[1].score == 0.87
        mock_post.assert_awaited_once_with(
            "/api/v1/ai/search",
            {"query": "login", "topic": "events", "top_k": 5},
        )

    @pytest.mark.asyncio
    async def test_search_empty_results(self, client):
        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"results": []}
            results = await client.search("nothing", topic="events")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_missing_results_key(self, client):
        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {}
            results = await client.search("query", topic="events")

        assert results == []


class TestAIClientDetectAnomalies:
    """Test the detect_anomalies() method."""

    @pytest.fixture
    def client(self):
        return AIClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_detect_anomalies_yields_alerts(self, client):
        sse_lines = [
            b"data: " + json.dumps({
                "field": "cpu",
                "value": 99.5,
                "z_score": 3.2,
                "timestamp": 1000,
            }).encode() + b"\n",
            b"data: " + json.dumps({
                "field": "memory",
                "value": 95.0,
                "z_score": 2.8,
                "timestamp": 1001,
            }).encode() + b"\n",
        ]

        mock_response = AsyncMock()
        mock_response.content = _async_iter(sse_lines)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.ai.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.ai.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)

            alerts = []
            async for alert in client.detect_anomalies("metrics", threshold=3.0):
                alerts.append(alert)

        assert len(alerts) == 2
        assert all(isinstance(a, AnomalyAlert) for a in alerts)
        assert alerts[0].field == "cpu"
        assert alerts[0].value == 99.5
        assert alerts[0].z_score == 3.2
        assert alerts[0].timestamp == 1000
        assert alerts[0].topic == "metrics"
        assert alerts[1].field == "memory"

    @pytest.mark.asyncio
    async def test_detect_anomalies_raises_without_aiohttp(self, client):
        with patch("streamline_sdk.ai.HAS_AIOHTTP", False):
            with pytest.raises(ImportError, match="aiohttp required"):
                async for _ in client.detect_anomalies("metrics"):
                    pass


class TestAIClientRAG:
    """Test the rag() method."""

    @pytest.fixture
    def client(self):
        return AIClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_rag_returns_response(self, client):
        mock_response = {
            "answer": "The system experienced a spike at 14:00.",
            "sources": [{"topic": "incidents", "offset": 10}],
            "model": "gpt-4",
        }

        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.rag("What happened?", context_topic="incidents")

        assert isinstance(result, RAGResponse)
        assert result.answer == "The system experienced a spike at 14:00."
        assert result.sources == [{"topic": "incidents", "offset": 10}]
        assert result.model == "gpt-4"
        mock_post.assert_awaited_once_with(
            "/api/v1/ai/rag",
            {
                "query": "What happened?",
                "context_topic": "incidents",
                "model": "gpt-4",
                "top_k": 5,
            },
        )

    @pytest.mark.asyncio
    async def test_rag_with_custom_params(self, client):
        mock_response = {"answer": "result", "sources": [], "model": "gpt-3.5"}

        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response
            result = await client.rag(
                "query",
                context_topic="logs",
                model="gpt-3.5",
                top_k=20,
            )

        assert result.model == "gpt-3.5"
        mock_post.assert_awaited_once_with(
            "/api/v1/ai/rag",
            {
                "query": "query",
                "context_topic": "logs",
                "model": "gpt-3.5",
                "top_k": 20,
            },
        )

    @pytest.mark.asyncio
    async def test_rag_empty_response_fields(self, client):
        with patch.object(client, "_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {}
            result = await client.rag("query", context_topic="ctx")

        assert result.answer == ""
        assert result.sources == []
        assert result.model == "gpt-4"


class TestAIClientPost:
    """Test the _post() internal method error handling."""

    @pytest.fixture
    def client(self):
        return AIClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_post_raises_on_non_200_with_aiohttp(self, client):
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.ai.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.ai.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)

            with pytest.raises(RuntimeError, match="AI API error \\(500\\)"):
                await client._post("/api/v1/ai/embed", {"texts": ["test"]})

    @pytest.mark.asyncio
    async def test_post_success_with_aiohttp(self, client):
        expected = {"vectors": [[1.0, 2.0]]}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=expected)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.ai.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.ai.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)

            result = await client._post("/api/v1/ai/embed", {"texts": ["test"]})

        assert result == expected


# ── Helpers ──────────────────────────────────────────────────────────────


async def _async_iter(items):
    """Create an async iterator from a list."""
    for item in items:
        yield item
