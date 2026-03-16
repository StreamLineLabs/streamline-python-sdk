"""Tests for the SQL query client module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk.query import QueryClient, QueryResult


class TestQueryClientInit:
    """Test QueryClient initialization."""

    def test_default_base_url(self):
        client = QueryClient()
        assert client.base_url == "http://localhost:9094"

    def test_custom_base_url(self):
        client = QueryClient("http://myhost:8080")
        assert client.base_url == "http://myhost:8080"

    def test_strips_trailing_slash(self):
        client = QueryClient("http://myhost:8080/")
        assert client.base_url == "http://myhost:8080"


class TestQueryClientQuery:
    """Test the query() method."""

    @pytest.fixture
    def client(self):
        return QueryClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_query_returns_query_result(self, client):
        mock_data = {
            "columns": [
                {"name": "id", "type": "INT"},
                {"name": "value", "type": "TEXT"},
            ],
            "rows": [[1, "hello"], [2, "world"]],
            "metadata": {
                "execution_time_ms": 42,
                "rows_scanned": 100,
                "rows_returned": 2,
                "truncated": False,
            },
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            result = await client.query("SELECT * FROM topic('events') LIMIT 10")

        assert isinstance(result, QueryResult)
        assert len(result.columns) == 2
        assert result.columns[0] == {"name": "id", "type": "INT"}
        assert len(result.rows) == 2
        assert result.rows[0] == [1, "hello"]
        assert result.execution_time_ms == 42
        assert result.rows_scanned == 100
        assert result.rows_returned == 2
        assert result.truncated is False

    @pytest.mark.asyncio
    async def test_query_sends_correct_payload(self, client):
        mock_data = {"columns": [], "rows": [], "metadata": {}}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            await client.query(
                "SELECT * FROM topic('events')",
                timeout_ms=5000,
                max_rows=100,
            )

        call_kwargs = mock_session.post.call_args
        assert call_kwargs.args[0] == "http://localhost:9094/api/v1/query"
        assert call_kwargs.kwargs["json"] == {
            "sql": "SELECT * FROM topic('events')",
            "timeout_ms": 5000,
            "max_rows": 100,
            "format": "json",
        }

    @pytest.mark.asyncio
    async def test_query_raises_on_non_200(self, client):
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.text = AsyncMock(
            return_value="Invalid SQL: syntax error near 'SELEC'"
        )

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            with pytest.raises(RuntimeError, match="Query failed \\(HTTP 400\\)"):
                await client.query("SELEC * FROM events")

    @pytest.mark.asyncio
    async def test_query_handles_missing_metadata(self, client):
        mock_data = {
            "columns": [{"name": "id", "type": "INT"}],
            "rows": [[1]],
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            result = await client.query("SELECT id FROM events")

        assert result.execution_time_ms == 0
        assert result.rows_scanned == 0
        assert result.rows_returned == 0
        assert result.truncated is False

    @pytest.mark.asyncio
    async def test_query_raises_on_connection_error(self, client):
        mock_session = AsyncMock()
        mock_session.post = MagicMock(side_effect=ConnectionError("refused"))

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            with pytest.raises(ConnectionError, match="refused"):
                await client.query("SELECT 1")

    @pytest.mark.asyncio
    async def test_query_empty_response(self, client):
        mock_data = {"columns": [], "rows": [], "metadata": {}}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            result = await client.query("SELECT * FROM empty_topic")

        assert result.columns == []
        assert result.rows == []

    @pytest.mark.asyncio
    async def test_query_server_error_500(self, client):
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

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)
            mock_aiohttp.ClientTimeout = MagicMock()

            with pytest.raises(RuntimeError, match="Query failed \\(HTTP 500\\)"):
                await client.query("SELECT 1")


class TestQueryClientExplain:
    """Test the explain() method."""

    @pytest.fixture
    def client(self):
        return QueryClient("http://localhost:9094")

    @pytest.mark.asyncio
    async def test_explain_returns_plan(self, client):
        mock_data = {"plan": "Seq Scan on events\n  Filter: id > 10"}

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)

            result = await client.explain("SELECT * FROM events WHERE id > 10")

        assert result == "Seq Scan on events\n  Filter: id > 10"

    @pytest.mark.asyncio
    async def test_explain_empty_plan(self, client):
        mock_data = {}

        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value=mock_data)

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_session_ctx)

        mock_client_session = AsyncMock()
        mock_client_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_client_session.__aexit__ = AsyncMock(return_value=False)

        with patch("streamline_sdk.query.aiohttp") as mock_aiohttp, \
             patch("streamline_sdk.query.HAS_AIOHTTP", True):
            mock_aiohttp.ClientSession = MagicMock(return_value=mock_client_session)

            result = await client.explain("SELECT 1")

        assert result == ""


class TestQueryResult:
    """Test the QueryResult dataclass."""

    def test_default_values(self):
        result = QueryResult(columns=[], rows=[])
        assert result.execution_time_ms == 0
        assert result.rows_scanned == 0
        assert result.rows_returned == 0
        assert result.truncated is False

    def test_with_all_fields(self):
        result = QueryResult(
            columns=[{"name": "id", "type": "INT"}],
            rows=[[1], [2]],
            execution_time_ms=50,
            rows_scanned=1000,
            rows_returned=2,
            truncated=True,
        )
        assert result.rows_returned == 2
        assert result.truncated is True
