"""StreamQL query client for executing SQL queries on streaming data."""

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional
import asyncio
import json

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


@dataclass
class QueryResult:
    """Result of a SQL query."""
    columns: list[dict[str, str]]
    rows: list[list[Any]]
    execution_time_ms: int = 0
    rows_scanned: int = 0
    rows_returned: int = 0
    truncated: bool = False


class QueryClient:
    """Client for the Streamline unified query API (POST /api/v1/query).

    Example:
        client = QueryClient("http://localhost:9094")
        result = await client.query("SELECT * FROM topic('events') LIMIT 10")
        for row in result.rows:
            print(row)
    """

    def __init__(self, base_url: str = "http://localhost:9094"):
        self.base_url = base_url.rstrip("/")

    async def query(
        self,
        sql: str,
        timeout_ms: int = 30000,
        max_rows: int = 10000,
    ) -> QueryResult:
        """Execute a SQL query and return results."""
        payload = {
            "sql": sql,
            "timeout_ms": timeout_ms,
            "max_rows": max_rows,
            "format": "json",
        }

        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v1/query",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=(timeout_ms + 5000) / 1000),
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"Query failed (HTTP {resp.status}): {text}")
                    data = await resp.json()
        else:
            # Fallback to urllib for environments without aiohttp
            # Run in thread to avoid blocking the event loop
            import urllib.request
            req = urllib.request.Request(
                f"{self.base_url}/api/v1/query",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            def _sync_request():
                with urllib.request.urlopen(req, timeout=timeout_ms / 1000) as resp:
                    return json.loads(resp.read())
            data = await asyncio.to_thread(_sync_request)

        meta = data.get("metadata", {})
        return QueryResult(
            columns=data.get("columns", []),
            rows=data.get("rows", []),
            execution_time_ms=meta.get("execution_time_ms", 0),
            rows_scanned=meta.get("rows_scanned", 0),
            rows_returned=meta.get("rows_returned", 0),
            truncated=meta.get("truncated", False),
        )

    async def explain(self, sql: str) -> str:
        """Get the query execution plan."""
        payload = {"sql": sql}
        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v1/query/explain",
                    json=payload,
                ) as resp:
                    data = await resp.json()
                    return data.get("plan", "")
        else:
            import urllib.request
            req = urllib.request.Request(
                f"{self.base_url}/api/v1/query/explain",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            def _sync_explain():
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read())
            data = await asyncio.to_thread(_sync_explain)
            return data.get("plan", "")

