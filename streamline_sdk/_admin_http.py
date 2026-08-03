"""Private HTTP transport used by :class:`streamline_sdk.admin.Admin`.

This module owns the REST transport concerns for admin operations: base URL
handling, the optional ``aiohttp``/``urllib`` transport selection, request
construction (GET/POST/DELETE), the fixed 10-second timeout, status/error
mapping, and JSON decoding. It is intentionally private (leading underscore)
because it is an implementation detail of :mod:`streamline_sdk.admin`, not a
public API surface.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from .exceptions import TopicError

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


class _AdminHttpTransport:
    """Thin REST transport for the Streamline admin HTTP API.

    Uses ``aiohttp`` when available, falling back to a synchronous
    ``urllib.request`` call executed on a worker thread otherwise.
    """

    def __init__(self, base_url: str) -> None:
        """Initialize the transport.

        Args:
            base_url: Base URL of the Streamline HTTP REST API.
        """
        self._base_url = base_url

    async def get(self, path: str) -> Any:
        """Make an HTTP GET request to the Streamline REST API."""
        url = f"{self._base_url}{path}"

        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 404:
                        raise TopicError(f"Not found: {path}")
                    if resp.status != 200:
                        text = await resp.text()
                        raise TopicError(f"HTTP {resp.status}: {text}")
                    return await resp.json()
        else:
            import urllib.request
            req = urllib.request.Request(url)
            def _sync_get():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read())
            return await asyncio.to_thread(_sync_get)

    async def post(self, path: str, body: Any) -> Any:
        """Make an HTTP POST request to the Streamline REST API."""
        url = f"{self._base_url}{path}"

        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=body, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 404:
                        raise TopicError(f"Not found: {path}")
                    if resp.status not in (200, 201):
                        text = await resp.text()
                        raise TopicError(f"HTTP {resp.status}: {text}")
                    return await resp.json()
        else:
            import urllib.request
            payload = json.dumps(body).encode("utf-8")
            req = urllib.request.Request(url, data=payload, method="POST")
            req.add_header("Content-Type", "application/json")
            def _sync_post():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read())
            return await asyncio.to_thread(_sync_post)

    async def delete(self, path: str) -> None:
        """Make an HTTP DELETE request to the Streamline REST API."""
        url = f"{self._base_url}{path}"

        if HAS_AIOHTTP:
            async with aiohttp.ClientSession() as session:
                async with session.delete(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 404:
                        raise TopicError(f"Not found: {path}")
                    if resp.status >= 300:
                        text = await resp.text()
                        raise TopicError(f"HTTP {resp.status}: {text}")
        else:
            import urllib.request
            req = urllib.request.Request(url, method="DELETE")
            def _sync_delete():
                with urllib.request.urlopen(req, timeout=10):
                    pass
            await asyncio.to_thread(_sync_delete)
