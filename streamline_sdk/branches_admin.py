"""HTTP admin client for branched streams (M5 P1, Experimental).

Wraps the broker's ``/api/v1/branches/*`` admin API. Sibling to
:class:`streamline_sdk.branches.BranchedTopic`, which handles read-side
wire-name parsing.

Example:
    client = BranchAdminClient("http://localhost:9094")
    await client.create("orders", "exp-a", parent=None)
    branches = await client.list()
    await client.append("orders/exp-a", role="user", text="hi")
    await client.delete("orders/exp-a")
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

try:
    import aiohttp

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover - import guard
    _HAS_AIOHTTP = False

from .exceptions import StreamlineError


class BranchAdminError(StreamlineError):
    """Raised on branch admin API failures."""


@dataclass
class BranchView:
    """A branch as returned by the admin API."""

    id: str
    parent: Optional[str] = None
    created_at_ms: int = 0
    message_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "BranchView":
        return cls(
            id=data["id"],
            parent=data.get("parent"),
            created_at_ms=int(data.get("created_at_ms", 0)),
            message_count=int(data.get("message_count", 0)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class BranchMessage:
    """A message within a branch."""

    role: str
    text: str
    timestamp_ms: int = 0

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"role": self.role, "text": self.text}
        if self.timestamp_ms:
            out["timestamp_ms"] = self.timestamp_ms
        return out

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "BranchMessage":
        return cls(
            role=str(data.get("role", "")),
            text=str(data.get("text", "")),
            timestamp_ms=int(data.get("timestamp_ms", 0)),
        )


class BranchAdminClient:
    """Async client for the broker's branch admin API.

    Args:
        http_url: HTTP base URL (default: ``http://localhost:9094``).
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self, http_url: str = "http://localhost:9094", timeout: float = 10.0
    ) -> None:
        self.http_url = http_url.rstrip("/")
        self.timeout = timeout

    async def create(
        self,
        topic: str,
        name: str,
        *,
        parent: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> BranchView:
        """Create a new branch ``<topic>/<name>``."""
        body: dict[str, Any] = {"topic": topic, "name": name}
        if parent is not None:
            body["parent"] = parent
        if metadata:
            body["metadata"] = metadata
        data = await self._request("POST", "/api/v1/branches", json_body=body)
        return BranchView.from_json(data)

    async def list(self) -> list[BranchView]:
        """List all known branches across topics."""
        data = await self._request("GET", "/api/v1/branches")
        items = data if isinstance(data, list) else data.get("items", [])
        return [BranchView.from_json(b) for b in items]

    async def get(self, branch_id: str) -> BranchView:
        """Get a single branch by id (``<topic>/<name>``)."""
        data = await self._request(
            "GET", f"/api/v1/branches/{branch_id}"
        )
        return BranchView.from_json(data)

    async def delete(self, branch_id: str) -> None:
        """Delete a branch."""
        await self._request("DELETE", f"/api/v1/branches/{branch_id}")

    async def append(
        self,
        branch_id: str,
        role: str,
        text: str,
        *,
        timestamp_ms: int = 0,
    ) -> None:
        """Append a single message to a branch."""
        msg = BranchMessage(role=role, text=text, timestamp_ms=timestamp_ms)
        await self._request(
            "POST",
            f"/api/v1/branches/{branch_id}/messages",
            json_body=msg.to_json(),
        )

    async def messages(self, branch_id: str) -> list[BranchMessage]:
        """Read all messages on a branch."""
        data = await self._request(
            "GET", f"/api/v1/branches/{branch_id}/messages"
        )
        items = data if isinstance(data, list) else data.get("messages", [])
        return [BranchMessage.from_json(m) for m in items]

    # ------------------------------------------------------------------
    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.http_url}{path}"

        if _HAS_AIOHTTP:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    method, url, json=json_body
                ) as resp:
                    body_text = await resp.text()
                    self._check(resp.status, body_text, method, path)
                    if not body_text:
                        return {}
                    try:
                        return json.loads(body_text)
                    except json.JSONDecodeError:
                        return body_text
        else:  # urllib fallback so the SDK stays importable without aiohttp
            import urllib.request
            import urllib.error

            data: Optional[bytes] = None
            headers = {}
            if json_body is not None:
                data = json.dumps(json_body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(
                url, data=data, headers=headers, method=method
            )

            def _sync() -> Any:
                try:
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        body = resp.read().decode("utf-8")
                        self._check(resp.status, body, method, path)
                        return json.loads(body) if body else {}
                except urllib.error.HTTPError as e:
                    body = e.read().decode("utf-8", errors="replace")
                    self._check(e.code, body, method, path)
                    return {}

            return await asyncio.to_thread(_sync)

    @staticmethod
    def _check(status: int, body: str, method: str, path: str) -> None:
        if 200 <= status < 300:
            return
        raise BranchAdminError(
            f"{method} {path} -> HTTP {status}: {body[:512]}"
        )


__all__ = [
    "BranchAdminClient",
    "BranchAdminError",
    "BranchMessage",
    "BranchView",
]
