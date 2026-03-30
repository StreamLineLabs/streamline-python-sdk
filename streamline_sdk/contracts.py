"""Contracts validate client (M2 wiring).

Wraps ``POST /api/v1/contracts/validate`` so producers can dry-run a
contract before applying it. Stateless: each call is self-contained.

Example:
    client = ContractsClient("http://localhost:9094")
    result = await client.validate(
        contract={
            "name": "orders.v1",
            "schema_id": 7,
            "fields": [{"name": "id", "type": "string", "required": True}],
        },
        value={"id": "abc"},
    )
    if not result.valid:
        print(result.errors)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Optional, Union

try:
    import aiohttp

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover
    _HAS_AIOHTTP = False

from .exceptions import StreamlineError


class ContractsError(StreamlineError):
    """Raised when the contracts API itself fails (5xx, network, etc.)."""


@dataclass
class ValidationError:
    """A single validation failure."""

    field_path: str
    expected: str
    actual: str
    message: str = ""

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ValidationError":
        return cls(
            field_path=str(data.get("field_path", "")),
            expected=str(data.get("expected", "")),
            actual=str(data.get("actual", "")),
            message=str(data.get("message", "")),
        )


@dataclass
class ValidationResult:
    """Outcome of a contract validation request."""

    valid: bool
    schema_id: Optional[int] = None
    errors: list[ValidationError] = field(default_factory=list)


class ContractsClient:
    """Async client for the contracts validate endpoint.

    Args:
        http_url: HTTP base URL (default: ``http://localhost:9094``).
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self, http_url: str = "http://localhost:9094", timeout: float = 10.0
    ) -> None:
        self.http_url = http_url.rstrip("/")
        self.timeout = timeout

    async def validate(
        self,
        contract: dict[str, Any],
        value: Union[dict[str, Any], list[Any], str, bytes],
    ) -> ValidationResult:
        """Dry-run ``contract`` against ``value``.

        ``value`` may be a JSON-serializable object (sent as ``value``)
        or raw bytes/str (sent as ``value_b64``-style, encoded inline).
        """
        body: dict[str, Any] = {"contract": contract}
        if isinstance(value, (bytes, bytearray)):
            body["value_string"] = value.decode("utf-8", errors="replace")
        elif isinstance(value, str):
            body["value_string"] = value
        else:
            body["value"] = value

        url = f"{self.http_url}/api/v1/contracts/validate"
        status, payload = await self._post(url, body)

        if status == 200:
            data = payload or {}
            return ValidationResult(
                valid=True,
                schema_id=data.get("schema_id"),
                errors=[],
            )
        if status == 400:
            data = payload or {}
            errs = [
                ValidationError.from_json(e)
                for e in data.get("errors", [])
            ]
            return ValidationResult(
                valid=False,
                schema_id=data.get("schema_id"),
                errors=errs,
            )
        raise ContractsError(
            f"validate -> HTTP {status}: {json.dumps(payload)[:512]}"
        )

    async def _post(self, url: str, body: dict[str, Any]) -> tuple[int, Any]:
        if _HAS_AIOHTTP:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as resp:
                    text = await resp.text()
                    payload: Any = None
                    if text:
                        try:
                            payload = json.loads(text)
                        except json.JSONDecodeError:
                            payload = {"raw": text}
                    return resp.status, payload
        else:
            import urllib.request
            import urllib.error

            data = json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            def _sync() -> tuple[int, Any]:
                try:
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        text = resp.read().decode("utf-8")
                        return resp.status, json.loads(text) if text else None
                except urllib.error.HTTPError as e:
                    text = e.read().decode("utf-8", errors="replace")
                    try:
                        return e.code, json.loads(text)
                    except json.JSONDecodeError:
                        return e.code, {"raw": text}

            return await asyncio.to_thread(_sync)


__all__ = [
    "ContractsClient",
    "ContractsError",
    "ValidationError",
    "ValidationResult",
]
