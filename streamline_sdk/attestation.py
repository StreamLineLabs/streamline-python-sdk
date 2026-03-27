"""Attestation client (M4 wiring).

Wraps ``POST /api/v1/attest`` and ``POST /api/v1/attest/verify`` for
producing and verifying Ed25519 envelope signatures over Kafka records.

Example:
    attestor = Attestor("http://localhost:9094", key_id="broker-0")
    sig = await attestor.sign(
        topic="orders", partition=0, offset=42,
        value=b'{"id":1}', schema_id=7,
    )
    ok = await attestor.verify(
        topic="orders", partition=0, offset=42,
        value=b'{"id":1}', schema_id=7,
        timestamp_ms=sig.timestamp_ms, signature_b64=sig.signature_b64,
    )
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import dataclass
from typing import Any, Optional, Union

try:
    import aiohttp

    _HAS_AIOHTTP = True
except ImportError:  # pragma: no cover
    _HAS_AIOHTTP = False

from .exceptions import StreamlineError

ATTEST_HEADER = "streamline-attest"


class AttestationError(StreamlineError):
    """Raised on attestation API failures."""


@dataclass
class SignedAttestation:
    """Result of a successful sign request."""

    key_id: str
    algorithm: str
    timestamp_ms: int
    payload_sha256: str
    signature_b64: str
    header_name: str
    header_value: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "SignedAttestation":
        return cls(
            key_id=str(data["key_id"]),
            algorithm=str(data["algorithm"]),
            timestamp_ms=int(data["timestamp_ms"]),
            payload_sha256=str(data["payload_sha256"]),
            signature_b64=str(data["signature_b64"]),
            header_name=str(data["header_name"]),
            header_value=str(data["header_value"]),
        )


def _encode_value(
    value: Union[bytes, bytearray, str],
) -> tuple[str, str]:
    """Return (json_field_name, json_field_value) for the value."""
    if isinstance(value, (bytes, bytearray)):
        return "value_b64", base64.b64encode(bytes(value)).decode("ascii")
    return "value", str(value)


class Attestor:
    """Async client for the broker's attestation sign/verify routes.

    Args:
        http_url: HTTP base URL.
        key_id: Default signing key id used by :meth:`sign`.
        algorithm: Default signature algorithm (server currently supports
            ``ed25519``; ``ecdsa-p256`` may also be accepted by ``verify``).
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        http_url: str = "http://localhost:9094",
        key_id: str = "broker-0",
        algorithm: str = "ed25519",
        timeout: float = 10.0,
    ) -> None:
        self.http_url = http_url.rstrip("/")
        self.key_id = key_id
        self.algorithm = algorithm
        self.timeout = timeout

    async def sign(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        value: Union[bytes, bytearray, str],
        schema_id: int = 0,
        timestamp_ms: Optional[int] = None,
        key_id: Optional[str] = None,
    ) -> SignedAttestation:
        """Sign an attestation envelope for a record."""
        ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        body: dict[str, Any] = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "schema_id": schema_id,
            "timestamp_ms": ts,
            "key_id": key_id or self.key_id,
        }
        field, encoded = _encode_value(value)
        body[field] = encoded

        status, payload = await self._post("/api/v1/attest", body)
        if status != 200:
            raise AttestationError(
                f"sign -> HTTP {status}: {json.dumps(payload)[:512]}"
            )
        return SignedAttestation.from_json(payload or {})

    async def verify(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        value: Union[bytes, bytearray, str],
        timestamp_ms: int,
        signature_b64: str,
        schema_id: int = 0,
        key_id: Optional[str] = None,
        algorithm: Optional[str] = None,
    ) -> bool:
        """Verify a previously-issued attestation. Returns ``True`` on success."""
        body: dict[str, Any] = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "schema_id": schema_id,
            "timestamp_ms": timestamp_ms,
            "key_id": key_id or self.key_id,
            "signature_b64": signature_b64,
            "algorithm": algorithm or self.algorithm,
        }
        field, encoded = _encode_value(value)
        body[field] = encoded

        status, payload = await self._post("/api/v1/attest/verify", body)
        if status != 200:
            raise AttestationError(
                f"verify -> HTTP {status}: {json.dumps(payload)[:512]}"
            )
        return bool((payload or {}).get("valid", False))

    async def _post(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, Any]:
        url = f"{self.http_url}{path}"
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
    "ATTEST_HEADER",
    "Attestor",
    "AttestationError",
    "SignedAttestation",
]
