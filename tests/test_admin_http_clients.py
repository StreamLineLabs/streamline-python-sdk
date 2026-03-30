"""Unit tests for the new HTTP admin clients (branches, contracts, attest).

These tests do not require a running broker. They patch the SDK's
``_post`` / ``_request`` async methods so we can drive every code path
deterministically.
"""

from __future__ import annotations

import asyncio
import base64
from typing import Any

import pytest

from streamline_sdk import (
    ATTEST_HEADER,
    AttestationError,
    Attestor,
    BranchAdminClient,
    BranchAdminError,
    BranchMessage,
    BranchView,
    ContractsClient,
    ContractsError,
    SignedAttestation,
    ValidationError,
    ValidationResult,
)


# --------------------------------------------------------------------- helpers


def _patch_branches(monkeypatch: pytest.MonkeyPatch, handler):
    async def _request(self, method: str, path: str, *, json_body: Any = None):
        return await handler(method, path, json_body)

    monkeypatch.setattr(BranchAdminClient, "_request", _request)


def _patch_contracts(monkeypatch: pytest.MonkeyPatch, handler):
    async def _post(self, url: str, body: dict[str, Any]):
        return await handler(url, body)

    monkeypatch.setattr(ContractsClient, "_post", _post)


def _patch_attest(monkeypatch: pytest.MonkeyPatch, handler):
    async def _post(self, path: str, body: dict[str, Any]):
        return await handler(path, body)

    monkeypatch.setattr(Attestor, "_post", _post)


# --------------------------------------------------------------------- branches


@pytest.mark.asyncio
async def test_branches_create_and_list(monkeypatch):
    captured: list[tuple[str, str, Any]] = []

    async def handler(method, path, body):
        captured.append((method, path, body))
        if method == "POST" and path == "/api/v1/branches":
            return {
                "id": f"{body['topic']}/{body['name']}",
                "parent": body.get("parent"),
                "created_at_ms": 1700000000000,
                "message_count": 0,
                "metadata": body.get("metadata", {}),
            }
        if method == "GET" and path == "/api/v1/branches":
            return [
                {"id": "orders/exp-a", "message_count": 0},
                {"id": "orders/exp-b", "message_count": 7},
            ]
        raise AssertionError(f"unexpected {method} {path}")

    _patch_branches(monkeypatch, handler)
    client = BranchAdminClient("http://example:9094")

    view = await client.create("orders", "exp-a", metadata={"owner": "a@b"})
    assert isinstance(view, BranchView)
    assert view.id == "orders/exp-a"
    assert view.metadata == {"owner": "a@b"}

    items = await client.list()
    assert [b.id for b in items] == ["orders/exp-a", "orders/exp-b"]

    assert captured[0][0:2] == ("POST", "/api/v1/branches")
    assert captured[1][0:2] == ("GET", "/api/v1/branches")


@pytest.mark.asyncio
async def test_branches_append_and_messages(monkeypatch):
    state: list[dict[str, Any]] = []

    async def handler(method, path, body):
        if method == "POST" and path.endswith("/messages"):
            state.append(body)
            return {}
        if method == "GET" and path.endswith("/messages"):
            return state
        raise AssertionError(path)

    _patch_branches(monkeypatch, handler)
    client = BranchAdminClient()

    await client.append("orders/exp-a", role="user", text="hi", timestamp_ms=42)
    msgs = await client.messages("orders/exp-a")
    assert len(msgs) == 1
    assert msgs[0] == BranchMessage(role="user", text="hi", timestamp_ms=42)


@pytest.mark.asyncio
async def test_branches_error_propagates(monkeypatch):
    # Use the real _check via a synthetic urllib path is overkill; just
    # raise from the handler patch to mimic any HTTP failure.
    async def handler(method, path, body):
        raise BranchAdminError("DELETE /api/v1/branches/x -> HTTP 404: missing")

    _patch_branches(monkeypatch, handler)
    client = BranchAdminClient()
    with pytest.raises(BranchAdminError):
        await client.delete("x")


# --------------------------------------------------------------------- contracts


@pytest.mark.asyncio
async def test_contracts_validate_ok(monkeypatch):
    async def handler(url, body):
        assert "value" in body and body["contract"]["name"] == "orders.v1"
        return 200, {"valid": True, "schema_id": 7, "errors": []}

    _patch_contracts(monkeypatch, handler)
    client = ContractsClient()
    result = await client.validate(
        contract={"name": "orders.v1", "schema_id": 7, "fields": []},
        value={"id": "abc"},
    )
    assert result == ValidationResult(valid=True, schema_id=7, errors=[])


@pytest.mark.asyncio
async def test_contracts_validate_rejected(monkeypatch):
    async def handler(url, body):
        return 400, {
            "valid": False,
            "schema_id": 7,
            "errors": [
                {
                    "field_path": "$.id",
                    "expected": "string",
                    "actual": "integer",
                    "message": "type mismatch",
                }
            ],
        }

    _patch_contracts(monkeypatch, handler)
    client = ContractsClient()
    res = await client.validate(contract={"name": "x"}, value={"id": 1})
    assert res.valid is False
    assert res.errors == [
        ValidationError(
            field_path="$.id",
            expected="string",
            actual="integer",
            message="type mismatch",
        )
    ]


@pytest.mark.asyncio
async def test_contracts_validate_string_value(monkeypatch):
    async def handler(url, body):
        assert "value_string" in body
        assert body["value_string"] == "raw string"
        return 200, {"valid": True}

    _patch_contracts(monkeypatch, handler)
    client = ContractsClient()
    res = await client.validate(contract={"name": "x"}, value="raw string")
    assert res.valid is True


@pytest.mark.asyncio
async def test_contracts_validate_5xx_raises(monkeypatch):
    async def handler(url, body):
        return 503, {"error": "unavailable"}

    _patch_contracts(monkeypatch, handler)
    client = ContractsClient()
    with pytest.raises(ContractsError):
        await client.validate(contract={"name": "x"}, value={})


# --------------------------------------------------------------------- attest


@pytest.mark.asyncio
async def test_attest_sign_then_verify_roundtrip(monkeypatch):
    """Roundtrip with stubbed broker: we record what was signed and
    return a synthetic signed envelope, then accept the same envelope
    on verify."""

    last_signed: dict[str, Any] = {}

    async def handler(path, body):
        if path == "/api/v1/attest":
            last_signed.update(body)
            return 200, {
                "key_id": body["key_id"],
                "algorithm": "ed25519",
                "timestamp_ms": body["timestamp_ms"],
                "payload_sha256": "deadbeef",
                "signature_b64": "ZmFrZXNpZw==",
                "header_name": ATTEST_HEADER,
                "header_value": "ZmFrZXNpZw==",
            }
        if path == "/api/v1/attest/verify":
            # Round-trip identity check on the bytes we previously signed.
            same_value = body.get("value") == last_signed.get("value") and body.get(
                "value_b64"
            ) == last_signed.get("value_b64")
            same_meta = (
                body["topic"] == last_signed["topic"]
                and body["partition"] == last_signed["partition"]
                and body["offset"] == last_signed["offset"]
                and body["timestamp_ms"] == last_signed["timestamp_ms"]
                and body["key_id"] == last_signed["key_id"]
            )
            valid = (
                same_value
                and same_meta
                and body["signature_b64"] == "ZmFrZXNpZw=="
            )
            return 200, {
                "valid": valid,
                "key_id": body["key_id"],
                "algorithm": body["algorithm"],
            }
        raise AssertionError(path)

    _patch_attest(monkeypatch, handler)
    attestor = Attestor(key_id="broker-0")

    sig = await attestor.sign(
        topic="orders",
        partition=0,
        offset=42,
        value=b'{"id":1}',
        schema_id=7,
        timestamp_ms=1700000000000,
    )
    assert isinstance(sig, SignedAttestation)
    assert sig.header_name == ATTEST_HEADER
    assert last_signed["value_b64"] == base64.b64encode(b'{"id":1}').decode()

    ok = await attestor.verify(
        topic="orders",
        partition=0,
        offset=42,
        value=b'{"id":1}',
        schema_id=7,
        timestamp_ms=1700000000000,
        signature_b64=sig.signature_b64,
    )
    assert ok is True


@pytest.mark.asyncio
async def test_attest_verify_rejects_tampered_offset(monkeypatch):
    async def handler(path, body):
        if path == "/api/v1/attest":
            return 200, {
                "key_id": body["key_id"],
                "algorithm": "ed25519",
                "timestamp_ms": body["timestamp_ms"],
                "payload_sha256": "x",
                "signature_b64": "AAAA",
                "header_name": ATTEST_HEADER,
                "header_value": "AAAA",
            }
        # verify: pretend broker rejects when offset != 1
        return 200, {
            "valid": body["offset"] == 1,
            "key_id": body["key_id"],
            "algorithm": body["algorithm"],
        }

    _patch_attest(monkeypatch, handler)
    attestor = Attestor()
    sig = await attestor.sign(
        topic="t",
        partition=0,
        offset=1,
        value="x",
        timestamp_ms=1,
    )
    assert (
        await attestor.verify(
            topic="t",
            partition=0,
            offset=999,
            value="x",
            timestamp_ms=1,
            signature_b64=sig.signature_b64,
        )
        is False
    )


@pytest.mark.asyncio
async def test_attest_sign_5xx_raises(monkeypatch):
    async def handler(path, body):
        return 500, {"error": "kms_unreachable"}

    _patch_attest(monkeypatch, handler)
    with pytest.raises(AttestationError):
        await Attestor().sign(
            topic="t", partition=0, offset=0, value="v"
        )


@pytest.mark.asyncio
async def test_attest_string_value_uses_value_field(monkeypatch):
    seen: dict[str, Any] = {}

    async def handler(path, body):
        seen.update(body)
        return 200, {
            "key_id": body["key_id"],
            "algorithm": "ed25519",
            "timestamp_ms": body["timestamp_ms"],
            "payload_sha256": "x",
            "signature_b64": "BBBB",
            "header_name": ATTEST_HEADER,
            "header_value": "BBBB",
        }

    _patch_attest(monkeypatch, handler)
    await Attestor().sign(topic="t", partition=0, offset=0, value="hello")
    assert seen.get("value") == "hello"
    assert "value_b64" not in seen
