"""Local Ed25519 attestation verifier for Streamline consumer records.

Verifies ``streamline-attest`` headers locally using an Ed25519 public key
without any network calls.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

ATTEST_HEADER = "streamline-attest"


@dataclass
class VerificationResult:
    """Result of an attestation verification."""

    verified: bool
    producer_id: str = ""
    schema_id: Optional[int] = None
    contract_id: Optional[str] = None
    timestamp_ms: int = 0


class StreamlineVerifier:
    """Verifies attestation headers on consumed records using a local Ed25519 public key.

    Args:
        public_key: An Ed25519 public key used for signature verification.

    Example::

        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        from streamline_sdk.verifier import StreamlineVerifier

        pub_key = Ed25519PublicKey.from_public_bytes(key_bytes)
        verifier = StreamlineVerifier(pub_key)
        result = verifier.verify(record)
        if result.verified:
            print(f"Verified from {result.producer_id}")
    """

    def __init__(self, public_key: Ed25519PublicKey) -> None:
        self._public_key = public_key

    def verify(self, record: Any) -> VerificationResult:
        """Verify the attestation on a consumer record.

        Extracts the ``streamline-attest`` header, parses the base64-encoded
        JSON attestation, reconstructs the canonical bytes, and verifies the
        Ed25519 signature.

        Args:
            record: A ConsumerRecord with a ``headers`` dict containing
                the ``streamline-attest`` header.

        Returns:
            VerificationResult with ``verified=True`` if the signature is valid.
        """
        headers: Dict[str, Union[bytes, str]] = getattr(record, "headers", {})
        raw = headers.get(ATTEST_HEADER)
        if raw is None:
            return VerificationResult(verified=False)

        try:
            raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else raw.encode("utf-8")
            attestation = json.loads(base64.b64decode(raw_bytes))
        except (json.JSONDecodeError, Exception):
            return VerificationResult(verified=False)

        try:
            payload_sha256 = str(attestation["payload_sha256"])
            topic = str(attestation["topic"])
            partition = int(attestation["partition"])
            offset = int(attestation["offset"])
            schema_id = int(attestation["schema_id"])
            timestamp_ms = int(attestation["timestamp_ms"])
            key_id = str(attestation["key_id"])
            signature_b64 = str(attestation["signature"])
        except (KeyError, ValueError, TypeError):
            return VerificationResult(verified=False)

        canonical = (
            f"{topic}|{partition}|{offset}|{payload_sha256}"
            f"|{schema_id}|{timestamp_ms}|{key_id}"
        )
        canonical_bytes = canonical.encode("utf-8")

        try:
            signature = base64.b64decode(signature_b64)
        except Exception:
            return VerificationResult(verified=False)

        try:
            self._public_key.verify(signature, canonical_bytes)
            verified = True
        except InvalidSignature:
            verified = False

        return VerificationResult(
            verified=verified,
            producer_id=key_id,
            schema_id=schema_id if schema_id != 0 else None,
            contract_id=attestation.get("contract_id"),
            timestamp_ms=timestamp_ms,
        )


__all__ = [
    "ATTEST_HEADER",
    "StreamlineVerifier",
    "VerificationResult",
]
