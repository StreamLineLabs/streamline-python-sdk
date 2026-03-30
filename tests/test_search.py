"""Tests for the M2 semantic search() helper.

These tests stub the HTTP layer; broker integration tests live under
``tests/integration/test_semantic_search.py`` and require a running broker
with semantic-topics enabled.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamline_sdk.consumer import SearchHit, search
from streamline_sdk.exceptions import ConsumerError


def _mock_session(payload: dict, status: int = 200):
    response = AsyncMock()
    response.status = status
    response.json = AsyncMock(return_value=payload)
    response.text = AsyncMock(return_value=json.dumps(payload))

    cm = AsyncMock()
    cm.__aenter__.return_value = response
    cm.__aexit__.return_value = None

    session = MagicMock()
    session.post = MagicMock(return_value=cm)
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_cm.__aexit__.return_value = None
    return session_cm


@pytest.mark.asyncio
async def test_search_returns_hits_in_order():
    payload = {
        "hits": [
            {"partition": 0, "offset": 42, "score": 0.93},
            {"partition": 1, "offset": 7, "score": 0.81},
        ],
        "took_ms": 5,
    }
    with patch("aiohttp.ClientSession", return_value=_mock_session(payload)):
        hits = await search("localhost:9092", "logs", "payment failure", k=5)

    assert [h.offset for h in hits] == [42, 7]
    assert hits[0].score > hits[1].score
    assert all(isinstance(h, SearchHit) for h in hits)


@pytest.mark.asyncio
async def test_search_raises_on_http_error():
    with patch("aiohttp.ClientSession", return_value=_mock_session({}, status=404)):
        with pytest.raises(ConsumerError):
            await search("localhost:9092", "missing", "x")


@pytest.mark.asyncio
async def test_default_k_is_10():
    payload = {"hits": [], "took_ms": 0}
    session_cm = _mock_session(payload)
    with patch("aiohttp.ClientSession", return_value=session_cm):
        await search("localhost:9092", "logs", "q")

    inner_session = session_cm.__aenter__.return_value
    args, kwargs = inner_session.post.call_args
    assert kwargs["json"]["k"] == 10
