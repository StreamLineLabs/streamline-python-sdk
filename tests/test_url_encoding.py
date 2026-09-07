"""Regression tests for dynamic URL path and query encoding."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from yarl import URL

from streamline_sdk._url import append_query, encode_path_segment
from streamline_sdk.admin import Admin
from streamline_sdk.branches_admin import BranchAdminClient
from streamline_sdk.client import ClientConfig
from streamline_sdk.exceptions import ConfigurationError, TopicError
from streamline_sdk.search import SearchClient
from streamline_sdk.serializers import SchemaRegistryClient, SchemaRegistryConfig


def test_url_helpers_encode_reserved_characters() -> None:
    assert encode_path_segment("orders/2026 + west") == "orders%2F2026%20%2B%20west"
    assert append_query("/v1/branches", {"topic": "orders & returns"}) == (
        "/v1/branches?topic=orders+%26+returns"
    )


@pytest.mark.parametrize("identifier", [".", ".."])
def test_encode_path_segment_rejects_exact_dot_identifiers(identifier: str) -> None:
    with pytest.raises(ConfigurationError, match="not allowed"):
        encode_path_segment(identifier)


@pytest.mark.parametrize(
    "identifier",
    ["...", "..hidden", "a..b", "..2026", "hidden..", "x..y.."],
)
def test_encode_path_segment_allows_dot_containing_but_not_exact_identifiers(
    identifier: str,
) -> None:
    # None of these equal exactly "." or ".."; they must still be accepted
    # (percent-encoded, not blocked) since only the *exact* reserved
    # segments are special to URL normalization.
    encoded = encode_path_segment(identifier)
    assert encoded  # does not raise


def test_yarl_confirms_exact_dot_segments_are_normalized_away() -> None:
    """Documents *why* '.'/'..' must be rejected: yarl (which aiohttp is
    built on) collapses these exact path segments — even when
    percent-encoded — while leaving any other dot-containing segment
    untouched. This is the vulnerability the guard in encode_path_segment
    closes."""
    base = "http://broker.internal/v1/topics/"

    assert URL(base + "..").path == "/v1/"
    assert URL(base + ".").path == "/v1/topics/"
    assert URL(base + "%2e%2e").path == "/v1/"

    # Non-exact dot segments are left alone by yarl.
    assert URL(base + "...").path == "/v1/topics/..."
    assert URL(base + "a..b").path == "/v1/topics/a..b"


def test_encode_path_segment_output_survives_yarl_round_trip() -> None:
    """The percent-encoded segment produced for a legitimate, tricky
    identifier must appear verbatim (and decode back to the original) in
    the final URL that an aiohttp/yarl-based request would actually send —
    not just in the raw f-string this SDK builds."""
    tricky_identifiers = [
        "orders/2026 + west",
        "orders?priority=high",
        "orders#fragment",
        "orders%25percent",
        "a..b",
        "..hidden-topic",
    ]
    for identifier in tricky_identifiers:
        encoded = encode_path_segment(identifier)
        url = URL(f"http://broker.internal/v1/topics/{encoded}", encoded=True)
        # yarl must not have reinterpreted/normalized away any part of the
        # identifier: decoding the final URL's path segment recovers it.
        assert url.parts[-1] == identifier, (
            f"{identifier!r} did not survive the final aiohttp/yarl URL "
            f"round-trip (got {url.parts[-1]!r} from {url})"
        )


@pytest.mark.asyncio
async def test_admin_encodes_dynamic_path_segments_and_queries() -> None:
    admin = Admin(ClientConfig())
    admin._started = True
    get = AsyncMock(
        side_effect=[
            {
                "name": "orders/2026",
                "partitions": 1,
                "replication_factor": 1,
            },
            {"partitions": []},
            [],
            [],
        ]
    )
    delete = AsyncMock()

    with patch.object(admin._http, "get", get):
        with patch.object(admin._http, "delete", delete):
            await admin.describe_topic("orders/2026")
            await admin.consumer_group_topic_lag(
                "workers/eu",
                "orders + returns",
            )
            await admin.inspect_messages(
                "orders?priority=high",
                partition=2,
                offset=7,
                limit=5,
            )
            await admin.list_branches("orders & returns")
            await admin.discard_branch("orders/experiment one")

    assert get.await_args_list[0].args[0] == "/v1/topics/orders%2F2026"
    assert get.await_args_list[1].args[0] == (
        "/v1/consumer-groups/workers%2Feu/lag/orders%20%2B%20returns"
    )
    assert get.await_args_list[2].args[0] == (
        "/v1/inspect/orders%3Fpriority%3Dhigh?partition=2&limit=5&offset=7"
    )
    assert get.await_args_list[3].args[0] == ("/v1/branches?topic=orders+%26+returns")
    delete.assert_awaited_once_with("/v1/branches/orders%2Fexperiment%20one")


@pytest.mark.asyncio
async def test_branch_and_search_clients_encode_route_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch_paths: list[str] = []

    async def branch_request(
        self: BranchAdminClient,
        method: str,
        path: str,
        *,
        json_body: object | None = None,
    ) -> object:
        branch_paths.append(path)
        return {"id": "orders/experiment one"} if method == "GET" else {}

    monkeypatch.setattr(BranchAdminClient, "_request", branch_request)
    branch_client = BranchAdminClient()
    await branch_client.get("orders/experiment one")
    await branch_client.append("orders/experiment one", "user", "hello")

    search_paths: list[str] = []

    async def search_post(
        self: SearchClient,
        path: str,
        body: dict[str, object],
    ) -> dict[str, object]:
        search_paths.append(path)
        return {"hits": [], "took_ms": 0}

    monkeypatch.setattr(SearchClient, "_post", search_post)
    await SearchClient("http://localhost:9094").search(
        "orders/2026 + west",
        "query",
    )

    assert branch_paths == [
        "/api/v1/branches/orders%2Fexperiment%20one",
        "/api/v1/branches/orders%2Fexperiment%20one/messages",
    ]
    assert search_paths == ["/api/v1/topics/orders%2F2026%20%2B%20west/search"]


@pytest.mark.asyncio
async def test_schema_registry_encodes_subject_segment() -> None:
    response = AsyncMock()
    response.status = 404

    response_context = AsyncMock()
    response_context.__aenter__.return_value = response

    session = MagicMock()
    session.get.return_value = response_context

    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    client = SchemaRegistryClient(SchemaRegistryConfig(url="http://registry.example"))
    with patch("aiohttp.ClientSession", return_value=session_context):
        assert await client.get_versions("orders/value + v1") == []

    session.get.assert_called_once_with(
        "http://registry.example/subjects/orders%2Fvalue%20%2B%20v1/versions"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", [".", ".."])
async def test_admin_rejects_dot_identifiers_before_building_url(
    identifier: str,
) -> None:
    """Every Admin method that embeds a caller-supplied identifier in a
    dynamic URL path segment must reject exact '.'/'..' identifiers before
    ever issuing an HTTP request. ``describe_topic`` wraps the underlying
    error in ``TopicError`` (chaining the original ``ConfigurationError`` as
    its cause); the other methods propagate ``ConfigurationError`` directly.
    """
    admin = Admin(ClientConfig())
    admin._started = True
    get = AsyncMock()
    delete = AsyncMock()

    with (
        patch.object(admin._http, "get", get),
        patch.object(admin._http, "delete", delete),
    ):
        with pytest.raises(TopicError, match="not allowed") as describe_exc:
            await admin.describe_topic(identifier)
        assert isinstance(describe_exc.value.__cause__, ConfigurationError)

        with pytest.raises(ConfigurationError, match="not allowed"):
            await admin.consumer_group_lag(identifier)
        with pytest.raises(ConfigurationError, match="not allowed"):
            await admin.consumer_group_topic_lag("workers", identifier)
        with pytest.raises(ConfigurationError, match="not allowed"):
            await admin.inspect_messages(identifier, partition=0)
        with pytest.raises(ConfigurationError, match="not allowed"):
            await admin.discard_branch(identifier)

    get.assert_not_awaited()
    delete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", [".", ".."])
async def test_branch_and_search_clients_reject_dot_identifiers(
    identifier: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    branch_request = AsyncMock()
    monkeypatch.setattr(BranchAdminClient, "_request", branch_request)
    branch_client = BranchAdminClient()
    with pytest.raises(ConfigurationError, match="not allowed"):
        await branch_client.get(identifier)
    with pytest.raises(ConfigurationError, match="not allowed"):
        await branch_client.delete(identifier)
    branch_request.assert_not_awaited()

    search_post = AsyncMock()
    monkeypatch.setattr(SearchClient, "_post", search_post)
    with pytest.raises(ConfigurationError, match="not allowed"):
        await SearchClient("http://localhost:9094").search(identifier, "query")
    search_post.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", [".", ".."])
async def test_schema_registry_rejects_dot_subject_identifiers(
    identifier: str,
) -> None:
    """The schema subject is validated before any HTTP request is issued.
    ``aiohttp.ClientSession()`` itself may still be opened as a context
    manager (it performs no I/O on its own), but no request method
    (``get``/``post``) may ever be called with an unencoded/unsafe path."""
    response_context = AsyncMock()
    session = MagicMock()
    session.get.return_value = response_context
    session.post.return_value = response_context
    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    client = SchemaRegistryClient(SchemaRegistryConfig(url="http://registry.example"))
    with patch("aiohttp.ClientSession", return_value=session_context):
        with pytest.raises(ConfigurationError, match="not allowed"):
            await client.get_versions(identifier)
        with pytest.raises(ConfigurationError, match="not allowed"):
            await client.register_schema(identifier, "{}")
        with pytest.raises(ConfigurationError, match="not allowed"):
            await client.check_compatibility(identifier, "{}")

    session.get.assert_not_called()
    session.post.assert_not_called()
