"""Characterization tests for ``_AdminHttpTransport.get`` / ``post`` /
``delete`` transport behavior.

These tests pin down the *current* behavior of the two transport branches
inside ``streamline_sdk._admin_http``:

* the ``aiohttp``-backed branch (``HAS_AIOHTTP`` truthy), and
* the ``urllib``-based fallback branch (``HAS_AIOHTTP`` falsy).

They exercise ``_AdminHttpTransport`` directly (the private module that
``streamline_sdk.admin.Admin`` delegates to for all HTTP transport). No
real network I/O is performed — both branches are driven through small,
deterministic fakes/monkeypatches of ``aiohttp`` and ``urllib.request``.
"""

from __future__ import annotations

import email.message
import json
import urllib.error
import urllib.request
from typing import Any, Callable

import pytest

from streamline_sdk import _admin_http as admin_module
from streamline_sdk._admin_http import _AdminHttpTransport
from streamline_sdk.exceptions import TopicError

HTTP_URL = "http://example-host:9094"


def _make_transport() -> _AdminHttpTransport:
    return _AdminHttpTransport(HTTP_URL)


# --------------------------------------------------------------------- #
# aiohttp branch fakes
# --------------------------------------------------------------------- #

# A recorded call: (method, url, timeout_total, json_body)
RecordedCall = tuple[str, str, Any, Any]
ResponseFactory = Callable[[str, str, Any], "_FakeAiohttpResponse"]


class _FakeClientTimeout:
    """Stand-in for ``aiohttp.ClientTimeout`` that records ``total``."""

    def __init__(self, total: float | None = None) -> None:
        self.total = total


class _FakeAiohttpResponse:
    """Stand-in for ``aiohttp.ClientResponse``."""

    def __init__(self, status: int, json_body: Any = None, text_body: str = "") -> None:
        self.status = status
        self._json_body = json_body
        self._text_body = text_body

    async def json(self) -> Any:
        return self._json_body

    async def text(self) -> str:
        return self._text_body


class _FakeResponseContext:
    """Async context manager wrapping a ``_FakeAiohttpResponse``."""

    def __init__(self, response: _FakeAiohttpResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeAiohttpResponse:
        return self._response

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakeClientSession:
    """Stand-in for ``aiohttp.ClientSession``."""

    def __init__(
        self, calls: list[RecordedCall], response_factory: ResponseFactory
    ) -> None:
        self._calls = calls
        self._response_factory = response_factory

    async def __aenter__(self) -> _FakeClientSession:
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    def get(
        self, url: str, timeout: _FakeClientTimeout | None = None
    ) -> _FakeResponseContext:
        total = timeout.total if timeout is not None else None
        self._calls.append(("GET", url, total, None))
        return _FakeResponseContext(self._response_factory("GET", url, None))

    def post(
        self,
        url: str,
        json: Any = None,
        timeout: _FakeClientTimeout | None = None,
    ) -> _FakeResponseContext:
        total = timeout.total if timeout is not None else None
        self._calls.append(("POST", url, total, json))
        return _FakeResponseContext(self._response_factory("POST", url, json))

    def delete(
        self, url: str, timeout: _FakeClientTimeout | None = None
    ) -> _FakeResponseContext:
        total = timeout.total if timeout is not None else None
        self._calls.append(("DELETE", url, total, None))
        return _FakeResponseContext(self._response_factory("DELETE", url, None))


class _FakeAiohttpModule:
    """Stand-in for the ``aiohttp`` module, exposing just what _admin_http.py uses."""

    def __init__(
        self, calls: list[RecordedCall], response_factory: ResponseFactory
    ) -> None:
        self.ClientTimeout = _FakeClientTimeout
        self._calls = calls
        self._response_factory = response_factory

    def ClientSession(self) -> _FakeClientSession:  # noqa: N802 (matches aiohttp API)
        return _FakeClientSession(self._calls, self._response_factory)


@pytest.fixture
def install_fake_aiohttp(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[ResponseFactory], list[RecordedCall]]:
    """Install a fake ``aiohttp`` module into ``_admin_http`` and force the
    aiohttp branch to be taken, regardless of whether real aiohttp is
    installed in the test environment."""

    def _install(response_factory: ResponseFactory) -> list[RecordedCall]:
        calls: list[RecordedCall] = []
        fake_module = _FakeAiohttpModule(calls, response_factory)
        monkeypatch.setattr(admin_module, "aiohttp", fake_module, raising=False)
        monkeypatch.setattr(admin_module, "HAS_AIOHTTP", True)
        return calls

    return _install


# --------------------------------------------------------------------- #
# aiohttp branch: get
# --------------------------------------------------------------------- #


class TestHttpGetAiohttp:
    async def test_success_url_method_timeout_and_json_decode(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        calls = install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(
                200, json_body={"topics": ["a", "b"]}
            )
        )
        transport = _make_transport()

        result = await transport.get("/v1/topics")

        assert result == {"topics": ["a", "b"]}
        assert len(calls) == 1
        method, url, timeout_total, body = calls[0]
        assert method == "GET"
        assert url == f"{HTTP_URL}/v1/topics"
        assert timeout_total == 10
        assert body is None

    async def test_404_raises_topic_error_not_found(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(lambda method, url, body: _FakeAiohttpResponse(404))
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.get("/v1/topics/missing")

        assert str(exc_info.value).startswith("Not found: /v1/topics/missing")

    async def test_other_error_status_raises_http_error_with_body(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(
                500, text_body="internal boom"
            )
        )
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.get("/v1/topics")

        assert str(exc_info.value).startswith("HTTP 500: internal boom")


# --------------------------------------------------------------------- #
# aiohttp branch: post
# --------------------------------------------------------------------- #


class TestHttpPostAiohttp:
    async def test_success_sends_json_body_and_decodes_response(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        calls = install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(200, json_body={"id": 1})
        )
        transport = _make_transport()
        body = {"name": "exp-a", "base_topic": "orders"}

        result = await transport.post("/v1/branches", body)

        assert result == {"id": 1}
        method, url, timeout_total, sent_body = calls[0]
        assert method == "POST"
        assert url == f"{HTTP_URL}/v1/branches"
        assert timeout_total == 10
        assert sent_body == body

    @pytest.mark.parametrize("accepted_status", [200, 201])
    async def test_200_and_201_are_both_accepted(
        self,
        install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]],
        accepted_status: int,
    ) -> None:
        install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(
                accepted_status, json_body={"ok": True}
            )
        )
        transport = _make_transport()

        result = await transport.post("/v1/branches", {"name": "x"})

        assert result == {"ok": True}

    async def test_404_raises_topic_error_not_found(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(lambda method, url, body: _FakeAiohttpResponse(404))
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.post("/v1/branches", {"name": "x"})

        assert str(exc_info.value).startswith("Not found: /v1/branches")

    async def test_other_error_status_raises_http_error_with_body(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(400, text_body="bad body")
        )
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.post("/v1/branches", {"name": "x"})

        assert str(exc_info.value).startswith("HTTP 400: bad body")


# --------------------------------------------------------------------- #
# aiohttp branch: delete
# --------------------------------------------------------------------- #


class TestHttpDeleteAiohttp:
    async def test_success_returns_none(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        calls = install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(200)
        )
        transport = _make_transport()

        # `delete` is annotated to return ``None`` on success.
        await transport.delete("/v1/branches/exp-a")

        method, url, timeout_total, _ = calls[0]
        assert method == "DELETE"
        assert url == f"{HTTP_URL}/v1/branches/exp-a"
        assert timeout_total == 10

    async def test_404_raises_topic_error_not_found(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(lambda method, url, body: _FakeAiohttpResponse(404))
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.delete("/v1/branches/missing")

        assert str(exc_info.value).startswith("Not found: /v1/branches/missing")

    async def test_other_error_status_raises_http_error_with_body(
        self, install_fake_aiohttp: Callable[[ResponseFactory], list[RecordedCall]]
    ) -> None:
        install_fake_aiohttp(
            lambda method, url, body: _FakeAiohttpResponse(503, text_body="down")
        )
        transport = _make_transport()

        with pytest.raises(TopicError) as exc_info:
            await transport.delete("/v1/branches/exp-a")

        assert str(exc_info.value).startswith("HTTP 503: down")


# --------------------------------------------------------------------- #
# urllib fallback branch (HAS_AIOHTTP forced False)
# --------------------------------------------------------------------- #


class _FakeUrllibResponse:
    """Stand-in for the context manager returned by ``urlopen``."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeUrllibResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


#: Handler invoked in place of the real ``urllib.request.urlopen``.
UrlopenHandler = Callable[[urllib.request.Request, "int | None"], Any]


@pytest.fixture
def force_urllib_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the urllib fallback branch regardless of whether aiohttp is
    actually installed in the test environment."""
    monkeypatch.setattr(admin_module, "HAS_AIOHTTP", False)


@pytest.fixture
def fake_urlopen(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[UrlopenHandler], list[Any]]:
    """Patch ``urllib.request.urlopen`` and record every ``Request`` passed
    to it, along with the ``timeout`` kwarg."""

    def _install(handler: UrlopenHandler) -> list[Any]:
        calls: list[Any] = []

        def _fake_urlopen(
            req: urllib.request.Request, timeout: int | None = None
        ) -> Any:
            calls.append((req, timeout))
            return handler(req, timeout)

        monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
        return calls

    return _install


@pytest.mark.usefixtures("force_urllib_fallback")
class TestHttpGetUrllibFallback:
    async def test_request_method_url_timeout_and_json_decode(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        calls = fake_urlopen(
            lambda req, timeout: _FakeUrllibResponse(
                json.dumps({"topics": ["a"]}).encode("utf-8")
            )
        )
        transport = _make_transport()

        result = await transport.get("/v1/topics")

        assert result == {"topics": ["a"]}
        assert len(calls) == 1
        req, timeout = calls[0]
        assert req.get_method() == "GET"
        assert req.full_url == f"{HTTP_URL}/v1/topics"
        assert timeout == 10

    async def test_url_error_propagates_uncaught(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        def _raise(req: urllib.request.Request, timeout: int | None) -> Any:
            raise urllib.error.URLError("connection refused")

        fake_urlopen(_raise)
        transport = _make_transport()

        # Characterizes current behavior: unlike the aiohttp branch, the
        # urllib fallback does not translate errors into TopicError — the
        # raw urllib error propagates unchanged.
        with pytest.raises(urllib.error.URLError):
            await transport.get("/v1/topics")

    async def test_http_error_propagates_uncaught(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        def _raise(req: urllib.request.Request, timeout: int | None) -> Any:
            raise urllib.error.HTTPError(
                req.full_url, 404, "Not Found", email.message.Message(), None
            )

        fake_urlopen(_raise)
        transport = _make_transport()

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            await transport.get("/v1/topics/missing")
        assert exc_info.value.code == 404

    async def test_uses_asyncio_to_thread(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        fake_urlopen(
            lambda req, timeout: _FakeUrllibResponse(json.dumps({}).encode("utf-8"))
        )
        transport = _make_transport()

        to_thread_calls: list[Any] = []
        real_to_thread = admin_module.asyncio.to_thread

        async def _recording_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
            to_thread_calls.append(func)
            return await real_to_thread(func, *args, **kwargs)

        monkeypatch.setattr(admin_module.asyncio, "to_thread", _recording_to_thread)

        await transport.get("/v1/topics")

        assert len(to_thread_calls) == 1


@pytest.mark.usefixtures("force_urllib_fallback")
class TestHttpPostUrllibFallback:
    async def test_request_method_body_content_type_and_json_decode(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        calls = fake_urlopen(
            lambda req, timeout: _FakeUrllibResponse(
                json.dumps({"id": 42}).encode("utf-8")
            )
        )
        transport = _make_transport()
        body = {"name": "exp-a", "base_topic": "orders"}

        result = await transport.post("/v1/branches", body)

        assert result == {"id": 42}
        req, timeout = calls[0]
        assert req.get_method() == "POST"
        assert req.full_url == f"{HTTP_URL}/v1/branches"
        assert req.get_header("Content-type") == "application/json"
        assert json.loads(req.data.decode("utf-8")) == body
        assert timeout == 10

    async def test_url_error_propagates_uncaught(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        def _raise(req: urllib.request.Request, timeout: int | None) -> Any:
            raise urllib.error.URLError("boom")

        fake_urlopen(_raise)
        transport = _make_transport()

        with pytest.raises(urllib.error.URLError):
            await transport.post("/v1/branches", {"name": "x"})

    async def test_uses_asyncio_to_thread(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        fake_urlopen(
            lambda req, timeout: _FakeUrllibResponse(json.dumps({}).encode("utf-8"))
        )
        transport = _make_transport()

        to_thread_calls: list[Any] = []
        real_to_thread = admin_module.asyncio.to_thread

        async def _recording_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
            to_thread_calls.append(func)
            return await real_to_thread(func, *args, **kwargs)

        monkeypatch.setattr(admin_module.asyncio, "to_thread", _recording_to_thread)

        await transport.post("/v1/branches", {"name": "x"})

        assert len(to_thread_calls) == 1


@pytest.mark.usefixtures("force_urllib_fallback")
class TestHttpDeleteUrllibFallback:
    async def test_request_method_url_and_success_returns_none(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        calls = fake_urlopen(lambda req, timeout: _FakeUrllibResponse(b""))
        transport = _make_transport()

        # `delete` is annotated to return ``None`` on success.
        await transport.delete("/v1/branches/exp-a")

        req, timeout = calls[0]
        assert req.get_method() == "DELETE"
        assert req.full_url == f"{HTTP_URL}/v1/branches/exp-a"
        assert timeout == 10

    async def test_url_error_propagates_uncaught(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        def _raise(req: urllib.request.Request, timeout: int | None) -> Any:
            raise urllib.error.URLError("boom")

        fake_urlopen(_raise)
        transport = _make_transport()

        with pytest.raises(urllib.error.URLError):
            await transport.delete("/v1/branches/exp-a")

    async def test_http_error_propagates_uncaught(
        self,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        def _raise(req: urllib.request.Request, timeout: int | None) -> Any:
            raise urllib.error.HTTPError(
                req.full_url, 500, "Server Error", email.message.Message(), None
            )

        fake_urlopen(_raise)
        transport = _make_transport()

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            await transport.delete("/v1/branches/exp-a")
        assert exc_info.value.code == 500

    async def test_uses_asyncio_to_thread(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_urlopen: Callable[[UrlopenHandler], list[Any]],
    ) -> None:
        fake_urlopen(lambda req, timeout: _FakeUrllibResponse(b""))
        transport = _make_transport()

        to_thread_calls: list[Any] = []
        real_to_thread = admin_module.asyncio.to_thread

        async def _recording_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
            to_thread_calls.append(func)
            return await real_to_thread(func, *args, **kwargs)

        monkeypatch.setattr(admin_module.asyncio, "to_thread", _recording_to_thread)

        await transport.delete("/v1/branches/exp-a")

        assert len(to_thread_calls) == 1


def test_transport_resolves_dynamic_base_url():
    current = {"url": "http://first:9094"}
    transport = _AdminHttpTransport(lambda: current["url"])

    assert transport._current_base_url() == "http://first:9094"
    current["url"] = "http://second:9094"
    assert transport._current_base_url() == "http://second:9094"
