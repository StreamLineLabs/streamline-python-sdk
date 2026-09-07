"""URL construction helpers for dynamic API values."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote, urlencode

from .exceptions import ConfigurationError

_RESERVED_PATH_IDENTIFIERS = (".", "..")


def encode_path_segment(value: str) -> str:
    """Percent-encode one dynamic URL path segment.

    Raises:
        ConfigurationError: If ``value`` is exactly ``"."`` or ``".."``.
            These are reserved relative-path segments (RFC 3986 §3.3); HTTP
            clients built on aiohttp/yarl normalize them out of the final
            request URL during construction (e.g. a path segment of ``".."``
            deletes the previous path segment instead of being sent
            literally), even when percent-encoded, because yarl decodes and
            re-normalizes the path. An identifier equal to exactly "." or
            ".." would therefore silently redirect the request to a
            different endpoint than the one the caller specified, so it is
            rejected outright rather than encoded.
    """
    if value in _RESERVED_PATH_IDENTIFIERS:
        raise ConfigurationError(
            f"path identifier {value!r} is not allowed",
            hint=(
                "'.' and '..' are reserved relative-path segments that "
                "get normalized out of the final URL by HTTP client "
                "libraries (aiohttp/yarl); choose a different value"
            ),
        )
    return quote(value, safe="")


def append_query(path: str, params: Mapping[str, str | int]) -> str:
    """Append a safely encoded query string to a path."""
    return f"{path}?{urlencode(params)}"
