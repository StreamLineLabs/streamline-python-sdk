"""Topic name validation for the Streamline SDK."""

from __future__ import annotations

import re

from .exceptions import StreamlineError

_TOPIC_NAME_MAX_LENGTH = 249
_TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")


class ConfigurationError(StreamlineError):
    """Raised when a configuration value is invalid."""

    pass


def validate_topic_name(topic: str) -> None:
    """Validate a Kafka topic name.

    Rules:
    - Cannot be empty
    - Maximum 249 characters
    - Only alphanumeric characters, '.', '_', and '-' are allowed
    - Cannot be '.' or '..'

    Args:
        topic: The topic name to validate.

    Raises:
        ConfigurationError: If the topic name is invalid.
    """
    if not topic:
        raise ConfigurationError(
            "Topic name cannot be empty",
            hint=(
                "Provide a non-empty topic name using "
                "alphanumeric characters, '.', '_', or '-'"
            ),
        )

    if len(topic) > _TOPIC_NAME_MAX_LENGTH:
        raise ConfigurationError(
            f"Topic name exceeds maximum length of "
            f"{_TOPIC_NAME_MAX_LENGTH} characters (got {len(topic)})",
            hint=f"Shorten the topic name to at most "
            f"{_TOPIC_NAME_MAX_LENGTH} characters",
        )

    if topic in (".", ".."):
        raise ConfigurationError(
            f"Topic name cannot be '{topic}'",
            hint="'.' and '..' are reserved and cannot be used as topic names",
        )

    if not _TOPIC_NAME_PATTERN.match(topic):
        invalid_chars = set(
            ch for ch in topic if not re.match(r"[a-zA-Z0-9._-]", ch)
        )
        raise ConfigurationError(
            f"Topic name contains invalid characters: {invalid_chars}",
            hint="Topic names may only contain alphanumeric "
            "characters, '.', '_', and '-'",
        )
