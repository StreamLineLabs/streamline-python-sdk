"""Branched-streams support for the Python SDK (M5 P1, Experimental).

Subscribe to a branched view by passing ``branch="<name>"`` to ``Consumer``.

Internally this maps to the wire-protocol topic ``<topic>@branch=<name>``
which the broker routes through the COW reader.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_BRANCH_NAME_RE = re.compile(r"^[a-z0-9-]+$")


class BranchError(ValueError):
    """Raised for invalid branch names or operations."""


@dataclass(frozen=True)
class BranchedTopic:
    """A topic name + branch view."""

    topic: str
    branch: Optional[str] = None

    def __post_init__(self) -> None:
        if self.branch is not None and not _BRANCH_NAME_RE.match(self.branch):
            raise BranchError(
                f"branch name must match [a-z0-9-]+: got {self.branch!r}"
            )

    def wire_name(self) -> str:
        """The on-the-wire topic name: ``logs`` or ``logs@branch=exp-a``."""
        if self.branch is None:
            return self.topic
        return f"{self.topic}@branch={self.branch}"


def parse_wire_name(s: str) -> BranchedTopic:
    """Inverse of :meth:`BranchedTopic.wire_name`."""
    if "@branch=" in s:
        topic, branch = s.split("@branch=", 1)
        return BranchedTopic(topic, branch)
    return BranchedTopic(s)
