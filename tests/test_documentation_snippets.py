"""Executable and typechecked tests for documented code snippets."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from mypy import api as mypy_api

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
QUICKSTART = ROOT / "examples" / "readme_quickstart.py"
QUICKSTART_MARKER = "<!-- snippet-source: examples/readme_quickstart.py -->"
TRANSACTIONS = ROOT / "examples" / "readme_transactions.py"
TRANSACTIONS_MARKER = "<!-- snippet-source: examples/readme_transactions.py -->"


def _snippet_after_marker(marker: str) -> str:
    readme = README.read_text(encoding="utf-8")
    marker_index = readme.index(marker)
    fence_start = readme.index("```python\n", marker_index) + len("```python\n")
    fence_end = readme.index("\n```", fence_start)
    return readme[fence_start:fence_end] + "\n"


def _quickstart_snippet() -> str:
    return _snippet_after_marker(QUICKSTART_MARKER)


def _transactions_snippet() -> str:
    return _snippet_after_marker(TRANSACTIONS_MARKER)


def _transactions_note() -> str:
    """Return the "Transactions" section's prose note (after the code fence)."""
    readme = README.read_text(encoding="utf-8")
    marker_index = readme.index(TRANSACTIONS_MARKER)
    fence_end = readme.index("\n```", readme.index("```python\n", marker_index))
    next_heading = readme.index("\n### ", fence_end)
    return readme[fence_end:next_heading]


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_quickstart_module() -> ModuleType:
    return _load_module(QUICKSTART, "readme_quickstart_test")


def _load_transactions_module() -> ModuleType:
    return _load_module(TRANSACTIONS, "readme_transactions_test")


def test_readme_quickstart_matches_executable_example() -> None:
    assert _quickstart_snippet() == QUICKSTART.read_text(encoding="utf-8")


def test_readme_quickstart_typechecks() -> None:
    stdout, stderr, status = mypy_api.run(
        [
            "--config-file",
            os.devnull,
            "--python-version",
            "3.9",
            "--strict",
            "--ignore-missing-imports",
            "-c",
            _quickstart_snippet(),
        ]
    )
    assert status == 0, stdout + stderr


@pytest.mark.asyncio
async def test_readme_quickstart_executes_async_flow(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[str, Any]] = []

    class FakeProducer:
        async def send(self, topic: str, **kwargs: Any) -> Any:
            calls.append(("send", (topic, kwargs)))
            return type("Metadata", (), {"partition": 2, "offset": 42})()

    class FakeConsumer:
        async def __aenter__(self) -> FakeConsumer:
            calls.append(("consumer_enter", None))
            return self

        async def __aexit__(self, *args: Any) -> None:
            calls.append(("consumer_exit", None))

        async def subscribe(self, topics: list[str]) -> None:
            calls.append(("subscribe", topics))

        async def poll(self, timeout_ms: int) -> list[Any]:
            calls.append(("poll", timeout_ms))
            return [type("Message", (), {"value": b"Hello, Streamline!"})()]

    class FakeClient:
        def __init__(self, bootstrap_servers: str) -> None:
            calls.append(("client_init", bootstrap_servers))
            self.producer = FakeProducer()

        async def __aenter__(self) -> FakeClient:
            calls.append(("client_enter", None))
            return self

        async def __aexit__(self, *args: Any) -> None:
            calls.append(("client_exit", None))

        def consumer(self, group_id: str) -> FakeConsumer:
            calls.append(("consumer", group_id))
            return FakeConsumer()

    module = _load_quickstart_module()
    monkeypatch.setattr(module, "StreamlineClient", FakeClient)

    await module.main()

    assert ("subscribe", ["events"]) in calls
    assert ("poll", 1_000) in calls
    output = capsys.readouterr().out
    assert "partition=2 offset=42" in output
    assert "Hello, Streamline!" in output


# --- Transactions snippet -------------------------------------------------


def test_readme_transactions_matches_executable_example() -> None:
    assert _transactions_snippet() == TRANSACTIONS.read_text(encoding="utf-8")


def test_readme_transactions_typechecks() -> None:
    stdout, stderr, status = mypy_api.run(
        [
            "--config-file",
            os.devnull,
            "--python-version",
            "3.9",
            "--strict",
            "--ignore-missing-imports",
            "-c",
            _transactions_snippet(),
        ]
    )
    assert status == 0, stdout + stderr


def test_readme_transactions_wording_has_no_all_or_nothing_claim() -> None:
    """Regression: transactions section must not claim all-or-nothing delivery."""
    note = _transactions_note().lower()
    assert "all-or-nothing" not in note
    assert "atomic" in note  # must still say it is explicitly *not* atomic
    assert "not atomic" in note or "non-atomic" in note


def test_readme_transactions_wording_states_partial_delivery() -> None:
    """Regression: the transactions section must clearly state partial delivery."""
    note = _transactions_note().lower()
    assert "partial delivery" in note or "partially delivered" in note


def test_readme_transactions_wording_documents_abort_guard() -> None:
    """Regression: the note must call out guarding abort with in_transaction."""
    note = _transactions_note()
    assert "in_transaction" in note
    assert "abort_transaction" in note


def _make_transaction_fakes(
    *,
    fail_send_while_buffering: bool = False,
    fail_commit_after_buffering_exit: bool = False,
) -> tuple[type, list[tuple[str, Any]]]:
    """Build a FakeClient/FakeProducer pair that mimics the real Producer's
    transaction state machine closely enough to exercise the README's
    exception-handling guard: ``in_transaction`` flips to False *before*
    ``commit_transaction`` replays buffered sends (see producer.py), so a
    failure during that replay must not be masked by an unconditional
    ``abort_transaction()`` call afterward.
    """
    calls: list[tuple[str, Any]] = []

    class FakeProducer:
        def __init__(self) -> None:
            self._in_transaction = False

        @property
        def in_transaction(self) -> bool:
            return self._in_transaction

        async def begin_transaction(self) -> None:
            calls.append(("begin_transaction", None))
            self._in_transaction = True

        async def send(self, topic: str, **kwargs: Any) -> Any:
            calls.append(("send", (topic, kwargs)))
            if fail_send_while_buffering:
                raise RuntimeError("send failed while buffering")
            return type("Metadata", (), {"partition": 0, "offset": 0})()

        async def commit_transaction(self) -> list[Any]:
            # Mirrors the real implementation: buffering mode is exited
            # before replay, so a mid-replay failure leaves in_transaction
            # already False.
            self._in_transaction = False
            calls.append(("commit_transaction", None))
            if fail_commit_after_buffering_exit:
                raise RuntimeError("broker connection lost during replay")
            return []

        async def abort_transaction(self) -> None:
            calls.append(("abort_transaction", None))
            self._in_transaction = False

    class FakeClient:
        def __init__(self, bootstrap_servers: str) -> None:
            calls.append(("client_init", bootstrap_servers))
            self.producer = FakeProducer()

        async def __aenter__(self) -> FakeClient:
            calls.append(("client_enter", None))
            return self

        async def __aexit__(self, *args: Any) -> None:
            calls.append(("client_exit", None))

    return FakeClient, calls


@pytest.mark.asyncio
async def test_readme_transactions_happy_path_never_calls_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client_cls, calls = _make_transaction_fakes()
    module = _load_transactions_module()
    monkeypatch.setattr(module, "StreamlineClient", fake_client_cls)

    await module.main()

    assert ("commit_transaction", None) in calls
    assert not any(name == "abort_transaction" for name, _ in calls)


@pytest.mark.asyncio
async def test_readme_transactions_guards_abort_after_commit_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for the fix: once commit_transaction() has exited buffering
    mode and then fails, the snippet must NOT call abort_transaction() (which
    would raise "No transaction in progress" and mask the real failure). The
    original RuntimeError from commit_transaction must propagate unchanged.
    """
    fake_client_cls, calls = _make_transaction_fakes(
        fail_commit_after_buffering_exit=True
    )
    module = _load_transactions_module()
    monkeypatch.setattr(module, "StreamlineClient", fake_client_cls)

    with pytest.raises(RuntimeError, match="broker connection lost during replay"):
        await module.main()

    assert not any(name == "abort_transaction" for name, _ in calls)


@pytest.mark.asyncio
async def test_readme_transactions_aborts_on_early_failure_while_buffering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a failure happens *before* commit_transaction (still buffering),
    in_transaction is still True, so the guard must still call
    abort_transaction() to discard the buffered messages.
    """
    fake_client_cls, calls = _make_transaction_fakes(fail_send_while_buffering=True)
    module = _load_transactions_module()
    monkeypatch.setattr(module, "StreamlineClient", fake_client_cls)

    with pytest.raises(RuntimeError, match="send failed while buffering"):
        await module.main()

    assert ("abort_transaction", None) in calls
