"""Regression tests for the default-run isolation of the test suite.

``pytest tests/`` must be self-contained: every test that needs a live
Streamline server has to be marked (``integration`` / ``conformance``) so the
gate in ``tests/conftest.py`` can skip it instead of hanging on localhost.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from conftest import (
    CONFORMANCE_ENV_VAR,
    INTEGRATION_ENV_VAR,
    REQUIRE_CONFORMANCE_ENV_VAR,
    conformance_enabled,
    conformance_required,
    integration_enabled,
)

TESTS_ROOT = Path(__file__).resolve().parent

SERVER_DEPENDENT_MODULES = {
    TESTS_ROOT / "test_conformance.py": "integration",
    TESTS_ROOT / "conformance" / "test_conformance.py": "conformance",
}


def _load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"_gating_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _marker_names(module: ModuleType) -> list[str]:
    pytestmark = getattr(module, "pytestmark", [])
    marks = pytestmark if isinstance(pytestmark, list) else [pytestmark]
    return [mark.name for mark in marks]


@pytest.mark.parametrize(
    ("path", "marker"),
    sorted(SERVER_DEPENDENT_MODULES.items()),
    ids=lambda value: value.name if isinstance(value, Path) else str(value),
)
def test_server_dependent_modules_are_marked(path: Path, marker: str) -> None:
    """Server-dependent modules must carry their gating marker."""
    assert path.exists(), f"missing server-dependent module: {path}"
    assert marker in _marker_names(_load(path)), (
        f"{path.name} must declare pytest.mark.{marker} so the default "
        "run skips it instead of connecting to localhost"
    )


def test_markers_are_registered_in_pyproject() -> None:
    """Both gating markers must be registered to avoid PytestUnknownMarkWarning."""
    pyproject = (TESTS_ROOT.parent / "pyproject.toml").read_text(encoding="utf-8")
    assert "integration: marks tests as integration tests" in pyproject
    assert "conformance: marks tests as conformance-suite tests" in pyproject


@pytest.mark.parametrize(
    ("env_var", "gate"),
    [
        (INTEGRATION_ENV_VAR, integration_enabled),
        (CONFORMANCE_ENV_VAR, conformance_enabled),
    ],
)
def test_gate_is_disabled_unless_env_var_is_truthy(
    monkeypatch: pytest.MonkeyPatch, env_var: str, gate
) -> None:
    """The gates default to off and only accept explicit opt-in values."""
    monkeypatch.delenv(env_var, raising=False)
    assert gate() is False

    for falsey in ("", "0", "false", "no"):
        monkeypatch.setenv(env_var, falsey)
        assert gate() is False

    for truthy in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv(env_var, truthy)
        assert gate() is True


def test_required_conformance_gate_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(REQUIRE_CONFORMANCE_ENV_VAR, raising=False)
    assert conformance_required() is False

    monkeypatch.setenv(REQUIRE_CONFORMANCE_ENV_VAR, "1")
    assert conformance_required() is True


def test_ci_conformance_job_cannot_use_default_skip_gate() -> None:
    workflow = (
        TESTS_ROOT.parent / ".github" / "workflows" / "integration.yml"
    ).read_text(encoding="utf-8")
    assert "conformance:" in workflow
    assert "CONFORMANCE: '1'" in workflow
    assert "STREAMLINE_REQUIRE_CONFORMANCE: '1'" in workflow
