"""Functional tests for the SBOM inspection guard (`scripts/check_sbom.py`).

These exercise the actual parsing/checking logic (not just static text
matching against the release workflow), to guard against a runtime SBOM
that leaks packaging/build tooling (pip, setuptools, wheel, build, twine,
cyclonedx-bom/cyclonedx-python-lib) or omits the SDK's declared runtime
dependencies (aiokafka, cryptography).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_sbom.py"


def _load_check_sbom_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_sbom", SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"could not load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sbom_with_components(names: list[str]) -> dict:
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "components": [
            {"type": "library", "name": name, "version": "0.0.0"} for name in names
        ],
    }


def _write_sbom(tmp_path: Path, names: list[str]) -> Path:
    sbom_path = tmp_path / "sbom.cdx.json"
    sbom_path.write_text(json.dumps(_sbom_with_components(names)), encoding="utf-8")
    return sbom_path


def test_check_sbom_passes_for_clean_runtime_dependencies(tmp_path: Path) -> None:
    module = _load_check_sbom_module()
    sbom_path = _write_sbom(
        tmp_path,
        ["aiokafka", "cryptography", "cffi", "pycparser", "typing_extensions"],
    )

    assert module.check_sbom(sbom_path) == []


@pytest.mark.parametrize(
    "banned",
    [
        "pip",
        "setuptools",
        "wheel",
        "build",
        "twine",
        "cyclonedx-bom",
        "cyclonedx-python-lib",
    ],
)
def test_check_sbom_fails_when_build_tooling_present(
    tmp_path: Path, banned: str
) -> None:
    module = _load_check_sbom_module()
    sbom_path = _write_sbom(tmp_path, ["aiokafka", "cryptography", banned])

    problems = module.check_sbom(sbom_path)
    assert problems, f"expected a problem to be reported for {banned!r}"
    assert any(banned in problem for problem in problems)


def test_check_sbom_is_case_insensitive_for_banned_components(tmp_path: Path) -> None:
    module = _load_check_sbom_module()
    sbom_path = _write_sbom(tmp_path, ["aiokafka", "cryptography", "PIP"])

    problems = module.check_sbom(sbom_path)
    assert problems


def test_check_sbom_fails_when_required_runtime_dependency_missing(
    tmp_path: Path,
) -> None:
    module = _load_check_sbom_module()
    # aiokafka is missing entirely.
    sbom_path = _write_sbom(tmp_path, ["cryptography"])

    problems = module.check_sbom(sbom_path)
    assert problems
    assert any("aiokafka" in problem for problem in problems)


def test_check_sbom_reports_both_problems_simultaneously(tmp_path: Path) -> None:
    module = _load_check_sbom_module()
    # Build tooling present *and* a required runtime dep missing.
    sbom_path = _write_sbom(tmp_path, ["pip", "cryptography"])

    problems = module.check_sbom(sbom_path)
    assert len(problems) == 2


def test_check_sbom_cli_exits_nonzero_on_failure(tmp_path: Path) -> None:
    module = _load_check_sbom_module()
    sbom_path = _write_sbom(tmp_path, ["pip", "aiokafka", "cryptography"])

    assert module.main(["check_sbom.py", str(sbom_path)]) == 1


def test_check_sbom_cli_exits_zero_on_success(tmp_path: Path) -> None:
    module = _load_check_sbom_module()
    sbom_path = _write_sbom(tmp_path, ["aiokafka", "cryptography"])

    assert module.main(["check_sbom.py", str(sbom_path)]) == 0


def test_check_sbom_cli_requires_exactly_one_argument() -> None:
    module = _load_check_sbom_module()

    assert module.main(["check_sbom.py"]) == 2
    assert module.main(["check_sbom.py", "a", "b"]) == 2


def test_check_sbom_cli_fails_gracefully_on_missing_file(tmp_path: Path) -> None:
    module = _load_check_sbom_module()

    assert module.main(["check_sbom.py", str(tmp_path / "missing.json")]) == 1
