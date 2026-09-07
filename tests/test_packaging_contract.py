"""Regression tests for the packaging / import contract of ``streamline_sdk``.

These guard the two failure modes that broke the verification baseline:

1. Python 3.9 raises at *import* time when PEP 604 (``str | None``) or PEP 585
   (``list[str]``) annotations are evaluated at runtime, so every runtime module
   must carry ``from __future__ import annotations``.
2. ``streamline_sdk/__init__.py`` re-exports symbols from modules that import
   third-party packages (e.g. ``cryptography`` via ``streamline_sdk.verifier``),
   so those packages must be declared as *runtime* dependencies.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import pkgutil
from importlib.metadata import metadata, requires
from pathlib import Path

import pytest

import streamline_sdk

PACKAGE_ROOT = Path(streamline_sdk.__file__).resolve().parent
DISTRIBUTION = "streamline-sdk"

#: Builtins that became subscriptable only in Python 3.9/3.10 depending on
#: context; using them in a runtime-evaluated annotation breaks Python 3.9.
_PEP585_BUILTINS = {"list", "dict", "set", "tuple", "frozenset", "type"}


def _module_files() -> list[Path]:
    return sorted(PACKAGE_ROOT.glob("*.py"))


def _module_names() -> list[str]:
    return sorted(m.name for m in pkgutil.iter_modules([str(PACKAGE_ROOT)]))


def _normalize(name: str) -> str:
    return name.replace("_", "-").lower()


class _AnnotationScanner(ast.NodeVisitor):
    """Collect annotations that Python 3.9 cannot evaluate at runtime."""

    def __init__(self) -> None:
        self.hits: list[str] = []

    def _scan(self, node: ast.AST, where: str) -> None:
        for sub in ast.walk(node):
            if isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.BitOr):
                self.hits.append(f"line {sub.lineno}: PEP 604 union in {where}")
            if (
                isinstance(sub, ast.Subscript)
                and isinstance(sub.value, ast.Name)
                and sub.value.id in _PEP585_BUILTINS
            ):
                self.hits.append(
                    f"line {sub.lineno}: PEP 585 {sub.value.id}[...] in {where}"
                )

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        args = node.args
        for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]:
            if arg.annotation is not None:
                self._scan(arg.annotation, node.name)
        for extra in (args.vararg, args.kwarg):
            if extra is not None and extra.annotation is not None:
                self._scan(extra.annotation, node.name)
        if node.returns is not None:
            self._scan(node.returns, node.name)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._scan(node.annotation, "variable annotation")
        self.generic_visit(node)


def _has_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


@pytest.mark.parametrize("path", _module_files(), ids=lambda p: p.name)
def test_runtime_modules_declare_future_annotations(path: Path) -> None:
    """Every SDK module must opt in to PEP 563 string annotations."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    assert _has_future_annotations(tree), (
        f"{path.name} is missing 'from __future__ import annotations'; "
        "modern annotations are evaluated at runtime on Python 3.9 and "
        "raise TypeError at import time"
    )


@pytest.mark.parametrize("path", _module_files(), ids=lambda p: p.name)
def test_no_python39_incompatible_runtime_annotations(path: Path) -> None:
    """Modern annotation syntax is only safe behind the future import."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    if _has_future_annotations(tree):
        return

    scanner = _AnnotationScanner()
    scanner.visit(tree)
    assert not scanner.hits, (
        f"{path.name} uses Python 3.10+ annotation syntax without "
        f"'from __future__ import annotations': {scanner.hits}"
    )


@pytest.mark.parametrize("module_name", _module_names())
def test_every_submodule_is_importable(module_name: str) -> None:
    """Importing any submodule must not fail on an undeclared dependency."""
    importlib.import_module(f"streamline_sdk.{module_name}")


def test_public_api_names_are_all_exported() -> None:
    """Every name in ``__all__`` must resolve on the package."""
    missing = [
        name for name in streamline_sdk.__all__ if not hasattr(streamline_sdk, name)
    ]
    assert missing == [], f"declared in __all__ but not importable: {missing}"


def test_attestation_verifier_is_publicly_importable() -> None:
    """Regression: ``verifier`` pulls in ``cryptography`` at import time."""
    from streamline_sdk import AttestationVerificationResult, StreamlineVerifier

    assert StreamlineVerifier.__module__ == "streamline_sdk.verifier"
    assert AttestationVerificationResult.__module__ == "streamline_sdk.verifier"


def _runtime_requirements() -> set[str]:
    """Distribution names required unconditionally (no ``extra ==`` marker)."""
    declared = requires(DISTRIBUTION) or []
    runtime = set()
    for requirement in declared:
        spec, _, marker = requirement.partition(";")
        if "extra ==" in marker:
            continue
        name = spec.strip().split("[")[0]
        for delimiter in ("=", "<", ">", "!", "~", " ", "("):
            name = name.split(delimiter)[0]
        if name:
            runtime.add(_normalize(name))
    return runtime


def _third_party_module_level_imports() -> set[str]:
    """Top-level packages imported unconditionally by SDK modules."""
    first_party = {"streamline_sdk", "streamline_embedded"}
    found: set[str] = set()

    for path in _module_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:  # module level only; guarded imports live in Try
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:  # relative import -> first party
                    continue
                names = [node.module or ""]
            else:
                continue

            for name in names:
                top = name.split(".")[0]
                if not top or top in first_party or top == "__future__":
                    continue
                spec = importlib.util.find_spec(top)
                origin = getattr(spec, "origin", None) or ""
                if "site-packages" in origin or "dist-packages" in origin:
                    found.add(_normalize(top))
    return found


def test_unconditional_third_party_imports_are_declared_runtime_deps() -> None:
    """Anything imported at module scope must be installable via ``pip install``."""
    undeclared = _third_party_module_level_imports() - _runtime_requirements()
    assert undeclared == set(), (
        "third-party packages imported at module scope but missing from "
        f"[project].dependencies: {sorted(undeclared)}"
    )


def test_cryptography_is_a_declared_runtime_dependency() -> None:
    """Regression: the public API re-exports ``StreamlineVerifier``."""
    assert "cryptography" in _runtime_requirements()


def test_distribution_still_supports_python_39() -> None:
    """The ``requires-python`` contract must keep Python 3.9 in range."""
    requires_python = metadata(DISTRIBUTION)["Requires-Python"]
    assert requires_python is not None
    assert "3.9" in requires_python


def test_runtime_version_matches_distribution_metadata() -> None:
    """The importable version must match the installed package metadata."""
    assert streamline_sdk.__version__ == metadata(DISTRIBUTION)["Version"]
