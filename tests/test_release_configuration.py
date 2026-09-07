"""Release-readiness assertions for CI and dependency automation."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_release_uses_trusted_publishing_and_mandatory_sbom() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert "pypa/gh-action-pypi-publish@release/v1" in workflow
    assert "id-token: write" in workflow
    assert "PYPI_API_TOKEN" not in workflow
    assert "twine upload" not in workflow
    assert "cyclonedx-py environment" in workflow
    assert "|| true" not in workflow
    assert "actions/attest-build-provenance@v2" in workflow
    assert "actions/attest-sbom@v2" in workflow
    assert "twine check dist/*" in workflow


def test_release_sbom_is_generated_from_a_clean_runtime_environment() -> None:
    """The SBOM must reflect the shipped wheel's runtime dependencies, not
    the build/release tooling used to produce it. See the regression this
    guards: a naive `cyclonedx-py environment` invocation in the same venv
    as `build`/`twine`/`cyclonedx-bom` lists those tools (and their many
    transitive dependencies) while omitting the wheel's actual runtime
    dependencies (aiokafka, cryptography) entirely. Even a *separate* venv
    created with a plain `python -m venv` (no `--without-pip`) still seeds
    its own pip, and pip itself then shows up as a scannable component --
    so the environment must be created with `--without-pip` and the wheel
    installed into it from the outside via `pip --python <target>`,
    which never installs (or requires) pip inside the target env.
    """
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    # The runtime-only environment never seeds its own pip: this is what
    # actually keeps `pip` itself out of the SBOM (a plain `python -m venv`
    # would install pip into the env, which cyclonedx-py would then list
    # as a component even if `build`/`twine`/`cyclonedx-bom` were never
    # installed there).
    assert "python -m venv --without-pip sbom-env" in workflow
    # The wheel is installed from the *outside*, via the tooling venv's
    # own pip pointed at the target interpreter -- never via a pip binary
    # living inside sbom-env (there isn't one).
    assert (
        'pip --python "$(pwd)/sbom-env/bin/python" install --quiet dist/*.whl'
        in workflow
    )
    assert "sbom-env/bin/pip" not in workflow
    assert "sbom-env/bin/pip install --quiet build" not in workflow
    assert "sbom-env/bin/pip install --quiet twine" not in workflow
    assert "sbom-env/bin/pip install --quiet cyclonedx" not in workflow
    # cyclonedx-py must be pointed at that clean environment explicitly,
    # not left to scan whatever environment it happens to run in.
    assert '"$(pwd)/sbom-env"' in workflow
    # And the generated SBOM must actually be inspected afterward, not
    # merely trusted to be clean because the environment was constructed
    # carefully.
    assert "python scripts/check_sbom.py sbom.cdx.json" in workflow


def test_sbom_guard_script_bans_build_tooling_and_requires_runtime_deps() -> None:
    """`scripts/check_sbom.py` is the actual enforcement point: it must
    reject pip/setuptools/wheel/build/twine/cyclonedx components and
    require the SDK's declared runtime dependencies to be present."""
    script = (ROOT / "scripts" / "check_sbom.py").read_text(encoding="utf-8")

    for banned in ("pip", "setuptools", "wheel", "build", "twine", "cyclonedx-bom"):
        assert f'"{banned}"' in script, f"{banned!r} must be in BANNED_COMPONENTS"

    for required in ("aiokafka", "cryptography"):
        assert f'"{required}"' in script, f"{required!r} must be in REQUIRED_COMPONENTS"


def test_release_publish_depends_on_live_conformance_and_attest() -> None:
    """`publish` must never run unless both the required live conformance
    suite and the build-provenance/SBOM attestation succeeded first."""
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert "needs: [build, conformance, attest]" in workflow
    assert "conformance:" in workflow
    # The executed-test-count guard (fails a run that executes zero
    # conformance tests) is enabled for the release's live run.
    assert "STREAMLINE_REQUIRE_CONFORMANCE: '1'" in workflow
    assert "-m conformance" in workflow


def test_release_conformance_requires_explicit_digest_pinned_image() -> None:
    """The release conformance job must hard-block (not silently skip) if
    no explicit, digest-pinned server image is configured, and must
    reject mutable tags such as ':latest' or ':0.4.0'."""
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert "STREAMLINE_CONFORMANCE_IMAGE" in workflow
    assert "@sha256:" in workflow
    assert "exit 1" in workflow
    assert "[0-9a-f]{64}" in workflow
    assert "is not an immutable digest reference" in workflow
    assert "tag-plus-digest aliases" in workflow
    # No hardcoded, unverified version tag is used as a fallback image.
    assert "streamline:0.4.0" not in workflow
    assert "streamline:latest" not in workflow


def test_integration_workflow_has_no_nonexistent_image_fallback() -> None:
    workflow = (ROOT / ".github" / "workflows" / "integration.yml").read_text(
        encoding="utf-8"
    )

    assert "STREAMLINE_CONFORMANCE_IMAGE" in workflow
    assert "[0-9a-f]{64}" in workflow
    assert "needs: resolve-image" in workflow
    assert "streamline:0.4.0" not in workflow
    assert "streamline:latest" not in workflow


def test_release_workflow_never_runs_concurrently() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert "concurrency:" in workflow
    assert "cancel-in-progress: false" in workflow


def test_ci_validates_all_nested_packages_without_publishing() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "testcontainers-package:" in workflow
    assert "python -m build testcontainers" in workflow
    assert "twine check testcontainers/dist/*" in workflow
    assert "embedded-package:" in workflow
    assert "cargo test --manifest-path streamline_embedded/Cargo.toml" in workflow
    assert "cargo package --manifest-path streamline_embedded/Cargo.toml" in workflow
    assert "--allow-dirty --no-verify" in workflow
    assert "maturin build --release" in workflow
    assert "twine check streamline_embedded/target/wheels/*" in workflow
    assert "publish" not in workflow.lower()


def test_dependabot_covers_root_and_nested_manifests() -> None:
    config = (ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")

    assert 'directory: "/"' in config
    assert 'directory: "/testcontainers"' in config
    assert 'directory: "/streamline_embedded"' in config
    assert 'package-ecosystem: "cargo"' in config
    assert config.count('directory: "/streamline_embedded"') == 2


def test_nested_packages_are_source_only_and_never_published() -> None:
    """Neither nested package (testcontainers, embedded) has a registry
    publish step anywhere in CI, and both READMEs must say so truthfully
    rather than advertising a `pip install <name>` / `cargo install`
    workflow that does not exist."""
    ci_workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "twine upload" not in ci_workflow
    assert "cargo publish" not in ci_workflow

    testcontainers_readme = (ROOT / "testcontainers" / "README.md").read_text(
        encoding="utf-8"
    )
    assert "pip install testcontainers-streamline" not in testcontainers_readme
    assert "pypi.org/project/testcontainers-streamline" not in testcontainers_readme
    assert "Source-only, unpublished" in testcontainers_readme
    assert "There is no `testcontainers-streamline` package on PyPI" in (
        testcontainers_readme
    )

    embedded_readme = (ROOT / "streamline_embedded" / "README.md").read_text(
        encoding="utf-8"
    )
    assert "pip install streamline-embedded\n" not in embedded_readme
    assert "source-only, unpublished" in embedded_readme
    assert "There is no `streamline-embedded` package on PyPI" in embedded_readme

    embedded_cargo_toml = (ROOT / "streamline_embedded" / "Cargo.toml").read_text(
        encoding="utf-8"
    )
    assert "publish = false" in embedded_cargo_toml


def test_testcontainers_requires_explicit_digest_pinned_image() -> None:
    """`StreamlineContainer` must never default to (or otherwise silently
    accept) a mutable, unverified image tag such as the nonexistent
    ``ghcr.io/streamlinelabs/streamline:0.3.0``."""
    container_source = (
        ROOT / "testcontainers" / "streamline_testcontainers" / "container.py"
    ).read_text(encoding="utf-8")

    assert 'image: str = "ghcr.io' not in container_source
    assert "_require_digest_pinned_image" in container_source
    assert "@sha256:" in container_source


def test_embedded_placeholder_storage_methods_never_fake_success() -> None:
    """Every placeholder storage operation in the embedded scaffold must
    raise rather than fabricate a successful result."""
    lib_rs = (ROOT / "streamline_embedded" / "src" / "lib.rs").read_text(
        encoding="utf-8"
    )

    assert "PyNotImplementedError" in lib_rs
    assert "ffi_not_implemented" in lib_rs
    for operation in [
        "create_topic",
        "delete_topic",
        "produce",
        "consume",
        "list_topics",
        "latest_offset",
        "flush",
    ]:
        assert f'ffi_not_implemented("{operation}")' in lib_rs, (
            f"{operation} must route through ffi_not_implemented(...) "
            "instead of returning a fabricated success value"
        )
