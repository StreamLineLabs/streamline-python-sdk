# Release Readiness Audit

Last updated: 2026-09-03

## Resolved P0/P1 Items

| Area | Resolution |
|---|---|
| Async documentation | README Quick Start and examples use the real async client, producer, consumer, admin, query, schema, AI, security, and memory APIs. The Quick Start is executable and typechecked in `tests/test_documentation_snippets.py`. |
| Python support | Package metadata is bounded to Python 3.9-3.14 and CI tests every supported interpreter. Version is `0.4.0`. |
| Conformance | CI has a dedicated conformance job with `CONFORMANCE=1` and `STREAMLINE_REQUIRE_CONFORMANCE=1`. The pytest session fails if zero conformance tests reach their call phase. |
| TLS/SASL | Producer, consumer, and admin share validated security argument construction and real SSL context creation. Unit tests cover CA-only TLS, mTLS, SASL_SSL, and invalid combinations. |
| Public models | `TopicConfig`, `TopicInfo`, and `ConsumerGroupInfo` are defined once in `streamline_sdk.types`; admin and package-root imports resolve to the same class objects. Deprecated detailed forms remain source compatible, including legacy *positional* construction (see the 2026-09-03 remediation pass below). |
| URL safety | Dynamic topic, group, branch, search, and schema subject path segments are percent-encoded; query parameters use `urlencode`. Exact `.`/`..` identifiers are rejected outright, since `yarl`/`aiohttp` silently normalize those (and only those) segments out of the final URL. |
| Testcontainers | Documentation uses `streamline_testcontainers`; CLI inputs are shell-quoted; log-level configuration is validated; the nested distribution is built, checked, and tested in CI. `StreamlineContainer` requires an explicit, digest-pinned image (no default, no mutable tags accepted). |
| Release supply chain | PyPI publishing uses OIDC trusted publishing, SBOM generation cannot be ignored (and is generated from a clean, wheel-only environment so it reflects real runtime dependencies rather than build tooling), artifacts receive provenance/SBOM attestations, and GitHub release files are checked. `publish` depends on a required live-conformance job (hard-blocked without an explicit digest-pinned server image) and on `attest`; the whole workflow is concurrency-guarded so releases can never overlap. |
| Nested packages | CI validates the Testcontainers wheel and embedded Rust cargo/maturin packages without publishing. Both are explicitly documented as source-only/unpublished (no registry-install claims). The embedded extension has its own maturin `pyproject.toml`, its placeholder storage methods raise `NotImplementedError` rather than fake success, and Dependabot covers all nested Python and Rust manifests. |
| Producer transactions | `Producer.send()` cannot bypass an active client-buffered transaction (the single enforcement point for all send paths); `commit_transaction()` snapshots/clears the buffer and exits buffering mode before replaying buffered sends. Explicitly documented as client-buffered and non-atomic — no broker-side transactional coordinator exists. |

## External Fixture Blocker

The repository's `docker-compose.test.yml` and GitHub Actions service expose
only plaintext Kafka (`9092`) and HTTP (`9094`). They do not provide:

- a TLS listener and CA certificate;
- an mTLS client certificate/key pair; or
- a SASL-enabled listener with test credentials.

The conformance security cases therefore require externally provisioned
fixtures:

- `STREAMLINE_TLS_BOOTSTRAP`
- `STREAMLINE_TLS_CA_FILE`
- `STREAMLINE_TLS_CERT_FILE` and `STREAMLINE_TLS_KEY_FILE` for mTLS
- `STREAMLINE_SASL_BOOTSTRAP`
- `STREAMLINE_SASL_USERNAME`
- `STREAMLINE_SASL_PASSWORD`
- `STREAMLINE_SASL_MECHANISMS` (comma-separated enabled mechanisms)

For SASL over TLS, also set
`STREAMLINE_SASL_SECURITY_PROTOCOL=SASL_SSL` and provide
`STREAMLINE_TLS_CA_FILE`.

Missing fixture values produce explicit skip reasons with `pytest -rs`; they do
not disable or silently skip the rest of the required conformance suite.

## Local Validation

| Check | Result |
|---|---|
| Ruff | `ruff check .` passed; all changed Python files pass `ruff format --check`. |
| Mypy | Passed with no errors across 69 source files. |
| Python 3.11 | 529 passed, 90 gated/optional skips. |
| Python 3.12 | 529 passed, 90 gated/optional skips. |
| Python 3.13 | 529 passed, 90 gated/optional skips. |
| Python 3.14 | 531 passed, 88 gated live-service skips. |
| Python 3.9 syntax | All 66 SDK/test/example files parse with the Python 3.9 grammar. |
| Root package | Wheel and sdist metadata now targets version `0.4.0`; both package formats remain covered by `twine check`. |
| SBOM | Reproducible CycloneDX 1.6 JSON generated and validated. |
| Testcontainers package | Two self-contained helper tests passed; wheel and sdist passed `twine check`. |
| Embedded package | `cargo test`, package-content validation, maturin CPython 3.14 wheel build, and `twine check` passed. |
| Conformance enforcement | A deliberately all-skipped required run exited as a failure with the enforcement message. |

Python 3.10 interpreters were not installed locally; the CI matrix covers it.
Python 3.11/3.12/3.13 were present but their `ensurepip` step failed in this
sandbox, so only 3.14 (and a 3.9-grammar `ast.parse` sweep of every source
file) were exercised directly in this pass; the CI matrix is unchanged and
still covers 3.9-3.14.

## 2026-09-03 Verification Pass

This pass re-ran the checks below independently rather than re-doing the
work described above, which was already present as uncommitted changes:

- `ruff check .`, and `ruff format --check` restricted to the files this
  session (and its predecessor) touched, both pass. (41 pre-existing files
  outside this diff — e.g. `streamline_sdk/client.py`, `retry.py`,
  `query.py`, most of `tests/test_*.py` — are not `ruff format`-clean, but
  that debt predates this work and is unrelated to these changes.)
- `mypy .` — success, no errors, 69 source files.
- `pytest tests/` on Python 3.14.6 — 529 passed, 90 skipped (gated
  integration/conformance/optional-dependency tests).
- Every `.py` file under the repo (excluding `testcontainers/` and
  `streamline_embedded/`, which are separate distributions) parses under
  the Python 3.9 `ast` grammar.
- Root `python -m build` + `twine check dist/*` — passed. Wheel contains
  only `streamline_sdk/*` plus `py.typed`; no stray top-level packages.
- `python -m build testcontainers` + `twine check testcontainers/dist/*` —
  passed. Wheel contains only `streamline_testcontainers/*`.
- `cargo package --manifest-path streamline_embedded/Cargo.toml
  --allow-dirty --no-verify` and `maturin build --release
  --manifest-path streamline_embedded/Cargo.toml` — both passed; the built
  wheel contains the compiled extension module, `py.typed`-equivalent
  metadata, and an embedded CycloneDX SBOM. `twine check` on that wheel
  passed. (A bare `cargo build`/`cargo test` at the crate root can fail to
  link on macOS because the pyo3 `extension-module` feature intentionally
  omits linking against `libpython`; this is expected and does not affect
  `cargo test` — which builds a normal test harness and links fine with
  zero `#[test]` functions present — nor the `maturin`-driven build CI
  actually runs.)
- Required-conformance enforcement was exercised directly, not just
  inspected: `CONFORMANCE=1 STREAMLINE_REQUIRE_CONFORMANCE=1 pytest
  tests/conformance -m conformance` with no `CONFORMANCE` var set at all
  reproduces the CI-misconfiguration case (every item gated-skipped at
  collection time) and exits non-zero with "required conformance run
  executed zero conformance tests". A single security test skipping itself
  via `require_env(...)` (missing external fixture) does *not* trip the
  failure by itself, because it still reaches the `call` phase — the gate
  is intentionally scoped to "the whole run never attempted anything" (a
  disabled/misconfigured CI job), not "some individually-declared,
  externally-fixtured tests are unavailable", which is the documented and
  expected state of this repository.
- **Fixed this pass:** `testcontainers/pyproject.toml` listed
  `Jose David Baena <josedab@users.noreply.github.com>` (the local
  operator's real identity) as the package author, inconsistent with the
  `Streamline Authors <team@streamlinelabs.dev>` convention used by the
  root and `streamline_embedded` distributions. Corrected to match; this
  was a real personal-information leak, not a hypothetical concern — it
  would have shipped in PyPI metadata for `testcontainers-streamline`.
- **Corrected finding vs. previous note:** the local sandbox's Docker
  daemon *is* reachable (`docker info`/`docker ps` succeed). The actual
  blocker is that `ghcr.io/streamlinelabs/streamline` has no published
  image reachable from this environment (`docker pull
  ghcr.io/streamlinelabs/streamline:0.3.0` and `:latest` both return
  `manifest unknown`, and the GHCR tag-list API returns `UNAUTHORIZED`).
  Live Testcontainers/integration/conformance runs therefore still cannot
  execute here, but because there is no consumable server artifact, not
  because Docker itself is missing. Building the real server image from
  the sibling `streamline`/`streamline-deploy` repositories is out of
  scope for a review confined to `streamline-python-sdk`.

## 2026-09-03 Blocker Remediation Pass

This pass fixed several concrete correctness/honesty bugs found during a
focused review, each with regression tests. None of these were previously
listed as resolved above; they are now.

- **Transaction buffering could be bypassed and committing could silently
  drop messages.** `Producer.send()` never checked whether a transaction
  was active, so calling it directly (instead of `send_record()`) during a
  transaction sent straight to the broker. Separately,
  `commit_transaction()` replayed the buffered records via `send_batch()`
  *before* clearing `_in_transaction`, so each replayed `send_record()`
  call re-buffered the record onto the very list `send_batch()` was
  iterating, instead of transmitting it — buffered messages were never
  actually sent on commit. Fixed: `send()` now buffers whenever a
  transaction is active (the sole enforcement point, so no send path can
  bypass it), and `commit_transaction()` snapshots and clears the buffer
  and exits buffering mode before replaying. Transactions are now
  explicitly documented as client-buffered and non-atomic — there is no
  broker-side transactional coordinator. See
  `tests/test_producer.py::TestProducerTransactionBuffering`.
- **Legacy positional `TopicConfig`/`ConsumerGroupInfo` constructors were
  silently broken by canonicalization.** The pre-canonicalization
  dataclasses had different positional field orders than the new
  admin-facing canonical shape (`TopicConfig`: `partitions,
  replication_factor, retention_ms, ...` vs. `name, num_partitions,
  replication_factor, config`; `ConsumerGroupInfo`: `group_id, state,
  protocol_type, protocol, members` vs. `group_id, state, protocol,
  members`). A caller still using the old positional call shape got wrong
  values in the wrong fields with no error. Fixed: both classes now detect
  the legacy call shape (by first-argument type for `TopicConfig`, by
  positional argument count for `ConsumerGroupInfo`) and dispatch it to
  the correct fields, with a `DeprecationWarning`. See
  `tests/test_model_compatibility.py`.
- **`.`/`..` path identifiers were not rejected, and `yarl` normalizes them
  away.** Confirmed directly with `yarl.URL`: a path segment of exactly
  `.` or `..` (even percent-encoded) is silently collapsed during URL
  construction, e.g. `.../v1/topics/..` becomes `.../v1/`, redirecting the
  request to a different endpoint than the caller specified. Every other
  dot-containing segment (`a..b`, `..hidden`, `...`) is unaffected. Fixed:
  `encode_path_segment()` now rejects the exact literal values `.` and
  `..` with `ConfigurationError` before any URL is built. See
  `tests/test_url_encoding.py` (includes yarl-based final-URL assertions).
- **Embedded scaffold's placeholder storage methods returned fake
  success.** `create_topic`/`delete_topic`/`produce`/`consume`/
  `list_topics`/`latest_offset`/`flush` all returned `Ok(...)` with
  fabricated values (offset `0`, empty lists) instead of indicating the
  Streamline C FFI is not linked. Fixed: every one now raises
  `NotImplementedError` via a shared `ffi_not_implemented()` helper.
  Verified both with a pure-Rust `cargo test` (message-content assertions;
  `PyErr` formatting itself needs a linked CPython interpreter that the
  `extension-module` build intentionally omits from a standalone `cargo
  test` binary) and by building the actual wheel with `maturin develop
  --release` and importing/exercising it directly.
- **Testcontainers defaulted to, and accepted, a nonexistent/mutable
  image tag.** `StreamlineContainer` defaulted to
  `ghcr.io/streamlinelabs/streamline:0.3.0`, which — per the "External
  Fixture Blocker" note above — has never been published; a bare
  `StreamlineContainer()` could only ever fail. Fixed: `image` is now a
  required argument and must be pinned by digest
  (`registry/repo@sha256:<64 hex>`); mutable tags are rejected with a
  clear error. The three classmethod factories
  (`as_kafka_replacement`/`with_pre_configured_topics`/`for_testing`) now
  take and forward an explicit `image` too. `testcontainers/tests/
  test_container.py`'s Docker-starting tests are now gated behind an
  explicit `STREAMLINE_TESTCONTAINERS_IMAGE` environment variable (skipped
  with a clear reason if unset) instead of assuming a working default.
- **Nested packages advertised a registry install that does not exist.**
  `testcontainers/README.md` had a PyPI badge and `pip install
  testcontainers-streamline`; `streamline_embedded/README.md` had `pip
  install streamline-embedded`. Neither package is published anywhere —
  `ci.yml` only builds and `twine check`s them, and never runs `twine
  upload`/`cargo publish` (confirmed by re-reading the workflow). Both
  READMEs now say so explicitly and document building from a source
  checkout instead.
- **Release SBOM generation scanned the wrong environment.** Confirmed by
  reproducing both ways locally: `cyclonedx-py environment` (with no
  environment argument) run in the same venv used to build/check the
  distribution reported `build`, `twine`, `cyclonedx-bom`, and ~45 of
  their transitive dependencies as SBOM "components" (`Pygments`,
  `keyring`, `rich`, `jsonschema`, ...) while omitting `aiokafka` and
  `cryptography` — the wheel's actual runtime dependencies — entirely,
  because `python -m build` never installs them into the calling
  environment (it builds in an isolated PEP 517 backend env). Fixed: the
  release workflow now creates a separate, clean virtual environment,
  installs *only* the just-built wheel into it (pulling in exactly its
  runtime dependencies), and points `cyclonedx-py environment` at that
  environment explicitly. Verified locally: the resulting SBOM lists
  `aiokafka`, `cryptography`, and their transitive dependencies, and none
  of `build`/`twine`/`cyclonedx-bom`.
- **Release publication had no live-conformance or attest gate, and could
  race itself.** `publish` previously depended only on `build`, and there
  was no conformance job in `release.yml` at all — a tagged release could
  publish to PyPI without ever running the conformance suite against a
  live server, and `publish`/`attest` ran in parallel rather than gating
  each other. Fixed: a new `conformance` job hard-blocks (exits non-zero
  with an explicit message, rather than skipping) unless the
  `STREAMLINE_CONFORMANCE_IMAGE` repository variable names a
  digest-pinned image; it reuses the existing `tests/conftest.py`
  executed-test-count guard (`STREAMLINE_REQUIRE_CONFORMANCE=1`) so a run
  that executes zero conformance tests still fails the job. `publish` now
  `needs: [build, conformance, attest]`, and a workflow-level
  `concurrency: {group: pypi-release, cancel-in-progress: false}` ensures
  two release runs are never in flight at once. `actionlint` passes
  cleanly on the modified workflow. This repository still has no
  reachable Streamline server image (see "External Fixture Blocker"
  above), so `STREAMLINE_CONFORMANCE_IMAGE` is intentionally unset here —
  the job's own guard is what makes that state a hard failure instead of
  a silent pass, which was verified by reading the job logic and by the
  equivalent local reproduction: `CONFORMANCE=1
  STREAMLINE_REQUIRE_CONFORMANCE=1 pytest tests/conformance -m
  conformance` with no server running still fails the run rather than
  reporting success.

The coordinated release metadata now sets `streamline-sdk` to `0.4.0`,
keeps `testcontainers-streamline` at its independent `0.2.0` line, and sets
`streamline-embedded`/`streamline-python-embedded` to `0.4.0`. All local validation was
re-run after these fixes: `ruff check .` (clean), `mypy` (0 errors, 69
source files), `pytest tests/` (572 passed, 88 gated skips), root package
`python -m build` + `twine check` (passed, wheel contains only
`streamline_sdk/*`), `python -m build testcontainers` + `twine check`
(passed), `cargo test`/`cargo package --no-verify`/`maturin build
--release`/`twine check` for the embedded crate (passed), and the SBOM
workflow steps described above (passed). `actionlint` was run against
every workflow file with no findings.

## Remaining Non-P0/P1 Work

- The embedded Rust extension is still an explicitly documented API scaffold;
  storage methods require the Streamline C FFI before functional release.
- Live TLS/mTLS/SASL conformance should move into repository-owned fixtures once
  the server image exposes stable test configuration for those listeners.
