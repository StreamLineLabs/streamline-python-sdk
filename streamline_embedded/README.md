# Streamline Embedded (Python)

> **Experimental scaffold, source-only, unpublished:** this crate validates
> the planned Python extension surface, but its storage operations are
> placeholders until the Streamline C FFI is linked — every storage method
> (`create_topic`, `delete_topic`, `produce`, `consume`, `list_topics`,
> `latest_offset`, `flush`) raises `NotImplementedError` rather than
> pretending to succeed. Do not use it as an in-process broker yet. It is
> not published to any registry (crates.io publication is disabled via
> `publish = false`, and there is no PyPI upload step in CI); build it from
> source.

## Install

There is no `streamline-embedded` package on PyPI or `streamline-python-embedded`
crate on crates.io. Build the extension from a source checkout instead:

```bash
pip install maturin
cd streamline_embedded
maturin develop --release
```

## Planned Usage

Once the Streamline C FFI is linked, the API is intended to look like this.
**Today, every storage call below raises `NotImplementedError`** — the
example illustrates the target shape, not current behavior:

```python
from streamline_embedded import EmbeddedStreamline

# Context manager for automatic cleanup
with EmbeddedStreamline.in_memory() as sl:
    sl.create_topic("events", partitions=3)  # raises NotImplementedError today

    offset = sl.produce("events", partition=0, key=b"user-1", value=b'{"action":"click"}')

    records = sl.consume("events", partition=0, offset=0, max_records=10)
    for r in records:
        print(f"offset={r.offset} value={r.value}")
```

## vs Testcontainers

| | Embedded scaffold | Testcontainers |
|---|---|---|
| Executes Streamline operations | No (raises `NotImplementedError`) | Yes |
| Docker required | No | Yes |
| Suitable for integration tests | No | Yes |
| Published to a registry | No (source-only) | No (source-only) |

## Building from Source

Requires Rust 1.80+ and maturin:

```bash
pip install maturin
cd streamline_embedded
maturin develop --release
```

Release validation uses `cargo test`, `cargo package --no-verify` (package
contents), and `maturin build --release` (authoritative extension linking);
none of these commands publishes the crate or wheel. Cargo publication is
disabled while the implementation remains a scaffold.
