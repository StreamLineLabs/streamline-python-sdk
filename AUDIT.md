# Clean Code and SRP Audit

## Summary

- **Highest-leverage split:** remove HTTP transport/session/error handling from
  the 500-line `Admin` class while keeping its public methods and Kafka admin
  lifecycle intact.
- `Admin` changes for Kafka topic/group APIs, REST transport, cluster
  observability, message inspection, metrics history, and Moonshot branches.
- `Consumer` mixes Kafka group consumption with a second semantic-search HTTP
  client and duplicates the dedicated `search.py` model/client.
- Duplicate topic/group dataclasses in `admin.py` and `types.py` have already
  diverged; consolidating exported types requires a compatibility decision and
  is deferred.
- Telemetry, serializers, retry, and circuit-breaker modules are long but each
  has one actor and should remain independent.

## Findings

| ID | Location | Category | Severity | Actors in conflict | Cost | Size | Behavior risk |
|---|---|---|---|---|---|---|---|
| PY-SRP-1 | `streamline_sdk/admin.py:229-753` | SRP, mixed class | P1 | Kafka administrators; HTTP transport; SRE/inspection; branch product | A transport/auth/error change and a topic/group behavior change edit one public lifecycle class. | L | Medium |
| PY-SRP-2 | `streamline_sdk/consumer.py:55-554` | SRP, mixed class | P2 | Kafka consumer groups; semantic-search HTTP API | Search dependencies/routes/error parsing live in the stateful Kafka consumer and duplicate `search.py`. | M | Medium |
| PY-CC-1 | `streamline_sdk/admin.py:24-226`; `streamline_sdk/types.py` | Duplication with drift | P2 | Admin public API; shared model consumers | Topic/group/query model copies differ in fields/defaults, so replacing one copy can break imports or state shape. | M | High |
| PY-CC-2 | `streamline_sdk/admin.py:748-753` | Dead comments | P2 | maintainers | Stale implementation notes imply features that are already implemented elsewhere and obscure the real end of the class. | S | None |

## Actor and State Partition

### `Admin`

| Partition | Methods/state | Actor/axis |
|---|---|---|
| Kafka lifecycle | `_admin`, `start`, `close`, topic/group Kafka calls | Kafka administrators |
| HTTP transport | `_http_get`, `_http_post`, `_http_delete`, aiohttp/urllib fallback | transport/platform |
| Cluster/lag/inspection | cluster, lag, message inspection, metrics mapping | SRE/tooling |
| Branches | create/list/discard branch | Moonshot product |

Resulting internal unit: `_AdminHttpTransport`, owning URL construction,
aiohttp/urllib fallback, timeout, status mapping, and JSON decoding. `Admin`
retains public methods, Kafka state, and response mapping decisions.

### `Consumer`

Kafka state (`_consumer`, subscription, offsets, polling, iteration) is
independent of semantic-search HTTP request construction. The search behavior
should use the existing `SearchClient`/`SearchHit` implementation where its
route and response contract are equivalent; otherwise the difference must be
reported rather than silently normalized.

## Ordered Refactor Sequence

1. Characterize Admin HTTP paths, methods, status errors, aiohttp and urllib
   fallback behavior.
2. Move HTTP transport unchanged into `_AdminHttpTransport`.
3. Remove stale end-of-file comments and keep response mapping in `Admin`.
4. Characterize Consumer semantic search against the dedicated search client.
5. Reuse one search response decoder/model only where tests prove behavior
   equivalence.
6. Run the Python 3.9/3.12 test, Ruff, mypy, and package-build matrix after
   every commit.

## Deferred

- Canonical `/v1` versus `/api/v1` routes require the org HTTP contract
  decision; this refactor preserves existing Python paths.
- Public model consolidation between `admin.py`, `types.py`, `query.py`, and
  `search.py` requires a deprecation/version plan.
- Live integration and conformance tests require the external server image.

## Out of Scope

- `telemetry.py`: one observability actor.
- `producer.py`: one producer/transaction lifecycle and shared producer state.
- `serializers.py`: schema serialization actor; format classes are intentionally
  separate.
- `retry.py` and `circuit_breaker.py`: separate reliability policies with
  distinct state machines.
