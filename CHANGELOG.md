# Changelog

All notable changes follow the principles of Keep a Changelog.

## [Unreleased]

- Add immutable typed span documents, closed feature/reference validation,
  code-point/UTF-16 conversion and indexed interval queries. Add local annotation
  processor DAGs with preflight schema checks, bounded output collection and
  versioned digest provenance; expose document creation, validation, tokenization
  and queries through `annotations` CLI commands.
- Add deterministic exclusive ID cursors to manifest-index queries through
  `ManifestIndex.query(after_id=...)` and `query-index --after-id`.
- Expose authenticated manifest rebuild and named drift detection through the
  loopback service's `verify` operation.
- Expose resumable versioned record pipelines through the loopback service.
- Add standalone privacy scanning through `scan_corpus()`, the `privacy` CLI,
  and the loopback service, with one versioned report, configurable rule packs,
  complete record counts, and input-preserving report destinations.

- Add reproducible `snapshot --exclude` path configuration. Exact exclusions
  are persisted in manifests and reused by `verify`, while legacy manifests
  remain loadable without migration.

- Add `ManifestIndex.build_stream()` for atomically building digest-bound SQLite
  indexes from one-pass validated `RecordEntry` streams without materializing a
  complete `Manifest` in memory.

- Add deterministic `default`, `credentials`, and `pii` privacy rule packs to
  the snapshot CLI, local service, and Python API.

- Add conservative backward/forward/full JSON Schema compatibility reports via
  `schema-compat`, the Python API, and the local service.
- Add bounded-memory canonical JSONL external sorting with chunk merge, atomic output, and digest reports.

- Add authenticated per-field list-order selectors through `CanonicalPolicy.sort_paths` and `snapshot --sort-path`.

- Add bounded-memory `schema-validate` corpus checks for a documented,
  dependency-free JSON Schema subset.

- Add authenticated-manifest `schema` CLI and service export to draft-2020-12
  JSON Schema with conservative required/type/array metadata.

- Expose snapshot catalog registration, lineage, listing, and diffs through a
  scriptable `catalog` CLI.
- Add authenticated duplicate-field groups and field-digest filtering to manifest indexes.
- Add a loopback-first HTTP/JSON service boundary for manifest, diff, and bundle verification operations.
- Add a language-neutral NDJSON processor gateway with deterministic stream
  digests, bounded lines, and one structured response per input record.
- Expose the gateway as a `corpusledger stream` CLI with safe identity/select
  processors, digest reports, and strict failure status handling.
- Add a resumable `corpusledger pipeline` CLI for deterministic select, rename,
  and drop transformations with checkpoint provenance.
- Add strict versioned JSON pipeline plans that compile to the same built-in
  transformations and are accepted by `corpusledger pipeline --plan`.
- Add digest-bound SQLite manifest indexes with atomic builds, record/source/field
  filters, full verification, and `index`, `verify-index`, and `query-index` CLI commands.

### Added

- Atomic streaming transformation pipelines with ID preservation, source/output digests,
  failure-safe output replacement, and resumable provenance checkpoints.
- Transactional snapshot catalog with named versions, parent lineage, and manifest diffs.
- SHA-256 object storage and deterministic ZIP snapshot bundles.
- Authenticated bundle verification and path-safe extraction, exposed through dedicated CLI commands.
- Explicit object-store garbage-collection plans with opt-in deletion and CLI safeguards.

## [0.2.0] - 2026-08-31

### Added

- Incremental canonical sequence hashing and schema aggregation, with explicit memory lower bounds and JSON-array and
  sorted-list compatibility limits.
- An injectable typed reader protocol plus explicitly loaded `corpusledger.readers` entry points; strict adapter
  name/version metadata is persisted without changing old version-1 manifests.
- Optional detached Ed25519 signing, encrypted PEM password support through an environment variable, strict signature
  envelopes, trusted-public-key verification, and tamper/key-mismatch CLI behavior.
- Property-based canonicalization and manifest roundtrip tests, legacy-hash compatibility tests, streaming boundary
  tests, adapter contract tests, and signature tamper tests.
- A deterministic 100,000-record generator and a checked end-to-end throughput/memory benchmark.

### Changed

- Default JSONL snapshots stream one raw record at a time, retaining only manifest-required indexes and aggregations.
  Exact version-1 hashes are preserved for both algorithms and list policies.
- CI covers Python 3.10-3.14 with packaged wheel smoke tests on Linux, Windows, and macOS.

### Fixed

- Reject output hard-link aliases, unpaired Unicode surrogates, overflowing binary64 values, and resource-exhausting
  integers.
- Validate reader identity and suffix metadata, and sanitize plugin discovery and Markdown report boundaries.

## [0.1.0] - 2026-08-31

### Added

- Deterministic JSON/JSONL and directory snapshots.
- Versioned record, file, corpus, and order fingerprints.
- Conservative schema summaries and redacted privacy-risk hints.
- Record, field, schema, privacy, and order-aware diffs.
- JSON/Markdown reports and snapshot/diff/verify CLI commands.
- Collision-free JSON Pointer field paths, with every derived manifest section verified.
- Versioned privacy scanner configuration persisted alongside findings.

### Fixed

- Reject duplicate JSON keys, non-finite numbers, and Unicode-equivalent duplicate IDs.
