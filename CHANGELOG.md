# Changelog

All notable changes follow the principles of Keep a Changelog.

## [Unreleased]

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
