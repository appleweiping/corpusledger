# Changelog

All notable changes are documented here. This project follows semantic versioning.

## 0.2.0 - 2026-08-31

- Stream default JSONL snapshots one raw record at a time while retaining only manifest-required indexes and
  aggregations; preserve exact version-1 hashes for both algorithms and list policies.
- Add incremental canonical sequence hashing and schema aggregation, with explicit memory lower bounds and JSON-array
  and sorted-list compatibility limits.
- Add an injectable typed reader protocol plus explicitly loaded `corpusledger.readers` entry points; persist strict
  adapter name/version metadata without changing old version-1 manifests.
- Add optional detached Ed25519 signing, encrypted PEM password support through an environment variable, strict
  signature envelopes, trusted-public-key verification, and tamper/key-mismatch CLI behavior.
- Add property-based canonicalization and manifest roundtrip tests, legacy-hash compatibility tests, streaming boundary
  tests, adapter contract tests, and signature tamper tests.
- Add a deterministic 100,000-record generator and checked end-to-end throughput/memory benchmark.
- Expand CI to Python 3.10–3.14 with packaged wheel smoke tests on Linux, Windows, and macOS.
- Reject output hard-link aliases, unpaired Unicode surrogates, overflowing binary64 values, and resource-exhausting
  integers; validate reader identity/suffix metadata and sanitize plugin discovery and Markdown report boundaries.

## 0.1.0 - 2026-08-31

- Add deterministic JSON/JSONL and directory snapshots.
- Add versioned record, file, corpus, and order fingerprints.
- Add conservative schema summaries and redacted privacy-risk hints.
- Add record, field, schema, privacy, and order-aware diffs.
- Add JSON/Markdown reports and snapshot/diff/verify CLI commands.
- Reject duplicate JSON keys, non-finite numbers, and Unicode-equivalent duplicate IDs.
- Use collision-free JSON Pointer field paths and verify every derived manifest section.
- Persist versioned privacy scanner configuration alongside findings.
