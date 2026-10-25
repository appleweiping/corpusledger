# Changelog

## 0.3.0 (unreleased)

Reworked into a diff tool for dataset versions.

- `corpusledger diff OLD NEW` works directly on data files (JSONL, JSON, CSV, Parquet, or a
  directory of them) as well as on saved snapshots, and prints a summary: unchanged, changed,
  removed and added records, which fields changed, schema changes and a few examples.
  `--json` gives the full result for CI; the exit status is 1 when anything differs.
- `--key instruction,input` matches records by a hash of chosen fields for datasets without an
  ID column; records whose key fields were edited but whose other fields are identical are
  reported as edits, not as a removal plus an addition. `--ignore` leaves fields out.
- Snapshots are JSON with one record per line, so they can be committed and diffed in git.
- Removed the privacy scanner, the reader plugin system and Ed25519 signing. Version 0.2 is
  still available under the `v0.2.0` tag.

## 0.2.0 (2026-08-31)

Streaming JSONL snapshots, reader plugins and optional Ed25519 signatures.

## 0.1.0 (2026-08-31)

First version: content-hash manifests and record/field/schema diffs for JSON and JSONL.
