# Architecture

CorpusLedger separates input identity, semantic content, physical layout, observed shape, and risk hints. Keeping these
signals separate makes a diff explainable.

## Pipeline

1. `readers` discovers supported files deterministically, streams strict UTF-8 JSONL, rejects duplicate object keys,
   and enforces one unique normalized scalar ID. Explicit `corpusledger.readers` entry points can add file formats;
   third-party code is never imported during ordinary discovery.
2. `canonical` recursively normalizes values under an explicit, serialized policy.
3. `fingerprint` hashes records, logical files, the ID-sorted corpus, and the observed ID order.
4. `schema` records only observed JSON-oriented types and presence. It performs no string-to-date coercion.
5. `privacy` reports sensitive field names and token-like, high-entropy strings using redacted evidence; its version and
   complete configuration are persisted.
6. `manifest` incrementally assembles record entries, file/order hashes, schema state, and findings before persisting the
   versioned artifact.
7. `diff` compares compatible artifacts at record, field, schema, order, and privacy-risk levels.
8. `reporting` exposes structured JSON or a value-free Markdown summary.
9. `signing` optionally creates and verifies detached Ed25519 envelopes over exact manifest bytes. It lazily imports the
   `cryptography` extra and does not place key material in either artifact.

## Determinism boundaries

Object ordering and insignificant JSON whitespace do not affect content hashes. NFC-equivalent Unicode text hashes
identically. Moving a file may alter file/source metadata but not the corpus hash. Reordering records changes the order
hash, not the corpus hash. Reordering a list is a content change under the default policy.

CorpusLedger rejects ambiguity rather than relying on platform behavior: duplicate JSON keys, non-string keys,
non-finite or overflowing binary64 floats, integers above the documented stable digit limit, unpaired Unicode surrogate
escapes, Python-only objects, malformed JSON, missing IDs, and Unicode-equivalent duplicate IDs are errors.
Field, schema, and privacy paths are RFC 6901 JSON Pointers rather than delimiter-joined strings.

## Memory and scaling

The default JSONL path has one-record raw working memory. A record is parsed and normalized, then its record and field
hashes, schema observations, and privacy findings are derived before its body is discarded. File and order hashes use
incremental canonical-array hashers. Corpus hashing happens from the much smaller record entries after input is
exhausted.

This is not a constant-memory manifest builder: the selected manifest format requires all record entries and field
hashes, global duplicate detection requires all normalized IDs, schema inference requires one accumulator per observed
path, and privacy findings are output data. The lower bound is
`O(manifest entries + unique IDs + schema paths + findings + largest record)`. Relative to v0.1, raw corpus bodies and
their second normalized copies are no longer retained.

Two compatibility boundaries remain:

- A `.json` array is materialized by the standard-library decoder. JSONL is the supported streaming representation.
- `list_strategy="sort"` historically sorts every canonical list, including fingerprint container sequences. Exact v0.1
  compatibility therefore requires retaining their encoded members for sorting. Default `list_strategy="preserve"`
  hashes and discards sequence members immediately.

The checked benchmark records complete pipeline throughput and `tracemalloc` peak allocations. See
[`benchmarks/README.md`](../benchmarks/README.md); benchmark results describe the named environment and are not a
universal performance guarantee.

## Compatibility

### Annotation execution is a separate exact-text boundary

Annotation documents/events do not use the manifest canonicalization policy:
their original text, codepoint offsets, closed schemas and typed feature values
are preserved. `annotation_protocol` defines bounded language-neutral JSON;
`annotation_remote` connects only to explicitly configured loopback workers;
`annotation_execution` plans and runs the DAG; `_annotation_journal` participates
in the same SQLite transaction as `annotation_store` when publishing results.
Worker calls are outside database writer transactions. The new event service
does not extend the generic path-accepting manifest `/v1/dispatch` interface.

Read [durable execution](annotation-execution.md) for migration, per-step CAS,
uncertainty and scope boundaries. The Go/Java code under `interop/` is runnable
processor examples, not a mature cross-language SDK or model suite. Raw protocol,
socket, journal and migration tests complement the existing document/event tests.

### Manifest compatibility

Manifests can be compared only when hash algorithm, canonicalization version/policy, and privacy scanner
version/configuration are identical. This prevents configuration changes from masquerading as corpus drift. The
top-level format string controls structural compatibility. An optional top-level `reader_metadata` extension is valid
within `corpusledger/1`; old manifests omit it and retain byte-stable load/save behavior. Manifests produced by an
adapter can only be compared or rebuilt with the same adapter name/version.

Reader discovery is an explicit trust boundary: selecting an installed entry point imports third-party Python in the
current process. Adapter metadata and suffixes are validated before use, and arbitrary plugin exception messages are
not copied into user-facing errors. This is opt-in execution, not sandboxing.

Detached signatures deliberately do not change the manifest format. A signature envelope binds exact bytes, names the
Ed25519 algorithm, records a full SHA-256 public-key fingerprint, and records the exact manifest SHA-256 digest. The
public key remains external so verification cannot accidentally trust a key supplied by the artifact being checked.
