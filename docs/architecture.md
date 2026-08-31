# Architecture

CorpusLedger separates input identity, semantic content, physical layout, observed shape, and risk hints. Keeping these
signals separate makes a diff explainable.

## Pipeline

1. `readers` discovers supported files deterministically, parses strict UTF-8 JSON, rejects duplicate object keys, and
   enforces one unique normalized scalar ID.
2. `canonical` recursively normalizes values under an explicit, serialized policy.
3. `fingerprint` hashes records, logical files, the ID-sorted corpus, and the observed ID order.
4. `schema` records only observed JSON-oriented types and presence. It performs no string-to-date coercion.
5. `privacy` reports sensitive field names and token-like, high-entropy strings using redacted evidence; its version and
   complete configuration are persisted.
6. `manifest` assembles and persists the versioned artifact.
7. `diff` compares compatible artifacts at record, field, schema, order, and privacy-risk levels.
8. `reporting` exposes structured JSON or a value-free Markdown summary.

## Determinism boundaries

Object ordering and insignificant JSON whitespace do not affect content hashes. NFC-equivalent Unicode text hashes
identically. Moving a file may alter file/source metadata but not the corpus hash. Reordering records changes the order
hash, not the corpus hash. Reordering a list is a content change under the default policy.

CorpusLedger rejects ambiguity rather than relying on platform behavior: duplicate JSON keys, non-string keys,
non-finite floats, Python-only objects, malformed JSON, missing IDs, and Unicode-equivalent duplicate IDs are errors.
Field, schema, and privacy paths are RFC 6901 JSON Pointers rather than delimiter-joined strings.

## Memory and scaling

Version 0.1 builds an in-memory record list so it can perform global duplicate detection, infer schema, and produce one
deterministic artifact. This keeps implementation and failure semantics straightforward but is not intended for corpora
larger than available memory. Streaming with an external sort is a roadmap item.

## Compatibility

Manifests can be compared only when hash algorithm, canonicalization version/policy, and privacy scanner
version/configuration are identical. This prevents configuration changes from masquerading as corpus drift. The
top-level format string controls structural compatibility.
