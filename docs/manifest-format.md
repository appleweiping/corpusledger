# Manifest format `corpusledger/1`

The manifest is canonical, newline-terminated UTF-8 JSON. Consumers may ignore unknown top-level keys inside a
compatible format, but must reject an unknown top-level `format` value. Version 1 nested objects are strict: unknown or
missing record-entry, hash-metadata, and privacy-metadata keys are invalid.

## Top-level fields

| Field | Meaning |
|---|---|
| `format` | Structural version, currently `corpusledger/1` |
| `source` | Absolute source path used for snapshot/verification |
| `id_field` | Record identity field |
| `hash_metadata` | Algorithm, canonical version, and full normalization policy |
| `privacy_metadata` | Privacy scanner version and complete scanner configuration |
| `corpus_hash` | Hash of ID-sorted record hashes; independent of physical order |
| `order_hash` | Hash of record IDs in observed order |
| `files` | Relative logical source path to hash |
| `records` | Ordered record entries |
| `schema` | Observed record count and field summaries |
| `privacy_findings` | Sorted, redacted heuristic findings |

Each record entry has `record_id`, content `hash`, relative `source`, one-based `position`, and `field_hashes`. A field
hash reveals equality/change but not the original value. Object leaves use RFC 6901 JSON Pointer paths; arrays are a
single leaf so their sequence semantics remain intact. Pointer escaping distinguishes nested keys from literal keys
containing `/`, `~`, or `.`.

## Schema summary

Every JSON Pointer path records `present`, `types`, `nullable`, `optional`, `item_types`, and `object_keys`. Arrays
additionally record observed `min_items` and `max_items`. A change in record count can change `optional` even when
individual record content at that path is unchanged; this is intentionally reported as schema drift.

## Reproducibility

To reproduce a hash, use the algorithm and exact policy in `hash_metadata`. Canonical JSON is UTF-8, compact, key-sorted,
and Unicode-normalized. Hashes are lowercase hexadecimal. Implementations must not silently compare manifests with
different hash or privacy metadata. The content hashes are location-independent, while the top-level absolute `source`
means full manifest bytes are intentionally location-specific.
