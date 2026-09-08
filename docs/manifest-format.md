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
| `reader_metadata` | Optional explicit third-party reader `name` and `version`; omitted for built-ins |
| `excluded_paths` | Optional sorted exact paths excluded from the snapshot; omitted when empty |

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
Unicode-normalized, and restricted to Unicode scalar values. JSON floats must fit finite binary64; integers use the
documented 4,300-digit resource limit. Hashes are lowercase hexadecimal. Implementations must not silently compare manifests with
different hash, privacy, or reader metadata. The content hashes are location-independent, while the top-level absolute
`source` means full manifest bytes are intentionally location-specific.

`reader_metadata` and `excluded_paths` are additive top-level extensions permitted by the original version-1
forward-compatibility rule. Consequently, existing version-1 manifests need no migration and load/save without
acquiring either field. A manifest that records a reader adapter must be rebuilt with the exact recorded name and
version. Nested reader metadata is strict and accepts only those two non-empty strings, without surrounding whitespace,
control/format characters, or surrogate code points. Exclusion entries are exact file paths, sorted and deduplicated;
relative entries are resolved against the verification source root, while external absolute entries remain tied to their
recorded location.

## Detached signature envelope

Signatures use a separate canonical JSON artifact with format `corpusledger-signature/1`:

| Field | Meaning |
|---|---|
| `format` | Signature envelope format |
| `algorithm` | Exactly `ed25519` |
| `key_id` | SHA-256 of the raw 32-byte Ed25519 public key |
| `manifest_sha256` | SHA-256 of the exact manifest file bytes |
| `signature` | Base64 encoding of the 64-byte Ed25519 signature over those exact bytes |

The envelope contains no public or private key. Verification requires a separately trusted Ed25519 PEM public key and
checks its fingerprint before checking the digest and signature. Unknown/missing fields, duplicate JSON members,
non-canonical digest shapes, malformed base64, wrong key identities, and changed manifest bytes fail closed.
