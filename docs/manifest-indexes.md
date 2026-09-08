# Manifest indexes

CorpusLedger manifests are intentionally portable JSON, but very large reviews
often need bounded queries without scanning every record in the JSON document.
`ManifestIndex` builds a local SQLite index over record identity, source,
position, content hash, and field paths. It stores no corpus text and is bound
to both the manifest's canonical digest and its corpus hash.

## Build and verify

```console
corpusledger index release.manifest.json release.index.db
corpusledger verify-index release.manifest.json release.index.db
```

Index construction writes a temporary database and replaces the destination
only after all rows and metadata have committed. The index format is explicitly
versioned (`corpusledger-index/1`). `verify-index` checks the exact canonical
manifest digest, corpus hash, record count, every record's source/position/hash,
and every indexed field path.

When the manifest's record metadata cannot fit in memory, callers that already
stream validated `RecordEntry` values can use `ManifestIndex.build_stream()`.
It accepts a one-pass iterable plus the authenticated manifest digest, corpus
hash, and expected record count; the SQLite writer retains no complete record
list and rejects identity/count mismatches before replacing the destination.
The resulting index has the same format and can be verified later against a
fully loaded `Manifest`.

## Query metadata

```console
corpusledger query-index release.index.db \
  --id-prefix customer- --field /text --limit 50 --output matches.json
```

Filters are optional and can be combined: `--id-prefix` uses a literal,
case-sensitive prefix; `--source` selects one manifest source file; and
`--field` selects records containing one JSON Pointer field path. Results are
ordered by record ID and bounded to 1–10,000 rows. Returned rows include only
review-safe metadata and field paths, never source values.

The Python API exposes the same contract:

```python
from corpusledger import Manifest, ManifestIndex

manifest = Manifest.load("release.manifest.json")
with ManifestIndex("release.index.db") as index:
    index.verify(manifest)
    for row in index.query(field_path="/label"):
        print(row.record_id, row.source, row.position)
```

For a database-backed or JSONL manifest pipeline, the bounded form is:

```python
with ManifestIndex.build_stream(
    stream_record_entries(),
    "release.index.db",
    manifest_digest=authenticated_manifest_digest,
    corpus_hash=authenticated_corpus_hash,
    record_count=authenticated_record_count,
) as index:
    print(index.stats())
```
