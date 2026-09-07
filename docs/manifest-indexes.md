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

