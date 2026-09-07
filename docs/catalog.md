# Snapshot catalog

`SnapshotCatalog` adds a local, transactional index over immutable
`Manifest` snapshots. It does not replace manifests: the full strict manifest
wire document is copied into SQLite, so a catalog remains inspectable without
the original source tree.

```python
from corpusledger import SnapshotCatalog

with SnapshotCatalog("catalog.sqlite") as catalog:
    ref = catalog.register("reviews", manifest, tags=("raw",))
    latest = catalog.manifest("reviews")
    history = catalog.lineage(ref.corpus_hash)
```

Versions are append-only per name. A parent is identified by its corpus hash and
must already be registered. `lineage` follows parent hashes to the root and
detects cycles. `diff(name, before, after)` delegates to CorpusLedger's strict
manifest comparison, including field-level changes, schema drift, order changes,
and newly observed privacy findings.

The catalog stores JSON text and uses parameterized SQLite queries. It rejects
unknown schema versions and does not migrate an unrelated database. This is a
local provenance index, not a remote registry, distributed lock, signature
authority, or garbage collector. Detached Ed25519 signatures remain the source
of authenticity; catalog approval alone does not authenticate a manifest.
