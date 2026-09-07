# Object store and snapshot bundles

`ObjectStore` provides immutable SHA-256-addressed bytes under a local
filesystem root. Writes are atomic and existing objects are authenticated before
deduplication is accepted. `bundle_snapshot` creates a deterministic ZIP with
the canonical manifest and every source file referenced by that manifest.

```python
from corpusledger import ObjectStore, bundle_snapshot

store = ObjectStore(".corpus-objects")
report = bundle_snapshot(manifest, "data/", "artifacts/snapshot.zip", store=store)
print(report.archive_digest, report.manifest_digest)
```

The bundle is a transport artifact, not a replacement for manifest verification:
consumers should load `manifest.json`, verify its digest/signature when available,
then rebuild against the extracted source files.
