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

`verify_bundle` authenticates the archive bytes, rejects duplicate or traversal
members, and checks that the manifest inventory matches every `source/` member.
`extract_bundle` performs that verification before writing and refuses to
overwrite existing files unless `overwrite=True`.

```python
from corpusledger import extract_bundle, verify_bundle

verified = verify_bundle("artifacts/snapshot.zip", expected_archive_digest=report.archive_digest)
extract_bundle("artifacts/snapshot.zip", "artifacts/unpacked")
```
