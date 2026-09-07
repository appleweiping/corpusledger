# Snapshot catalog CLI

The `catalog` command exposes the SQLite snapshot catalog as a scriptable
lineage workflow:

```bash
corpusledger catalog register catalog.db dataset manifest.json --tag raw
corpusledger catalog register catalog.db dataset next.json --parent <corpus-hash>
corpusledger catalog list catalog.db --name dataset
corpusledger catalog lineage catalog.db <corpus-hash>
corpusledger catalog diff catalog.db dataset 1 2 --format json
```

Registration stores the complete manifest and immutable parent relationship.
Listing, lineage, and diff output is deterministic and does not reread source
corpus files. The Python API remains `SnapshotCatalog` for applications that
need transactional registration or custom reporting.
