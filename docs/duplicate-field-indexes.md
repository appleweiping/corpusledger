# Duplicate field indexes

Manifest indexes retain field digests, not source text. The
`duplicate-fields` command groups records with equal authenticated field
digests, making exact duplicate and possible leakage reviewable without
exporting corpus contents.

```bash
corpusledger duplicate-fields records.index.db --field /text --limit 100
corpusledger query-index records.index.db --field-hash <sha256>
```

Groups and record queries are deterministic and bounded. Use `verify-index`
before consuming an index in CI so the digest binding to the manifest is
checked first.
