# External JSONL sorting

`external_sort_jsonl` sorts records by their canonical JSON representation
without retaining the complete corpus in memory. It writes sorted temporary
chunks, merges them with a heap, fsyncs the output, and replaces the destination
only after a successful merge. Temporary chunks are removed on success or
failure.

```python
from corpusledger import external_sort_jsonl

report = external_sort_jsonl(
    "records.jsonl",
    "sorted.jsonl",
    chunk_size=50_000,
)
```

The report contains the record count, number of chunks, and SHA-256 digest of
the exact output bytes. JSONL is required so input memory remains bounded;
duplicate IDs and malformed records retain the normal CorpusLedger reader
errors.
