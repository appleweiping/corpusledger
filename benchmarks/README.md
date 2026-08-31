# Snapshot benchmark

The benchmark covers the complete default JSONL path: deterministic generation, strict parsing, Unicode
canonicalization, record/field/file/corpus/order hashes, incremental schema and privacy aggregation, and manifest
serialization. It does not time installation or filesystem cache warm-up.

Run the checked 100,000-record case from the repository root:

```bash
python benchmarks/run_snapshot.py \
  --records 100000 --payload-bytes 256 --repeats 3 \
  --memory-records 10000 \
  --result benchmarks/results/windows-python314-100k.json
```

Throughput runs without allocation tracing. A separate 10,000-record pass uses `tracemalloc`, because tracing every
allocation strongly distorts a 100,000-record timing; that peak is Python allocations, not operating-system RSS. The
resulting manifest necessarily retains one
entry and its field hashes per record, so total memory remains `O(records + unique IDs + schema fields + findings)`.
The streaming guarantee is narrower and important: default JSONL snapshotting does not retain raw record bodies after
their hashes, field summaries, and findings are derived. One current record and the longest input line bound the raw
payload working set.

Two deliberate exceptions are measured separately if relevant:

- `.json` arrays are materialized by the standard-library JSON decoder; use JSONL for streaming.
- `CanonicalPolicy(list_strategy="sort")` preserves the v0.1 global-list hashing semantics and therefore retains
  canonical sequence members for sorting. The default `preserve` policy is the streaming path.

Checked result files are observations on named hardware and Python versions, not universal performance claims. Compare
results only when the generator parameters, CorpusLedger version, Python version, and storage conditions are relevant.

## Checked result (2026-08-31)

[`results/windows-python314-100k.json`](results/windows-python314-100k.json) records a complete run on the shared Windows
11 development workstation with Python 3.14.5 and CorpusLedger 0.2.0:

- 100,000 records / 37,842,381 input bytes / 63,790,514 manifest bytes;
- full-run times of 369.0899, 592.1088, and 375.4652 seconds;
- median 375.4652 seconds, or 266.34 records/second;
- separate 10,000-record allocation profile: 33.19 MiB peak Python allocations and 149.7214 seconds with tracing.

The machine was shared with concurrent build/test jobs, which is visible in the spread between repeats. These numbers
are intentionally retained instead of publishing only the fastest run. Treat the file as a reproducible baseline and
format/memory audit, not as a hardware capacity claim; rerun it on the intended deployment storage before sizing a job.
