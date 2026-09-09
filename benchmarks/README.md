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

`benchmark_fixture.py` additionally exercises the checked-in human-authored
JSONL example through manifest creation, deterministic bundling, and archive
verification. It records the input digest and environment as `checked-in-example`.
This small authored fixture is distinct from both external datasets and generated
100,000-record scale data; older `fixture-real` labels should not be interpreted
as external real-data validation.

## Cornell Movie-Dialogs privacy scan

`benchmark_cornell_privacy.py` accepts the original archive from the
[official Cornell corpus page](https://www.cs.cornell.edu/~cristian/Cornell_Movie-Dialogs_Corpus.html).
It verifies a pinned archive SHA-256, converts all 304,713 utterances to temporary
UTF-8 JSONL, and compares the privacy scanner's record count with the independently
counted archive conversion. Its report records source/converted-data digests,
scanner configuration, runtime source digests, environment, elapsed scan time and
peak Python allocations. Conversion and digesting are outside the timed region.
Timing includes `tracemalloc` overhead and concurrent workstation activity; memory
does not include operating-system RSS. Use `--limit N` for an explicitly reported,
deterministic prefix instead of the full dataset.

```bash
python benchmarks/benchmark_cornell_privacy.py /data/cornell_movie_dialogs_corpus.zip \
  --output benchmarks/results/cornell-privacy.json
```

This is real-input parsing/scanning engineering evidence, not a labelled privacy
detection accuracy study. The source is *Chameleons in Imagined Conversations*
(Cristian Danescu-Niculescu-Mizil and Lillian Lee, CMCL/ACL 2011). The archive README
provides citation and provenance but no explicit license grant; obtain the original
archive from its publisher. The repository contains code and aggregate results only,
not redistributed dialogue. The local conversion uses Latin-1 decoding and retains
utterance ID, speaker ID, movie ID and text, omitting the character-name column.
