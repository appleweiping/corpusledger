# Persistent annotation event benchmark

This benchmark exercises the local SQLite event store on a bounded prefix of
real dialogue text. It tests storage and processing invariants, not linguistic
accuracy or equivalence to a distributed annotation service.

```shell
python benchmarks/benchmark_annotation_store.py /local/cornell_movie_dialogs_corpus.zip \
  --limit 200 --output benchmarks/results/annotation-store.json
```

The input is the locally supplied, SHA-256-pinned
[Cornell Movie-Dialogs Corpus archive](https://www.cs.cornell.edu/~cristian/data/cornell_movie_dialogs_corpus.zip).
Its required digest is
`3bde8a571f615201bc2d2453e22878090719638592f774720eddec739de8c900`.
The archive README contains no explicit redistribution license grant; the raw
archive remains outside this repository. Citation: Cristian Danescu-Niculescu-Mizil
and Lillian Lee, *Chameleons in Imagined Conversations*, CMCL/ACL 2011.

The script reuses the pinned reader in `benchmark_annotations.py`, which hashes
the complete archive/member/README and selected raw lines. It decodes
`movie_lines.txt` as Latin-1 and splits at most four field delimiters to preserve
the dialogue field exactly. The default selects the first 200 physical records;
`--limit` accepts 1–1,000. This prefix is not representative sampling or a
full-corpus throughput result. The script does not download or extract the
archive to a permanent directory.

## Workload and independent checks

Each source record creates an event with two documents: exact original text and
a `str.casefold()`-derived view. The latter is **not a translation or gold data**.
Event IDs are local sequence IDs, not the source corpus's identifiers. Initial
documents have no annotations. The first half of events, rounded down, receive
a second revision through `AnnotationStore.process` with an explicitly declared
local pipeline. Its single rule emits `[0, len(text))` with a typed integer
code-point-length feature in the original document. The sibling view is not
processed. These are engineering fixtures on real text, not gold annotations.

After closing and reopening the database, the benchmark checks:

- Event pagination with a page size of 37 returns every event exactly once in
  order; history pagination with a page size of one preserves revision sequence,
  parent links, and the digests returned by the original commits.
- Every first-revision document still equals its source-derived immutable
  snapshot. Current original text matches an independently computed UTF-8 hash;
  every related sibling document remains unchanged.
- Updated spans and their length fields equal independently constructed
  full-text intervals; persisted pipeline provenance has the expected version
  and input/output document digests.
- Whole-store verification matches independently expected event, revision, and
  unique-document counts. A separate read-only SQLite query compares the entire
  physical document table with the exact expected set of content digests, so
  extra orphan rows and failed deduplication are not hidden by public API counts.

Two failure oracles run before reopening. A stale compare-and-swap write must
fail. A two-processor pipeline produces a valid intermediate annotation, then
the second processor deliberately raises. Both callbacks must have run in
dependency order, but the event, all history, and sibling document must remain
unchanged. The callback counter is intentionally not rolled back: local store
atomicity does not promise rollback or exactly-once execution of external
callback effects. The physical document inventory check also ensures neither
failed operation left intermediate document rows.

The recorded 200-event run has 100 successful updates and 300 revisions. Across
600 historical document references, 494 distinct document payloads are stored:
106 references reuse content. All 200 source hashes and sibling documents and
100 persisted pipeline provenance records were checked. Raw dialogue, source
IDs, features, and document snapshots are absent from the published JSON.

## Timing, memory, and limitations

The JSON records separate times for source hashing/loading, initial event
creation, processing plus commit, failure oracles, and reopening plus independent
verification. Processing measures local Python and SQLite work, with no model
calls or remote servers. Each event is committed separately; this is not a
bulk-insert throughput benchmark. Verification time includes correctness oracles,
not just database reads. Runtime source files, both benchmark scripts, and the
archive are rehashed after the run; a concurrent change prevents publication.

Traced peak memory covers Python allocations during creation, updates, failure
oracles, and reopening/verification. Source records and pipeline/schema objects
were loaded before tracing and are excluded. SQLite native caches and process
RSS are also excluded; the reported peak is not total memory usage. Database
size is measured after connections close, and the temporary database directory
is then removed automatically. A fresh directory is created on each invocation;
the script never clears or reuses an existing user database.

Results include runtime, SQLite version, source hashes, environment, timings,
and counters. They establish these checks for the recorded local workload only.
They do not establish crash/power-loss recovery, cross-process scaling, model
quality, gold NLP accuracy, protection against an attacker rewriting history,
or feature/performance parity with another repository.
