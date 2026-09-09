# Real-text cross-language annotation service benchmark

This engineering benchmark belongs to the unreleased
`feat/whole-repository-alignment` branch. It is not a published-package or `main`
capability. It exercises an independent Python client, a separate Python event
service, a compiled Go tokenizer and a compiled Java reference-group processor.
There are no downloaded models or paid API calls.

## Reproduce offline

Use the explicit development checkout and already installed Python, Go and JDK
21+. Build the workers as described in [annotation workers](annotation-workers.md),
then provide the locally held, checksum-pinned Cornell archive:

```powershell
python benchmarks/benchmark_annotation_service.py D:/Company/nlp-original-projects/alignment/datasets/cornell-movie-dialogs/cornell_movie_dialogs_corpus.zip --build-dir D:/Company/build-verification-corpusledger-workers-20260909-v3 --events 50 --output D:/Company/annotation-service-new-run.json
```

`--events` defaults to 50 and accepts only 1–200. The output parent must exist and
the output itself must be new: the script refuses existing files, symlinks and
hardlink aliases. It writes a complete temporary report and atomically links it
to the requested name without replacement. Choose a new report name for each
run. No existing output, database or build directory is deleted or reused.
Only the benchmark's own fresh temporary SQLite directory is removed after its
child service has exited. The script never extracts the corpus permanently.
If report publication succeeds but console delivery or temporary-report cleanup
fails, the script exits with code 2 and reports
`annotation_service_benchmark_report_published_but_delivery_failed`; the published
artifact remains available. Failures before publication report
`annotation_service_benchmark_failed_no_report_published`. Both messages omit
exception details and corpus contents.

The checked-in aggregate report is
[`benchmarks/results/annotation-service.json`](../benchmarks/results/annotation-service.json).
Its benchmark/helper/runtime hashes identify the exact evaluated sources, even
when a later repository commit changes the surrounding documentation.

The recorded Windows/Python 3.12.13 trial processed 50 events from 100 records,
checking 476 tokens, 50 groups and 50 unchanged siblings. It retained exactly
100 event revisions, 300 operation transitions and 200 physical document rows
before temporary cleanup. All 14 aggregate invariant checks passed. The client
Python traced peak was 2,055,477 bytes; the closed SQLite database was 1,753,088
bytes. Creation/execution/oracles took 35.61 seconds and service restart/requery/
idempotence checks took 25.75 seconds. These figures include verification work,
not just processor computation; see the measurement caveats below.

## Dataset and scope

The input is the official
[Cornell Movie-Dialogs Corpus archive](https://www.cs.cornell.edu/~cristian/data/cornell_movie_dialogs_corpus.zip),
SHA-256 `3bde8a571f615201bc2d2453e22878090719638592f774720eddec739de8c900`.
The shared `benchmark_annotations.real_texts` loader verifies the complete archive,
member and README hashes and the selected raw lines. It decodes `movie_lines.txt`
as Latin-1 without normalizing text, splits at most four corpus delimiters and
selects the first `2 * events` records in archive order. No random sampling occurs.

Consecutive records become two documents in each event: the first is selected for
processing; the next remains an untouched sibling. This pairing is a fixture
construction, **not** a claim that the records are semantically related. All text
comes from the pinned corpus; token/group annotations are deterministically
generated engineering output, not human gold labels.

The archive README contains no explicit redistribution license grant. Raw text,
record IDs, token strings, feature values, transcripts and temporary databases are
not published. Only counts, digests, timings, source provenance and implementation
metadata are retained. Dataset citation: Cristian Danescu-Niculescu-Mizil and
Lillian Lee (2011), *Chameleons in Imagined Conversations*, CMCL/ACL.

## What is independently checked

The expected annotation documents are constructed as plain JSON independently of
the processors and the Python annotation constructors. The token oracle uses
`unicodedata.category` separator categories `Zs`, `Zl` and `Zp`, plus HT, LF, VT,
FF, CR and NEL controls. It groups maximal nonseparator codepoints; it does not
copy the worker's explicit Unicode range predicate. This corresponds to the
Unicode 15.0 `White_Space` property, checked against the official
[UCD property list](https://raw.githubusercontent.com/unicode-org/unicodetools/main/unicodetools/data/ucd/15.0.0/PropList.txt).
Python documents the database version and category API in
[`unicodedata`](https://docs.python.org/3.12/library/unicodedata.html).

The benchmark enumerates the Unicode inventory and requires exactly 25 whitespace
codepoints, records its digest and the runtime UCD version, and checks a separate
emoji/combining/CRLF/NBSP boundary example. It explicitly excludes U+001C–U+001F,
U+200B and U+FEFF; Python `str.isspace()` alone would include four extra controls.
That synthetic oracle check is not counted as an additional real corpus record.

For every real-text event, the run verifies:

- Exact token text, ordinal positions and half-open codepoint spans against the
  independent maximal-run oracle, plus the complete declared token schema.
- Group member references in token order, member count and enclosing span,
  including the zero-token `[0,0)` anchor rule when applicable.
- Exact source text/hash and untouched sibling, preserving JSON scalar types
  through canonical byte comparisons rather than permissive Python equality.
- The initial source revision, selected document, complete ordered Go→Java
  pipeline, processor schemas/configuration hashes and endpoint hashes in the
  operation request; independent request and document digest calculations.
- Both intermediate document hashes, execution attempts, step order, duration
  integer type, complete provenance binding, and final event revision hash.
- The committed operation's result points to that exact revision; history
  contains only revisions 1 and 2 and six expected execution transitions.

The service process is then stopped and restarted on the same temporary SQLite
database. Every event and operation is queried again. Both workers stop before
repeating each operation's `begin` and `resume`; these calls must retrieve the
identical committed state without another attempt or event revision. The final
read-only SQLite check independently compares the physical document inventory
against all expected input/intermediate/output hashes, verifies revision and
operation-row counts, rejects orphan documents and runs `PRAGMA integrity_check`.
Credential markers must not occur in operation journal bodies.

This is a successful-execution/reopen/idempotence benchmark. Actual worker loss,
uncertain execution and explicit retry are separately exercised by
`interop/verify_workers.py`; the benchmark does not mislabel its intentional
post-completion shutdown as an in-flight failure-recovery experiment.

## Interpreting the numbers

This is one sequential local trial, with cold process startup. Timed stages include
source hashing/loading, process startup, creation and execution with independent
oracles, service restart/requery/idempotence, and physical SQLite verification.
They are not isolated tokenizer throughput, latency percentiles, load-test or
production capacity measurements. Python tracing and verification add overhead;
other local activity can affect wall time.

`peak_client_python_bytes` is the Python client's `tracemalloc` peak from child
startup through the final SQLite checks. It excludes corpus records loaded before
tracing, **all Python-service/Go/Java child memory**, SQLite native caches, other
native allocations and process RSS. It is not total system memory usage.

The report pins every current CorpusLedger Python runtime source, the benchmark,
source loader, interoperability harnesses, worker/build sources, build manifest,
compiled Go executable, Java worker jar, Jackson jar and Python/Java/Go launcher
executables before and after the run. A changed source or artifact prevents report
publication. Tool versions and each live worker configuration hash are included.
These are reproducibility identifiers, not code-signing attestations or hashes of
every operating-system/shared-library dependency.

The report publishes `expected_final_document_inventory_sha256` from the manual
plain-JSON oracle and `observed_final_document_inventory_sha256` independently
from returned final documents. They must match before publication. Each event's
zero-based ordinal and ordered document digests (selected, then untouched sibling)
form one compact, sorted-key UTF-8 JSON object with keys `ordinal` and `documents`,
followed by a literal LF; SHA-256 covers these frames in archive-pair order. Each
document digest covers its complete canonical JSON, including text, schema and
annotations. The inventory detects missing/swapped events or siblings without
publishing individual documents, identifiers or digests.
The recorded expected and observed inventories both equal
`1d52de308e5c37a66cef8696277b199a7fdd64d5c59eb586ef1b6777b50c841e`.

Those two inventory hashes are deterministic for the same records, order and
rules, excluding timing and endpoint information. Full pipeline bindings include
ephemeral loopback endpoint hashes, and provenance includes measured worker
durations; **revision and operation request digests may therefore differ across
legitimate runs**. Compare the deterministic document inventory and invariant
checks, not an expected universal revision digest.

Passing this run demonstrates bounded real-text cross-language transport,
selection, persistence, provenance and idempotence correctness. It establishes
neither linguistic accuracy nor distributed production reliability, protocol
compatibility with another platform, or whole-repository parity.
