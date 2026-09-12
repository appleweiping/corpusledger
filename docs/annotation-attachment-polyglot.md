# Binary attachments across Go, Python and Java

This is an original, bounded interoperability oracle for the development branch,
not a general Go/Java SDK or a claim of production deployment readiness. It uses
actual independently compiled native clients, a separate Python HTTP service
process, a durable SQLite store, and the existing Go/Java text workers. Inputs
are handcrafted synthetic fixtures, not evidence of learned NLP quality.

The [retained Windows run](../benchmarks/results/annotation-attachments.json)
records the actual source-bound workflow on Python 3.12.13, Go 1.26.4 and Java
21.0.7. Its runtime/helper hashes identify the tested implementation; they are
not a signature or a claim about untested later changes. This is a synthetic
contract test, not a throughput benchmark or evidence of broad NLP accuracy.

## Run without downloading dependencies

Use the checkout containing this example and its matching Python package. Go and
JDK 21 or newer must already be installed, and an existing `build.json` produced
by `interop/build_workers.py` must identify the Go token worker, Java reference
worker and the repository-pinned Jackson Core JAR. This example verifies their
checksums; it does not fetch a JAR, install a toolchain or trust a manifest as a
publisher signature. The selected build directory remains an operator trust
decision.

From the checkout, with its virtual environment already prepared:

```powershell
.venv/Scripts/python.exe examples/annotation_attachment_polyglot.py --build-dir D:/Company/build-verification-corpusledger-workers-20260909-v3
```

The orchestrator performs a fresh offline `go build -trimpath` with
`GOTOOLCHAIN=local`, `GOPROXY=off`, `GOSUMDB=off`, and `GOWORK=off`. It also runs
`javac --release 21 -Xlint:all -Werror` using only that verified local JAR. The
native client sources are `interop/go/attachment_example/main.go` and
`interop/java/AttachmentExample.java`. Each receives a bounded JSON configuration
on stdin and reads a randomly generated bearer token from an environment
variable. Tokens do not appear in command-line arguments or the report.

All generated binaries, raw payloads, databases and service-stop markers live in
a new `TemporaryDirectory`. Existing builds and user databases are not changed.
The result is one aggregate JSON object on stdout, with no automatic report-file
publication or overwrite. Redirect stdout only to a new path if preserving it.
Python optimization is explicitly rejected: this oracle must not run with its
assertions removed by `-O` or `PYTHONOPTIMIZE`.

## Independently checked workflow

1. Python creates an explicit v2 event at revision 1, containing a selected text
   document, an untouched sibling, and metadata with a boolean and a `2**200`
   integer. Text includes emoji, a combining accent, CRLF and literal template
   syntax. These are literal data, never executable instructions.
2. Go reads an exact 4,194,304-byte file containing every possible octet, including
   NUL and `0xff`. It independently computes SHA-256, sends canonical Base64 to
   `attachment_attach`, and checks the returned manifest and revision-2 parent
   binding. Its expected digest is a stored **revision** digest, not an event
   content digest.
3. The Python service exits and a new process reopens the same database. Java
   requests that exact event/revision/digest, checks the response pin, independently
   computes SHA-256 and size, decodes canonical Base64, and compares every byte
   against its separately read local reference file. Python also checks the typed
   read and list APIs.
4. A complete bounded snapshot is exported and imported into a second database
   served by a third Python process. Its externally supplied snapshot digest is
   required. The import retains event content but creates a new local revision 1;
   Java verifies that new pin and the import retry creates no extra revision.
5. The existing real Go token worker and Java token-reference worker process only
   the selected document. Four token spans are hand-counted in Unicode codepoints:
   `0..2`, `3..5`, `8..9`, `10..11`. A single `0..11` group references exactly those
   token IDs in position order. The event advances to revision 3; attachment
   manifests, raw bytes, sibling document and large-integer metadata are retained.
6. Detaching the logical name creates revision 4 without deleting the immutable
   blob or old revision. Java still reads revision 2 byte-for-byte. Repeating the
   original Go upload returns its original revision-2 receipt, and repeating the
   detach returns revision 4. The exact main history is `[1, 2, 3, 4]`.

The raw fixture's independent SHA-256 is
`2b07811057df887086f06a67edc6ebf911de8b6741156e7a2eb1416a4b8b1b2e`.
The report includes all runtime/example/helper source hashes, worker/JAR hashes,
the compiled Go binary hash, every compiled Java class hash, tool versions and
aggregate counts. Source and input-build hashes are checked again before success
is reported; the actually imported Python package must match that complete
runtime source inventory before and after execution. Ephemeral worker endpoints and execution durations enter runtime
provenance, so final revision digests need not match between runs.

## Three different size boundaries

The harness verifies the distinctions, not just a convenient small payload:

| Boundary | Actual check |
| --- | --- |
| 4 MiB raw blob | Exact maximum succeeds through Go; maximum plus one is sent through the raw RPC envelope and rejected with `413 too_large`, without a new revision. |
| 12 MiB complete inline snapshot | A valid event with 7 MiB metadata and an independently valid 4 MiB attachment can be stored, but their combined inline export exceeds 12 MiB and is rejected with `413 too_large`. History remains `[1, 2]`. |
| 16 MiB complete HTTP envelope | An authenticated request declaring one byte above the limit is rejected with `413` before any body is transmitted or revision changes. |

Base64 expands the binary data: a 4 MiB blob is not a 4 MiB HTTP message. The
default 8 MiB aggregate logical-attachment limit is a separate runtime policy;
this oracle does not claim to exhaustively test every quota or concurrency case.
The snapshot rejection uses individually valid stored components, not a forged
checksum or malformed snapshot passed off as a size-limit test.

## Tests and limits of the evidence

Fast tests do not require a compiler or network. The explicit native opt-in runs
the complete workflow, including compilation, sockets, shutdown and reopen:

```powershell
.venv/Scripts/pytest.exe --no-cov tests/test_annotation_attachment_polyglot.py
$env:CORPUSLEDGER_ATTACHMENT_WORKER_BUILD = 'D:/Company/build-verification-corpusledger-workers-20260909-v3'
.venv/Scripts/pytest.exe --no-cov tests/test_annotation_attachment_polyglot.py
```

Without the environment variable, the native case is reported as **skipped**,
not passed. Unit tests independently pin the payload digest, exact Unicode spans,
expected revision counts, source inventory, no-overwrite behavior, rejection of
unverified native output, and error redaction. A native test pass is contract
evidence for this declared environment and fixture only.

The clients accept literal `http://127.0.0.1:<port>` endpoints only, disable
proxies and redirects, require bearer authorization, bound input/response JSON,
reject duplicate keys and invalid Unicode, and keep operational errors redacted.
No arbitrary attachment is executed, rendered or interpreted as a program. This
does not establish sandbox security against executing malicious attachments,
multi-user authorization, general Internet deployment, streaming uploads, a full
polyglot SDK, or complete parity with a reference project's entire repository.

## Required CI execution

The `worker-interop` matrix invokes `scripts/worker_ci.py` on Linux, Windows, and
macOS. After building and checking the original worker and event-execution flows,
the runner invokes this attachment oracle directly with `--build-dir`; it does
not depend on the optional pytest environment variable and cannot silently skip
native execution. Go and JDK 21 are installed by the existing matrix setup. The
attachment examples build offline against the already verified worker dependency.

An attachment subprocess failure, missing/false required check, mismatched source
inventory, or source change during the run fails the job before a successful
combined report is written. The retained `evidence.json` includes the complete
`attachment_contract` report alongside the worker and event-service contracts,
including the attachment Python/Go/Java source hashes. Matrix configuration is
not evidence that a particular commit passed: inspect that commit's individual
job results and retained artifact.

`tests/test_worker_ci.py` tests these aggregation and failure rules using explicit
test doubles. Those portable tests are **not** evidence that Go or Java ran;
native evidence comes from the direct oracle invocation in the matrix (or a
separately recorded local invocation).
