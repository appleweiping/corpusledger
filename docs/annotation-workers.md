# Independent annotation workers

These examples belong to the unreleased `feat/whole-repository-alignment`
development branch. They are not features of a published package or of `main`.
Use this checkout, or install that explicit Git ref, before running the Python
coordinator checks below.

The implemented workflow is a real Python → Go → Java chain. Python creates a
validated document; a standalone Go HTTP process adds `token` annotations; a
standalone Java HTTP process reads those tokens and adds a `token_group` whose
members reference their IDs. Python validates each response and constructs the
successor without modifying the original document. These are small deterministic
processor examples, not a distributed scheduler, a production NLP model, or a
complete Java/Go SDK.

## Build and verify

Install the development checkout into your existing Python environment. Go
1.23+ and JDK 21+ must already be available. The verified local toolchains are Go
1.26.4 and Temurin 21.0.7. No system installation occurs. The Java worker uses
only `jackson-core`, not databind, annotations, polymorphic deserialization, or
runtime reflection over input objects.

```powershell
python interop/build_workers.py --output D:/Company/worker-build-001 --dependency-cache D:/Company/.tools/corpusledger-worker-deps --fetch
python interop/verify_workers.py --build-dir D:/Company/worker-build-001
python interop/verify_execution.py --build-dir D:/Company/worker-build-001
```

The output directory must not exist and must be outside the repository. Builds
never delete or reuse an earlier artifact directory. `--fetch` explicitly permits
downloading one small jar from its pinned Maven Central URL into the given cache;
omit it for a fully offline build. Redirects and environment proxies are disabled
for that download, and the exact SHA-256 must match
`interop/java/dependencies.json`. Go builds set `GOPROXY=off`, `GOSUMDB=off`, and
`GOTOOLCHAIN=local`; there are no Go module dependencies. The build also runs Go
unit tests and compiles Java with `-Xlint:all -Werror`.

The verification script starts both compiled processes on ephemeral loopback
ports with generated, unprinted Bearer tokens. It checks real request/response
interoperability, source immutability, text/offset/reference fidelity, empty
input, a 1,001-digit integer, positions beyond binary64 and uint64, malformed
JSON/schema/identity rejection, authentication, and partial-body timeouts. It
also runs the durable executor against a temporary SQLite v2 journal: after Go's
validated output is saved, the actual Java process is terminated. The event
remains at its original revision. Reopening the database refuses implicit replay;
restarting Java on its pinned endpoint and explicitly acknowledging the retry
finishes only the missing step, then atomically publishes the event revision and
the committed operation result. The selected document and untouched sibling are
checked by canonical bytes/digests, not Python's permissive `True == 1` equality.

`verify_execution.py` adds a separate Python event-service process. It creates and
queries a two-document event over authenticated HTTP, begins a server-registered
pipeline, terminates and restarts the service, resumes the saved operation, and
queries the final revision/history. After both workers stop, repeating the same
committed key must return the saved result without another worker invocation or
event revision. Invalid credentials, an unknown pipeline registration, and a
changed document selection are rejected without publishing another operation or
revision. Operation discovery and history use exclusive-cursor pagination.
Worker descriptors and endpoints reach the service through
bounded startup input; tokens exist only in process environments. The script's
private `--serve` mode is a test harness, not a deployment interface; see
[durable execution](annotation-execution.md) for the application API and limits.

Both scripts print machine-readable evidence, stop their child processes, and
remove only their own temporary test databases. These are checked-in contract
fixtures, not an accuracy or throughput benchmark. Passing them does not establish
compatibility with another platform's protocol or full distributed-service parity.

## Running a worker

Set `CORPUSLEDGER_WORKER_TOKEN` in the environment to a secret 32–256 character
ASCII token using only letters, digits, `.`, `_`, `~`, or `-`. Do not put a token
in the command line, a tracked configuration file, or a public example output.

```powershell
D:/Company/worker-build-001/token-worker.exe --host 127.0.0.1 --port 0
java --add-modules jdk.httpserver -cp "D:/Company/worker-build-001/reference-worker.jar;D:/Company/.tools/corpusledger-worker-deps/jackson-core-2.21.6.jar" ReferenceWorker --host 127.0.0.1 --port 0
```

On Unix use `:` as the Java classpath separator and `token-worker` without the
`.exe` suffix. `--token-env NAME` selects another environment variable; it does
not accept the secret value. The first stdout line is the actual bound HTTP
address, not a token or a document. Only the literal addresses `127.0.0.1` and
`::1` are accepted; `localhost`, other addresses, and public binds are rejected.

Both endpoints require `Authorization: Bearer <secret>`:

| Endpoint | Contract |
| --- | --- |
| `GET /v1/info` | Exact `ProcessorDescription.to_dict()` object |
| `POST /v1/process` | Exact `AnnotationRequest.to_dict()` object; returns `AnnotationResponse.to_dict()` |

Requests use `Content-Type: application/json` and a bounded `Content-Length`.
No redirects, outbound requests, environment proxies, filesystem document paths,
remote code/module selection, cross-origin browser requests, compressed bodies,
or chunked bodies are supported. Unknown routes and query strings are rejected.
Error bodies contain only a fixed rejection code; input and decoder exception
details are neither echoed nor logged.

## Annotation semantics

`demo.go.tokens`, version `1`, produces `token` with required, non-null fields
`text:string` and `position:integer`. It splits on an explicit Unicode
`White_Space` set in the source, not on a locale-sensitive regular expression.
Positions start at zero. IDs are `demo.go.token.N`. Conflicts with existing IDs
or an existing token layer are rejected, never overwritten.

`demo.java.group`, version `1`, requires that exact token schema and produces
`token_group` with `members:references → token` and `count:integer`. Token
positions must be distinct non-negative integers. The worker orders members by
the full integer position, verifies each token's text against its span, and
creates `demo.java.group.0` spanning the minimum start and maximum end. With no
tokens it creates an empty group anchored at `[0, 0)`. An existing group layer
or conflicting group ID is rejected.

All wire spans are half-open Unicode **codepoint** offsets. Neither process
normalizes text, changes CRLF, composes combining characters, nor treats an
emoji's UTF-16 pair as two codepoints. Java builds one codepoint-to-UTF-16
lookup table before calling `substring`, avoiding repeated full-prefix scans.

Both workers verify the original UTF-8 `text_sha256`, closed document/schema/
annotation fields, required/nullable features, duplicate IDs, and intra-document
references, including unrelated existing layers. JSON integers remain exact up
to the protocol's 4,300-digit limit; non-finite floating values, duplicate keys,
invalid UTF-8, lone surrogates, and excessive nesting are rejected. The Python
`input_digest` is an opaque request correlation value to these workers: it is
echoed, **not independently recomputed using a foreign JSON serializer**. The
Python coordinator verifies that digest against its reserved original document.

The response contains only new annotations, the original operation/step/digest
and processor descriptor, and non-negative `duration_ms`. A successful HTTP
response is not an event commit. Durable reservation, resume, and atomic event
CAS publication belong to the Python execution service; these stateless worker
examples do not provide exactly-once execution or persistent response caching.

## Identity, limits, and dependency boundary

`config_sha256` includes the running Go executable bytes and Go runtime/platform,
or the Java worker jar bytes, JDK runtime/vendor/VM and exact Jackson jar bytes,
plus the declared algorithm choices. Rebuilding code or changing the runtime
therefore changes the descriptor. Port, token, and document content are not part
of the processor configuration. The Java worker refuses a Jackson jar whose
hash differs from its compiled dependency pin. This is reproducibility metadata,
not an authenticated code-signing scheme or a defense against replacing a
running machine's software.

The maximum wire body/response is 16 MiB and nesting depth is 64. Existing
document bounds remain 10 million codepoints and one million annotations; a
worker can reject earlier when its output would exceed the response budget. Go
reserves 8 KiB for the response envelope before accumulating token annotations.
Both runtimes cap open connections at eight, bound header sizes, disable
keep-alive, and bound input/output time. Go uses 3-second header, 5-second read
and 10-second write deadlines. The Java example sets the default JDK HTTP server's
request/response limits and verifies slow-body closure in the integration check.
Timer granularity is runtime-specific; these are not hard real-time deadlines.
Wire/resource counts do not constitute a parser sandbox or a strict process-RSS
limit. Run only trusted processor implementations in a trusted local workspace.

Jackson 2.21 is an open LTS branch. The pinned 2.21.6 release includes streaming
parser limit fixes; see the official [release notes](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21.6),
[branch status](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21),
and [core security advisories](https://github.com/FasterXML/jackson-core/security/advisories).
This pin was checked on 2026-09-09, not certified free of all vulnerabilities.
The JDK-specific HTTP controls are documented in the official
[JDK HTTP server module](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/module-summary.html).
Dependency jars and generated binaries are external build artifacts and are not
committed to this repository.
