# Local, versioned binary attachments

These commands target the development branch, not a released/main feature set.
They use the standard-library SQLite attachment implementation directly, without
network requests or subprocess processors. See the [storage contract](annotation-attachments.md)
for content identity, quotas, migration and concurrent-write semantics, and the
[local service](annotation-attachment-service.md) for the separate HTTP protocol.

## Explicit initialization and schema upgrades

An event and database must already exist. The existing `annotations-store put`
command or Python API can create a v1 event. For example:

```python
from corpusledger import AnnotationDocument, AnnotationEvent, AnnotationStore

with AnnotationStore("events.db") as store:
    revision = store.put(AnnotationEvent("example", (AnnotationDocument("text", "Hello world."),)))
    print(revision.revision, revision.digest)
```

Upgrade deliberately in two commands:

```shell
corpusledger annotations-attachments enable-execution events.db
corpusledger annotations-attachments enable events.db
```

The first opts into schema v2, the second into attachment schema v3. They are
independently committed migrations, **not one combined transaction**. If the
second fails, the first may already have succeeded. Both are idempotent; neither
normal opening nor a read command upgrades the database. `enable` requires v2
or an already-v3 database. These commands do not create a missing database or
rewrite historical event/operation bodies and digests. Older binaries may reject
the explicitly newer schema; this is not a backwards-reader compatibility claim.

## Attach, read and remove one name

Obtain the expected revision **and revision digest** from the Python result or
`annotations-store history`. A document digest or event-content digest is not a
replacement for the revision-chain digest.

```shell
corpusledger annotations-attachments attach events.db example source-image image.bin --media-type application/octet-stream --expected-revision 1 --expected-digest <revision-digest> --command-id original-upload
corpusledger annotations-attachments list events.db example --revision 2 --expected-digest <new-revision-digest>
corpusledger annotations-attachments get events.db example source-image --revision 2 --expected-digest <new-revision-digest> --output restored.bin
corpusledger annotations-attachments detach events.db example source-image --expected-revision 2 --expected-digest <new-revision-digest> --command-id original-detach
```

Replace angle-bracket values with the actual digests; they are explanatory
placeholders, not literal shell tokens. Binary bytes, including empty files,
NUL and non-UTF-8 sequences, are stored unchanged. Logical names are not paths;
the supplied media type is only a declaration, not validation of file contents.
No attachment is executed, rendered or automatically decoded.

Attach requires the name to be absent. Detach appends a new event version without
that name; its bytes remain readable from the old pinned revision. Even when the
last name is removed, the event retains its v2 attachment-capable identity. This
is not physical deletion, garbage collection or secure erasure.

Mutation output is a small JSON receipt with `committed`, event ID, revision,
parent digest and result digest, not the complete event or binary. If an identical
command ID and request are retried, the original pinned result is returned even
after the head has advanced. Changing the request while reusing its command ID
fails. No CLI command retries automatically; a retry with a fresh ID is a distinct
operation and does not repair an uncertain earlier call.

## Full, bounded single-event snapshots

```shell
corpusledger annotations-attachments export events.db example --revision 2 --expected-digest <revision-digest> --output snapshot.json
corpusledger annotations-attachments import other.db snapshot.json --snapshot-digest <snapshot-sha256-field> --expected-revision 0 --command-id original-import
```

`other.db` must already have schema v3. A snapshot contains the complete selected
event, manifests and deduplicated canonical Base64 bytes. Its `sha256` field binds
the canonical envelope body; it is not the hash of the serialized file including
that field. Use a digest supplied by a trusted export/source when origin integrity
matters. Reading a digest from an untrusted file and trusting that same file proves
only self-consistency, not authenticity.

Import uses the snapshot's event ID; it does not silently rename it. Expected
revision zero creates only and requires omission of `--expected-digest`; an update
requires a positive expected revision and its exact digest. The target gets a new
local revision, while the original source revision/digest are provenance. This is
one-event transfer, **not restoration of the source revision history**. Missing,
extra, duplicated, noncanonical or corrupted blob entries fail before publication.

## Bounds and file/stream failure semantics

Each command exposes `AttachmentLimits` as kebab-case flags. Defaults are 4 MiB
per blob, 64 logical names, 8 MiB logical bytes per event, 64 MiB unique bytes per
store, 1,024 unique blobs and 100,000 receipts. These limits may be tightened
within their validated bounds. Physical unique-byte quotas and logical event-byte
quotas differ when multiple names/events reference the same content. They are
not process-RSS, SQLite WAL/disk-size or elapsed-time guarantees.

Snapshot input/output has a separate 12 MiB complete UTF-8 envelope cap, including
JSON and Base64. Large otherwise-valid events may be ineligible for a snapshot.
Input files are bounded while reading, before database mutation; binary input
uses its configured single-blob cap. These commands require regular local files
and protect known database/SQLite-sidecar/input aliases. The threat boundary is
a cooperating local filesystem, not hostile directory mutation during an operation.

Binary and snapshot exports require a **new** output path whose parent already
exists. Existing files, directories and dangling links are rejected. A complete
owned temporary file is flushed, then published exclusively; a competing writer
cannot be overwritten. Cleanup failure after publication reports success with a
warning that a private temporary copy remains. Inspect it manually; there is no
secure-erasure promise. List output may also be written to a new file or stdout.

After a mutation commits, failure to deliver its optional stdout receipt preserves
exit zero and emits a best-effort warning. Query stdout failure returns an error;
stdout may contain a prefix and is not atomic. Short writes are detected, not
assumed complete or automatically resent. A failed real descriptor is silenced
before Python shutdown to preserve the chosen status. Controlled validation or
publication failures reach the normal CLI error exit (2). Neither this behavior
nor a content hash guarantees that an external caller received a response.

Attachment-command validation diagnostics are static: they do not echo stored
event IDs, annotation type/feature names or malformed snapshot contents. A
conflict asks the caller to check the revision, digest, attachment name and
command ID; other lower-level data-validation failures are deliberately less
specific. Successful JSON receipts, manifests and exports remain **private data**
and contain identifiers. This boundary does not sanitize other CLI commands,
shell history, caller-provided arguments or unexpected programming tracebacks.
