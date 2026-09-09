# Versioned local annotation events

An annotation event groups related documents under one stable event ID. Each
document has its own ID, exact Unicode text, closed annotation schemas, and typed
spans. For example, one event can hold an original note and its translation.
Document IDs must be unique within that event; annotation references remain
inside their document, not across documents or events.

`AnnotationStore` persists complete immutable event snapshots in a local SQLite
database. Updating an event appends a revision; it does not alter historical
snapshots. Each revision records a digest of its descriptor (event ID, revision,
predecessor digest, metadata, provenance, and document content digests). This
history digest is distinct from `AnnotationEvent.digest`, which describes only
the event snapshot and has no revision or provenance fields.
The event model freezes metadata and documents before persistence. This is a
local persistence layer, not a remote annotation server, distributed event
service, work queue, or document lease protocol.

## Create an event and append a revision

Use the typed Python API to construct an event from validated documents:

```python
import json
from pathlib import Path

from corpusledger import AnnotationDocument
from corpusledger.annotation_store import AnnotationEvent

event = AnnotationEvent(
    "visit:1",
    (
        AnnotationDocument("original", "Hello 😀!\r\n"),
        AnnotationDocument("translation", "Bonjour !\r\n"),
    ),
    {"stage": "source"},
)
Path("event.json").write_text(json.dumps(event.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
```

The event JSON format is `corpusledger.annotation-event.v1`, with exactly
`format`, `id`, `documents`, and `metadata` fields. Every entry in `documents` is
a complete `corpusledger.annotations.v1` document, not a file path or executable
processor configuration. The CLI never imports code named by event data.

```shell
corpusledger annotations-store put events.db event.json
corpusledger annotations-store get events.db visit:1 --output editable-event.json
# Edit editable-event.json, retaining its event ID and complete document schemas.
corpusledger annotations-store put events.db editable-event.json --expected-revision 1
```

`put` defaults to `--expected-revision 0`, which means **create only**. For an
existing event, supply the exact current revision number. A stale number fails
without appending a revision. The store checks the expected revision and commits
the complete snapshot in one SQLite write transaction. Concurrent writers cannot
both append against the same predecessor. There is no implicit merge of spans,
metadata, or documents; a submitted event is the entire successor snapshot.

Python callers can also use `store.process(event_id, pipelines,
expected_revision=...)`, where `pipelines` maps document IDs to explicitly
constructed [local annotation pipelines](annotation-pipelines.md). All selected
documents and pipeline dependencies are checked before callbacks run. Successful
outputs are published together in one compare-and-swap revision; per-document
pipeline reports are retained as revision provenance. A callback failure or a
concurrent revision prevents publication, but cannot undo callbacks' external
side effects. The CLI does not deserialize pipelines or execute code from JSON.

The successful `put` response includes the committed revision and digest. Keep
that revision number to construct the next explicit update. Do not implement an
unconditional retry that substitutes the latest revision: inspect the competing
change and decide how to reconcile it first.

## Export, inspect, and verify

```shell
corpusledger annotations-store get events.db visit:1 --revision 1 --output original-event.json
corpusledger annotations-store get events.db visit:1 --document original --output original-document.json
corpusledger annotations validate original-document.json
corpusledger annotations-store list events.db --limit 100
corpusledger annotations-store history events.db visit:1 --limit 100
corpusledger annotations-store verify events.db
corpusledger annotations-store verify events.db --event-id visit:1
```

`get` exports the raw event format accepted by `put`; `--document` instead exports
the raw annotation document accepted by `annotations validate/query/tokenize`.
Omit `--revision` to read the current head. Exporting does not create a revision.

`list` returns event heads ordered by event ID. Pass its `last_event_id` as
`--after-event-id` to fetch the next page. `history` returns revisions in
ascending order; pass `last_revision` as `--after-revision`. A page with no rows
returns a null last cursor. A cursor is an exclusive lower bound, not a snapshot
token: separate page requests can observe concurrently committed updates.

`verify` checks stored snapshots and their revision digest chains. Digests detect
inconsistency, not authorship: a party able to rewrite the entire database and
recompute every digest can create another internally valid history. This is not
a cryptographic signature or an external tamper-proof audit log. Deleting a
complete suffix of history can also leave a valid shorter chain without an
independently retained head digest to compare against.

Events contain at most 10,000 documents, and each persisted revision's complete
descriptor plus document payloads is capped at 128 MiB. History descriptors are
revalidated from the first revision even for a later page or point lookup, so
latency grows with the event's history length. Returned history pages are bounded
(`--limit` is 1–1,000); descriptor traversal retains only the bounded page and
current descriptor, while full event reads also materialize their document
payloads. Whole-store verification checks every referenced historical document
version, retaining a digest-to-document-ID inventory for deduplication.

## Input and file safety

Imports are UTF-8 and capped at 128 MiB before decoding. Duplicate JSON keys,
non-finite numbers, invalid nesting, and invalid document schemas, offsets,
references, or metadata are rejected before opening or creating the database.
Text is not normalized, and CRLF and Unicode combining marks are preserved.
Read commands open existing stores only; a misspelled database path does not
silently create an empty database.

Every command supports `--output FILE`; the default is JSON on standard output.
Serialized output is also capped at 128 MiB, including JSON formatting overhead.
File output is written to a temporary sibling and atomically replaced. The CLI
refuses input/output aliases of the database and its `-wal`, `-shm`, and
`-journal` sidecars, including symlinks and hardlinks. It also refuses output
that aliases an imported JSON file. Path identity errors fail closed. These
checks are not a defense against another process deliberately changing path
identities between validation and use; use a directory controlled by the caller.

Database commit and report-file publication are separate operations. If `put`
commits but subsequently cannot write its JSON response, the revision remains
committed while an existing output file is left intact. Check `get` or `history`
before retrying; the old expected revision will then correctly conflict. SQLite
provides transactional persistence, but atomic JSON replacement alone does not
promise persistence across power loss. Store contents, including text and
metadata, are not encrypted; protect the database and backups accordingly.
