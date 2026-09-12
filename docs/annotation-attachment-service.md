# Revision-pinned attachment RPC

This is an unreleased feature on `feat/whole-repository-alignment`. Use an
editable checkout of that branch; the release on PyPI and the default `main`
installation must not be assumed to contain it. No new runtime dependency,
model download, paid service, or remote migration is required.

Attachments are opaque bytes associated with an immutable event revision.
Their names are logical names, not paths, URLs, MIME instructions, or code.
Neither service nor client decodes an attachment as a document or sends it to
a processor. The existing `processor-request.v1` protocol is unchanged.

## Explicit local startup

The existing [local service boundary](annotation-service.md) still applies:
literal-loopback HTTP only, required bearer credentials from an environment
variable, fixed local processor registry, no proxy/redirect handling, no CORS,
and no internet-facing deployment claim. Every bearer holder has access to the
configured store; this is not per-participant authorization.

```python
from corpusledger.annotation_service import create_annotation_server
from corpusledger.attachment_types import AttachmentLimits

server = create_annotation_server(
    "private-events.sqlite",
    {},  # Or a fixed registry of explicitly configured remote pipelines.
    token_env="CORPUSLEDGER_LOCAL_TOKEN",
    enable_execution_journal=True,
    enable_attachments=True,
    attachment_limits=AttachmentLimits(max_store_bytes=64 * 1024 * 1024),
)
try:
    server.serve_forever()
finally:
    server.server_close()
```

Set the named credential in the launching environment, not in the Python
source or JSON configuration. A fresh/v1 database requires **both** flags:
the journal opt-in creates schema v2, and the separate attachment opt-in
upgrades v2 to v3. `enable_attachments=True` alone does not implicitly enable
the execution journal. Ordinary reopening never migrates a database. An
already migrated v3 database can serve attachments without repeating flags.
These are persistent local migrations, not an enable/disable switch for each
HTTP request. Existing v1 event serialization and old revision digests remain
unchanged; attaching creates a new v2 event descriptor.

The module entry point exposes the same explicit flags:

```text
python -m corpusledger.annotation_service --store private-events.sqlite --config workers.json --token-env CORPUSLEDGER_LOCAL_TOKEN --enable-execution-journal --enable-attachments
```

Local quota flags are `--attachment-max-blob-bytes`, `--attachment-max-names`,
`--attachment-max-event-bytes`, `--attachment-max-store-bytes`,
`--attachment-max-store-blobs`, and `--attachment-max-receipts`.
They cannot be supplied or changed by an RPC caller. Starting against a store
whose contents exceed newly reduced quotas may reject subsequent operations;
it does not delete existing data to satisfy those quotas.

## Python round trip

```python
from corpusledger.annotation_client import AnnotationClient

client = AnnotationClient("http://127.0.0.1:8123", "CORPUSLEDGER_LOCAL_TOKEN")
before = client.get("interview-1")  # An event must already exist for attach.
after = client.attach(
    "interview-1",
    "recording",
    b"\x00\xffopaque bytes",
    "application/octet-stream",
    expected_revision=before.revision,
    expected_digest=before.digest,
    command_id="upload-recording-1",
)
content = client.read_attachment("interview-1", "recording", revision=after.revision, expected_digest=after.digest)
inventory = client.list_attachments("interview-1", revision=after.revision, expected_digest=after.digest)
```

`attach` and `detach` require an exact source revision, its digest, and a stable
command ID. The command ID is store-wide. Reusing it with the same canonical
request returns its **original pinned result**, even if newer revisions now
exist. Reusing it for different bytes, names, metadata, action, or source pin
conflicts. The receipt and event revision commit in the same SQLite transaction.
Two independent writers targeting one head cannot both win. A lost connection
does not prove rollback: inspect the head or retry the **identical** command.
The client never automatically retries a write or invents a replacement ID.

Existing names cannot be overwritten by `attach`; explicitly detach first.
Detachment creates another event revision, retaining its version-2 format. It
does not erase historical manifests or bytes. There is no BLOB garbage
collection, physical purge, retention deadline, TTL, lease, or worker lock in
this slice. Historical bytes and receipts continue consuming quota. Ordinary
text annotation pipelines preserve the event's manifests and unselected sibling
documents; they do not receive the attachment payloads.

## Closed wire operations

All six operations use the existing authenticated `POST /v1/events` envelope:
`{"format":"corpusledger.event-command.v1","command":...,"arguments":...}`.
Arguments are exact: missing/unknown fields, booleans as revisions or sizes,
duplicate JSON keys, noncanonical Base64, and unsupported names/media types are
rejected. Base64 is standard padded ASCII, with canonical pad bits and no
whitespace. Empty bytes are valid. No client field may name a local file.

| Command | Exact arguments | Successful result |
| --- | --- | --- |
| `attachment_attach` | `event_id,name,data,media_type,expected_revision,expected_digest,command_id` | Full pinned revision |
| `attachment_detach` | `event_id,name,expected_revision,expected_digest,command_id` | Full pinned revision |
| `attachment_get` | `event_id,name,revision,expected_digest` | `format,event_id,revision,digest,attachment,data` |
| `attachment_list` | `event_id,revision,expected_digest` | `format,event_id,revision,digest,attachments` |
| `attachment_snapshot_export` | `event_id,revision,expected_digest` | Complete typed attachment snapshot |
| `attachment_snapshot_import` | `snapshot,command_id,expected_revision,expected_digest,expected_snapshot_digest` | Full pinned revision |

`data` is Base64. A manifest is exactly `name,sha256,size,media_type`. Read/list
result formats are `corpusledger.attachment-data.v1` and
`corpusledger.attachment-list.v1`. Lists are unique and sorted by logical name.
Successful responses still use `corpusledger.event-response.v1` with the exact
command, `ok:true`, and `result`. Errors return stable content-free codes;
conflicts are 409, wire/quota limits 413, and detected unavailable/corrupt
attachment storage 503. No raw BLOB, database exception, or credential appears
in an error diagnostic. Successful events/snapshots **are private data**, not
redacted publications.

The typed client checks returned pins, closed fields, canonical manifests and
Base64, byte counts/SHA-256, full revision descriptor digest, and the mutation's
canonical command/provenance binding. This detects inconsistent responses, not
a malicious authenticated server that fabricates a whole internally consistent
history. Read/list pins are checked **echoes**, not cryptographic membership
proofs: a malicious server could replace a read's bytes, manifest, and content
hash together while retaining the requested revision digest. Likewise, a list's
self-declared revision digest does not prove the entire unseen event. Neither
response includes the full revision descriptor needed to establish membership.
Mutation responses do include and validate that descriptor. TLS, signatures,
server attestation, or independent history anchoring are not added by these hashes.

## Snapshot transfer, not history restore

```python
snapshot = client.export_attachment_snapshot("interview-1", revision=after.revision, expected_digest=after.digest)
# Obtain this pin through the caller's trusted out-of-band transfer process.
trusted_snapshot_digest = snapshot.digest
target = AnnotationClient("http://127.0.0.1:8124", "CORPUSLEDGER_TARGET_TOKEN")
imported = target.import_attachment_snapshot(
    snapshot,
    command_id="import-interview-1",
    expected_snapshot_digest=trusted_snapshot_digest,
    expected_revision=0,
    expected_digest=None,
)
```

The example's assignment alone does not authenticate a received snapshot; an
external trusted digest must come from an independently trusted source when
that guarantee is needed. Export contains exactly one event, its declared source
pin, deduplicated sorted BLOBs, and a complete snapshot digest. Import preserves
the event ID and bytes, not its historical revision number. Revision zero/null
means create-only at the target; replacing an existing target requires its exact
positive revision/digest. Import appends one revision with the source pin in
provenance. It does not restore old history, execution operations, old receipts,
or authenticate the claimed source revision descriptor. No renaming is implied.

## Boundedness and verification scope

Defaults: 4 MiB/raw BLOB, 64 names and 8 MiB logical attachment bytes per event;
64 MiB unique retained BLOBs, 1,024 retained BLOBs, and 100,000 receipts per store.
The complete canonical snapshot is at most 12 MiB. Each HTTP request **and**
response, including JSON, event text, metadata and Base64 expansion, is at most
16 MiB (or a lower locally configured wire limit). A locally valid large event
can therefore be unavailable through this inline transport. Mutation responses
are preflighted before commit; CAS is rechecked inside the writer transaction.
Read failures produce no partial successful object. These are encoded/decoded
data and admission bounds, not a hard Python/SQLite RSS allocation ceiling.

The existing absolute socket deadline and bounded connection admission remain;
an expired client socket does not cancel accepted storage work. No streaming
upload/download, arbitrary process execution, or background cleanup is added.

`tests/test_annotation_attachment_service.py` uses real ephemeral loopback HTTP
servers with deterministic shutdown. It covers binary/empty data, separate
connections and races, original-receipt retry after lost response and newer
heads, old-version reads after detach/reopen, snapshot create/replace/idempotence,
quota/response preflight atomicity, deliberate temporary-database corruption,
strict malicious response checks, and manifest/sibling preservation through the
existing execution API. Its worker callback is an explicitly local test double;
it is not evidence of remote-model processing or broad security certification.
