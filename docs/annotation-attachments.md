# Versioned event attachments

Attachments are arbitrary immutable bytes associated with an event revision by a
logical name. They are not file paths, executable payloads, automatically decoded
text, processor inputs, or object-store URLs. A manifest records exactly
`name`, lowercase `sha256`, integer `size`, and caller-declared lowercase
`media_type` (`type/subtype`, without parameters). Media types are not sniffed or
trusted as proof of content. Names exclude path separators, colon, control
characters, `.`/`..`, and leading/trailing whitespace; they are limited to 256
UTF-8 bytes. Do not turn an untrusted logical name into a filesystem path.

## Explicit versions and migration

Existing `AnnotationEvent(..., version=1)` is the default. Its JSON fields and
digest remain exactly the previous `corpusledger.annotation-event.v1` contract.
Only explicit `version=2` permits `attachments`; its JSON format is
`corpusledger.annotation-event.v2`, including an attachments array even when empty.
The first attach command creates such a v2 event. Detaching its last name leaves
an empty **v2** event, not a silently downgraded v1 event.
Once an event head is v2, ordinary `put` and snapshot import reject a v1 successor;
older callers cannot unknowingly remove its current manifest. Historical v1
revisions remain readable. Low-level `put` with an explicitly constructed v2 event
and an empty/replacement manifest can intentionally change references, subject to
CAS and existing valid BLOB checks. That is an ordinary event write, **not** an
attachment command: it does not fabricate an attachment command receipt. Use the
dedicated attach/detach API when durable command-id retry semantics are required.

```python
from corpusledger.annotation_store import AnnotationStore

with AnnotationStore("annotations.sqlite", create=False) as store:
    store.enable_execution_journal()  # Explicit v1 -> v2, if not already enabled.
    store.enable_attachments()  # Explicit v2 -> v3.
    assert store.execution_enabled and store.attachments_enabled
```

Ordinary creation remains schema v1; opening, reading, and checking capability
properties never migrate. `enable_attachments()` refuses v1, so both upgrades
are deliberate. The v3 migration is transactional and idempotent across
independent connections. It adds `annotation_blobs` and
`annotation_attachment_receipts`, with update/delete rejection triggers, while
preserving every existing document, revision, and operation-journal row byte for
byte. Existing execution APIs continue to work on v3. Older software supporting
only v1/v2 rejects the newer schema instead of ignoring attachments.

Stored revision descriptors also use an explicit new
`corpusledger.annotation-store.v2` format with the manifest included in their
digest. Old descriptor bytes remain unchanged. `AnnotationStore.process()` and
`AnnotationExecutor` preserve the source manifest and event version while
replacing selected annotation documents. `processor-request.v1` is unchanged:
workers receive their original document request, not attachment bytes.

## Atomic commands and pinned reads

```python
from corpusledger.annotation_attachments import AnnotationAttachments
from corpusledger.annotation_store import AnnotationStore

with AnnotationStore("annotations.sqlite", create=False) as store:
    attachments = AnnotationAttachments(store)
    source = store.get("conversation")
    saved = attachments.attach(
        "conversation",
        "source.bin",
        bytes(range(256)),
        "application/octet-stream",
        expected_revision=source.revision,
        expected_digest=source.digest,
        command_id="upload-source-001",
    )
    assert attachments.read("conversation", "source.bin", revision=saved.revision) == bytes(range(256))
    removed = attachments.detach(
        "conversation",
        "source.bin",
        expected_revision=saved.revision,
        expected_digest=saved.digest,
        command_id="detach-source-001",
    )
    assert attachments.list("conversation", revision=removed.revision) == ()
    assert attachments.read("conversation", "source.bin", revision=saved.revision) == bytes(range(256))
```

This example assumes an existing event with no `source.bin` attachment. An
existing logical name is never overwritten by attach, even with identical bytes.
To associate different bytes with that name, detach it first, then attach against
the resulting revision. Every successful command creates one event revision.

Both the expected revision number **and its revision digest** are required; the
digest is not the standalone event digest. The command ID is bounded to 256 UTF-8
bytes and bound to the exact canonical request: action, event, source pin, logical
name/manifest or snapshot identity. The request stores hashes and sizes, not raw
bytes. The same command with identical inputs returns its **original pinned
result**, including after later revisions or a process restart. Changed inputs
cannot reuse that command ID. A different command ID is not an idempotent retry.
`receipt(command_id)` verifies the stored request, result, provenance, source
ancestry, and content before returning its pinned revision.

Content-addressed BLOB insertion, new revision, and receipt insertion occur in one
SQLite writer transaction. Failure rolls all three back; a failed command does
not leave an orphan BLOB. Distinct event names and revisions may share identical
BLOB content without storing another copy. No user callbacks or network calls run
inside these commands. Concurrent independent connections are serialized by
SQLite and checked with optimistic CAS; a source changed by attachment operations
also conflicts with pending document-processing publication.

`preview_attach` and `preview_detach` accept the same arguments as their mutation
methods and return the precise proposed revision without writes. They allow a
transport to admit its response size before publication. They do not reserve the
source or guarantee future quota availability: subsequent changes produce a
conflict, not permission to publish the stale preview. Snapshot import has a
matching `preview_import_snapshot` helper.

Read and list require an explicit positive revision. They validate its ancestry,
manifest, and referenced BLOBs. Before fetching bytes, the store checks SQLite
storage type, physical length, declared size, and the manifest size; then it hashes
the bounded bytes. Missing content, changed bytes, text stored in a BLOB column,
or mismatched sizes are errors. `verify()` now checks referenced attachments in
addition to documents without changing the meanings of its existing
`events`, `revisions`, and `documents` counters.

## Resource policy and retention

`AttachmentLimits` defaults to:

| Bound | Default | Meaning |
| --- | ---: | --- |
| `max_blob_bytes` | 4 MiB | One immutable byte payload |
| `max_names` | 64 | Logical names in one event revision |
| `max_event_bytes` | 8 MiB | Sum of manifest sizes, counting each name |
| `max_store_bytes` | 64 MiB | Physical unique BLOB bytes in this database |
| `max_store_blobs` | 1,024 | Physical unique BLOB rows |
| `max_receipts` | 100,000 | Durable attachment command receipts |

The first three cannot exceed their defaults. The last three have explicit hard
ceilings of 1 GiB, 100,000 BLOBs, and 1,000,000 receipts. Smaller policies, including
zero capacity, are allowed. Policies are supplied by the caller/service operator;
they are not durable tenant-wide authorization rules. Physical quotas are checked
under the writer lock, including previously stored content and receipts. Existing
matching receipts are checked before new-write quota admission. Configuration
changes can therefore stop new writes without silently deleting old data.

Detaching removes a name only from the new manifest. Prior revisions, BLOB bytes,
and receipts are retained. There is no garbage collection, secure erasure,
retention deadline, leasing, or distributed exactly-once execution claim. SQLite
pages, document bodies, receipt text, indexes, journaling, and allocator overhead
are **not** included in the unique-BLOB byte quota; it is not a database-file-size
or process-memory guarantee. Large receipt inventories require explicit operator
capacity planning.

`AttachmentQuotaError` identifies configured logical/physical/receipt or complete
snapshot-byte exhaustion. `AttachmentConflictError` identifies command/name/digest
conflicts; revision-number conflicts retain `AnnotationConflictError`.
`AttachmentCorruptionError` and `AnnotationStoreError` indicate unverifiable
persisted content or storage failure. Invalid input types/closed schemas use
`AttachmentError`/`InputError`. No error is a successful mutation receipt.

## Single-version snapshots

```python
from corpusledger.annotation_attachment_snapshot import export_snapshot, import_snapshot

snapshot = export_snapshot(source_store, "conversation", revision=3)
snapshot_bytes = snapshot.to_bytes()
copied = import_snapshot(target_store, snapshot, command_id="copy-001")
assert copied.revision == 1  # New target history, not restored source history.
```

Both stores must be explicitly schema v3. The source pin is mandatory.
`AnnotationAttachmentSnapshot.from_dict()` accepts the closed format
`corpusledger.annotation-attachment-snapshot.v1` with:

- `source`: original `event_id`, `revision`, and revision `digest`;
- `event`: one complete versioned event;
- `blobs`: sorted, deduplicated `{sha256, size, base64}` records;
- `sha256`: SHA-256 of the canonical envelope body excluding this field.

Missing, extra, duplicate, mis-sized, wrong-hash, unsorted, and noncanonical Base64
entries are rejected. Padding bits must be canonical; whitespace and extra padding
are not ignored. Arbitrary bytes, including empty content, all 256 byte values,
NUL, and invalid UTF-8, are preserved exactly. Only their transport representation
is Base64. The full canonical UTF-8 envelope, including metadata, documents,
Base64 expansion and checksum, is bounded to **12 MiB**, leaving room below the
current 16 MiB service envelope cap. This is a separate bound: an event or BLOB
inventory accepted by storage may still be too large to export together.

Serialization visits documents/annotations incrementally and escapes/encodes
strings in chunks; it does not first expand a full 128 MiB event into dictionaries.
The complete snapshot is validated before the import writer transaction. Export
and import accept `limits=AttachmentLimits(...)` and enforce logical/physical
admission as appropriate. Import creates or CAS-replaces the envelope's event ID;
it never renames it. `expected_revision=0` requires `expected_digest=None` and means
create-only. A positive expectation requires its exact target revision digest.

Source revision/digest values are retained as provenance, **not** installed as
target revision numbers or parent links. A snapshot is not a historical database
backup and contains no execution journal, earlier versions, receipts, or signatures.
SHA-256 consistency is not authentication: an attacker can edit content and
recompute an envelope hash. Compare `snapshot.digest` with an independently trusted
value when origin integrity is required. Treat snapshots and BLOBs as sensitive
data; the library performs no malware scanning, redaction, consent checking, or
decryption.
