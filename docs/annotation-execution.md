# Durable remote annotation execution

This unreleased development-branch API extends the [persistent event store](annotation-store.md)
with a same-database execution journal. A configured DAG can call independent
processors, save each validated successor, and publish all selected documents
and provenance together. The [Go and Java examples](annotation-workers.md) are
real, separately compiled processes; the [event service](annotation-service.md)
provides an authenticated local API around the coordinator.

This is not a distributed exactly-once scheduler. A worker may have performed
external effects even when its response was lost. A caller must explicitly
acknowledge that possibility before retrying unresolved work.

## API and identity

An application constructs `RemoteAnnotationProcessor(endpoint, description,
token_env)` from a trusted configuration. `description` is a pinned
`ProcessorDescription`, including name, version, full required/produced schemas,
and a configuration SHA-256. Do not silently trust a changed `/v1/info` identity
on every run; inspect and pin intended changes in the application configuration.
The token value is read from the named environment variable for every exchange
and never enters an operation, request fingerprint or provenance record.

```python
from corpusledger import AnnotationExecutor, AnnotationStore, RemoteAnnotationPipeline

# go_worker and java_worker are explicitly configured/pinned adapters.
pipeline = RemoteAnnotationPipeline("tokens-and-groups", "1", (java_worker, go_worker))
selected = {"article": pipeline}

with AnnotationStore("events.sqlite", create=False) as store:
    store.enable_execution_journal()  # explicit, durable schema-v2 opt-in
    source = store.get("article-event")
    executor = AnnotationExecutor(store)
    operation = executor.begin(
        "article-run-001",
        "article-event",
        selected,
        expected_revision=source.revision,
        expected_digest=source.digest,
    )
    completed = executor.resume(operation.operation_id, selected)
    assert completed.status == "committed"
    revision = store.get("article-event")
```

The pipeline validates and topologically sorts exact schemas, so registration
order need not be execution order. Every selected document is preflighted before
any processor call. Documents not selected remain unchanged. A request may use
different configured pipelines for different documents. The whole execution is
bounded to 128 document/processor steps, not 128 per document.

`begin` binds the operation ID to the source event ID, exact revision **and revision
digest**, document selection, pipeline IDs/versions, ordered processor schemas,
configuration hashes and endpoint hashes. The revision digest is the descriptor's
`source.digest`, not `source.event.digest` or a text digest. Reusing the same ID and
request returns the durable state without contacting workers, including after a
successful commit. A different request must use a new operation ID.

`get(operation_id)` verifies the journal and its saved documents without calling
workers. `resume` on an already committed operation also retrieves its validated
result without requiring a still-available worker configuration; use `begin` to
check a proposed request against the bound request. Unfinished resume requires
the original exact DAG and pinned identities. Rotating an environment credential
does not change the execution identity; changing an endpoint does.

`list(after_operation_id=..., limit=...)` discovers verified operation heads,
including unresolved reservations after a coordinator restart.
`history(operation_id, after_version=..., limit=...)` exposes the verified
reservation/result/retry transitions. Both use ascending exclusive cursors and
accept limits from 1 to 1,000. They return complete metadata snapshots; choose
smaller pages when plans are large. Neither method invokes workers.

The generic in-memory `AnnotationProcessor`/`AnnotationPipeline` reports name and
version only. The durable executor's additional provenance records the complete
pinned worker description and endpoint hash; converting a remote adapter to a
generic callable does not give the generic pipeline this durable contract.

## Reservations, interruption and recovery

| Durable state | Meaning | Default resume |
| --- | --- | --- |
| `ready` | No unresolved call; zero or more validated results are saved | Continue at the next step, or commit |
| `reserved` | A durable attempt was reserved; its worker may still be running or the coordinator may have died | Refuse to replay |
| `uncertain` | The coordinator caught an unavailable/invalid worker result | Refuse to replay |
| `committed` | Event and operation were committed atomically | Return the same result |
| `conflict` | Source event changed; no execution event revision was published | Require a new operation on a reviewed source |

To deliberately retry a `reserved` or `uncertain` operation:

```python
completed = executor.resume("article-run-001", selected, retry_uncertain=True)
```

This is an explicit acknowledgement of possible duplicate external effects,
not proof that the old worker stopped. It invalidates the old reservation. A
late old response cannot replace the new attempt's result. Attempts are capped
at 1,000 per step. Saved earlier steps are reused rather than called again.
Once that limit is reached, review the external state instead of treating a new
operation ID as an automatic safe retry.

Ctrl-C and other `BaseException` interruptions do not get converted into a
reported worker failure; the existing reservation remains discoverable. Ordinary
transport/schema failures store only `worker_result_unavailable`, never raw
exceptions, server bodies or credentials. A successful worker HTTP response is
still not an event commit. A lost client response after final commit is recovered
by the operation ID, without appending a second event revision.

An ordinary competing event `put` cannot be overwritten by this execution. Final
publication compares the original revision and digest under the SQLite writer
lock. A source change after `begin` may be discovered **after processors have
run**; external effects cannot be rolled back. This API does not promise immediate
cancellation of work when some other process edits the event.

## Storage and verification

Ordinary stores remain schema v1. `enable_execution_journal()` explicitly performs
a transactional, idempotent v1→v2 migration, adding operation heads, append-only
operation history and guards. Existing event descriptors, document bodies,
revision numbers and digests are preserved byte-for-byte. Opening a store never
implicitly upgrades it. Old v1-only programs reject the upgraded format; back up
and coordinate readers before opting in. There is no automatic downgrade.

Each reservation commits before the network call. SQLite writer locks are not
held across worker calls. A successful response is checked against its operation,
step, pinned description and exact input digest. The coordinator appends only
declared new layers and revalidates text, codepoint spans, existing annotations,
feature types and references. The output snapshot and the saved step journal
state are then committed in one transaction using the current reservation CAS.

Final publication pre-serializes the event outside the writer lock, then appends
the event revision and marks the operation committed in one SQLite transaction.
No independently committed sidecar is used. A crash or storage failure at that
boundary cannot publish only half of the event/operation pair.

Operation histories have contiguous versions, content digests, parent digests,
closed schemas and checked state transitions. Materialization reconstructs every
saved successor from its verified prior state and compares canonical digests,
not Python equality (where `True == 1`). Committed event/provenance must match the
journal. These checks detect corruption; they are not signatures against someone
able to rewrite all database bytes. `AnnotationStore.verify()` verifies event
history; `AnnotationExecutor.get()` additionally verifies an operation's journal
and intermediate documents. The former is not a substitute for the latter.

## Limits and intended scope

Connections/executors are thread-affine. Use one store per thread/process; SQLite
writer serialization and operation CAS arbitrate competing coordinators. The
transport accepts only HTTP numeric loopback origins, no DNS/proxy/redirects,
uses bounded strict JSON and required environment credentials, and validates
worker identity before each POST. A verification GET and a processing POST have
separate exchange deadlines; those deadlines do not bound local JSON/schema CPU.

The wire and operation snapshot limit is 16 MiB; local event snapshots separately
allow up to 128 MiB including selected document payloads. A document accepted by
the local event store can therefore be too large for a remote request. Page/step
and byte budgets are not process-RSS or wall-clock guarantees. Journal snapshots
repeat bounded plan/completion metadata; reads verify the whole operation history
and saved predecessors, so very long retry histories add work. No compaction,
distributed leases, public multi-tenancy, binary document objects, service
discovery, model-quality evaluation or cross-host deployment is claimed here.
