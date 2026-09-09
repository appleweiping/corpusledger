# Local annotation event service

This development-branch API connects immutable multi-document events, SQLite
revision history, and durable remote annotation execution. It is a local
integration service, not a public internet deployment, hosted platform, or
general code-execution endpoint. No model download or paid API is required by
the service itself. Registered workers must be started separately.

These features are unreleased. Use the `feat/whole-repository-alignment` checkout
and `python -m pip install -e .`; installing the repository's default branch or
an older package does not select these APIs.

## Configuration and startup

The server owns one explicitly selected database and a fixed registry of pinned
`RemoteAnnotationPipeline` values. Clients select a registered pipeline by ID;
they cannot provide database paths, Python imports, executable code, worker
origins, or processor schemas. Registry keys must equal pipeline IDs. There are
at most 128 registered pipelines and at most 128 selected execution steps.

```python
from corpusledger.annotation_service import create_annotation_server

# registry maps IDs to explicitly constructed RemoteAnnotationPipeline objects.
# Supply the credential through your process environment, not this file.
server = create_annotation_server(
    "events.sqlite",
    registry,
    token_env="ANNOTATION_SERVICE_TOKEN",
    enable_execution_journal=True,
    host="127.0.0.1",
    port=8031,
)
try:
    server.serve_forever()
finally:
    server.server_close()
```

For threaded embedding, call `server.shutdown()` from a different thread before
`server.server_close()`, as with Python's `socketserver` lifecycle. The factory
binds the listening socket before creating/upgrading the database, so an occupied
port does not cause a migration. Missing parents are not automatically created.

The execution journal is **never enabled implicitly**. Pass
`enable_execution_journal=True` or the CLI flag below to opt into the additive
store-format upgrade. Without the upgrade, event create/get/list/history still
work; execution commands return `execution_disabled`. Reopening an already
upgraded store does not require repeating the flag. Older v1-only readers cannot
open the upgraded store; preserve a backup before your first upgrade.

```text
python -m corpusledger.annotation_service --store events.sqlite --config service.json --token-env ANNOTATION_SERVICE_TOKEN --host 127.0.0.1 --port 8031 --enable-execution-journal
```

The configuration is strict JSON with exactly this shape. Replace the example
description with the actual independently pinned worker description; a worker's
live description must agree before processing. The example is structural, not a
claim that a worker with this hash exists.

```json
{
  "format": "corpusledger.annotation-service-config.v1",
  "pipelines": [
    {
      "id": "example",
      "version": "1",
      "workers": [
        {
          "endpoint": "http://127.0.0.1:8032",
          "token_env": "EXAMPLE_WORKER_TOKEN",
          "timeout": 10,
          "max_response_bytes": 16777216,
          "description": {
            "format": "corpusledger.processor.v1",
            "name": "example",
            "version": "1",
            "config_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "requires": [],
            "produces": [{"name": "span", "fields": {}}]
          }
        }
      ]
    }
  ]
}
```

Only environment **names** belong in this file. The Python service and remote
adapters accept bearer values of 16–4096 printable ASCII non-space characters, read from their
named environment variables at request time. They are not logged, echoed, or
persisted in operation identities. The CLI rejects config files aliasing the
database or `-wal`, `-shm`, `-journal` sidecars, including existing hard links.
The Go/Java examples have a narrower 32–256 character token alphabet; use a token
accepted by both sides as documented in [worker setup](annotation-workers.md).

## Typed client and recovery

```python
from corpusledger.annotation_client import AnnotationClient
from corpusledger.annotation_store import AnnotationEvent
from corpusledger.annotations import AnnotationDocument

client = AnnotationClient("http://127.0.0.1:8031", "ANNOTATION_SERVICE_TOKEN")
event = AnnotationEvent(
    "conversation-1",
    (
        AnnotationDocument("original", "Exact source text."),
        AnnotationDocument("related", "A separately identified related view."),
    ),
)
revision = client.create(event)  # Create only; duplicate IDs conflict.
operation = client.begin(
    "operation-1",
    event.event_id,
    {"original": "example"},
    expected_revision=revision.revision,
    expected_digest=revision.digest,
)
assert operation.status == "ready"  # Preflight completed, no worker POST yet.
completed = client.resume("operation-1", {"original": "example"})
updated = client.get(event.event_id)
assert completed.status == "committed"
assert updated.event.get_document("related").digest == event.get_document("related").digest
```

The client validates closed envelopes, event/revision identity, the canonical
revision descriptor digest, typed document schemas and references, operation
state invariants, and adjacent operation-history transitions. Hash validation is
not remote attestation: a malicious local service can fabricate a self-consistent
history. Cross-page history ancestry must be retained/compared by applications
that need verification across separately fetched pages.

After a lost client response or service restart, inspect `status(operation_id)`
before deciding what to do. `operations(after_operation_id=None, limit=100)`
discovers operation heads; `operation_history(operation_id, after_version=0,
limit=100)` returns immutable versions. Event `list(after_event_id=None,
limit=100)` and `history(event_id, after_revision=0, limit=100)` have exclusive
cursors. Limits are integers from 1 to 1000; booleans are not integers here.

`begin` binds the operation ID to the exact source revision/digest, complete DAG,
processor descriptions, and worker endpoint hashes. Repeating the same `begin`
returns the existing operation without contacting workers. Changing that binding
conflicts. Every worker call has a durable reservation before its effects, and
validated successor documents are checkpointed before subsequent steps.

A default `resume` does not replay an uncertain/reserved worker call. Explicit
`retry_uncertain=True` acknowledges possible duplicate external effects, including
a previous call that is still running. Late responses cannot replace a newer
reservation. Final event publication and the committed operation state share one
SQLite transaction; a changed source causes a conflict, not a partial event
revision. However, previously invoked workers may already have performed effects.
There is no exactly-once or automatic cancellation promise.

The HTTP `resume` command requires the same registered selection and pinned
configuration even for committed operations, but does not contact workers again
for a matching committed operation. The Python executor's committed `resume`
shortcut is less restrictive and can retrieve completion without pipeline
availability. HTTP `status`, `get`, and operation history remain recovery reads
independent of the registry; they can be used after configuration changes.

## Wire and resource boundary

Only `POST /v1/events HTTP/1.1` is supported, one request per connection. A command
has exact keys `format`, `command`, `arguments`, with format
`corpusledger.event-command.v1`. Arguments have the following exact fields:

| Command | Arguments |
| --- | --- |
| `create` | `event` (complete serialized annotation event) |
| `get` | `event_id`, `revision` (integer or null for latest) |
| `list` | `after_event_id` (string or null), `limit` |
| `history` | `event_id`, `after_revision`, `limit` |
| `begin` | `operation_id`, `event_id`, `pipelines`, `expected_revision`, `expected_digest` |
| `resume` | `operation_id`, `pipelines`, `retry_uncertain` (boolean) |
| `status` | `operation_id` |
| `operations` | `after_operation_id` (string or null), `limit` |
| `operation_history` | `operation_id`, `after_version`, `limit` |

Success is exactly `{format, command, ok: true, result}`. Failure is exactly
`{format, command, ok: false, error: {code}}`; format is
`corpusledger.event-response.v1`. For unrecognized/unauthenticated requests,
`command` may be null. Stable error codes include `request_invalid`,
`unauthorized`, `not_found`, `conflict`, `uncertain`, `execution_disabled`,
`too_large`, `unavailable`, and `internal_error`. The client adds redacted local
transport/validation errors; it never exposes raw server error content.

- Numeric IPv4/IPv6 loopback origins only: no DNS hosts, non-loopback binds,
  endpoint paths, credentials in URLs, proxies, or redirects. Canonical `Host`
  must match the listening origin, including its explicit port.
- Bearer authentication is required. Requests containing `Origin`, proxy
  authorization/connection headers, `Expect`, transfer encoding, or content
  encoding are rejected. No CORS headers or browser access mode exists.
- Exactly one unambiguous decimal content length and JSON content type are
  required. Duplicate headers, folding, malformed ASCII headers, duplicate JSON
  keys, non-finite values, unknown fields, and excessive JSON depth are rejected.
- Header lines are at most 8192 bytes; total headers/request line at most 16384
  bytes; at most 32 headers. Input and output JSON each default to 16 MiB.
  A smaller service limit can be configured from 1024 bytes upward. Execution
  needs more than 64 KiB: 64 KiB is reserved for all bounded operation checkpoint
  records and envelope overhead before an operation is accepted. Large read
  pages return `too_large`; request a smaller page. Events created by other APIs
  may be too large to fetch through this transport.
- Create preflights the full revision response size before publication. Worker
  outputs can still make a committed event too large for HTTP `get`; its compact
  operation status remains available and the local store API can retrieve the
  event. An HTTP response failure never proves a write did not happen.
- Default admission is eight concurrent request threads (configurable 1–64).
  Excess connections are closed without reading their bodies. Each accepted
  command owns its own thread-affine SQLite connection. There is no unbounded
  application queue and no background scheduler.
- A monotonic socket deadline (default 30 s, maximum 300 s) spans all request
  headers/body and response writes, including trickled traffic. Short polling
  bounds idle socket shutdown without deadline timer threads. Client deadlines
  cover connect/send/receive, including blocked sends; timers are joined.
- Socket deadlines do **not** bound JSON/schema CPU work, SQLite history scans,
  process RSS, or accepted worker execution. Shutdown stops acceptance and closes
  sockets, then waits at most one second for handler threads. Already accepted
  execution may continue in bounded daemon threads until it finishes or the host
  process exits; inspect the durable operation after restart. This is not a
  multi-tenant or hostile-native-code sandbox.

Tests use actual local sockets for framing, auth, deadlines, admission and client
validation, and temporary SQLite databases for revisions/concurrent creation and
restart recovery. Unit worker callbacks are controlled fixtures. Separately,
`interop/verify_execution.py` exercises a Python service process and real Go/Java
worker processes, including service restart. `interop/verify_workers.py`
separately exercises a lost Java worker, saved Go output and explicit uncertain
retry against the durable executor.
