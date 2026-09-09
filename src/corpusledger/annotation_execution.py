"""Durable, explicitly recoverable remote annotation DAG execution.

Reservations and validated successor documents survive coordinator restarts.
Worker calls happen outside SQLite write transactions. The final event revision
and committed operation state share one transaction; remote external effects do
not. A lost or interrupted worker response therefore requires an explicit retry.
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ._annotation_journal import (
    FORMAT,
    MAX_ATTEMPTS,
    MAX_STEPS,
    AnnotationExecutionConflict,
    AnnotationExecutionError,
    AnnotationExecutionUncertain,
    ExecutionJournal,
    successor,
    validate_request,
    validate_state,
    validate_transition,
)
from .annotation_pipeline import AnnotationPipeline
from .annotation_protocol import AnnotationRequest, AnnotationResponse, ProcessorDescription, identifier, sha256_text
from .annotation_remote import RemoteAnnotationProcessor
from .annotation_store import (
    AnnotationEvent,
    AnnotationRevision,
    AnnotationStore,
    _digest,
    _json,
    _limit,
    _loads,
    _revision,
)
from .annotations import AnnotationDocument, _freeze, _name, _thaw


@dataclass(frozen=True, slots=True)
class RemoteAnnotationPipeline:
    """Application-configured DAG; callers cannot supply executable endpoints."""

    pipeline_id: str
    version: str
    processors: tuple[RemoteAnnotationProcessor, ...]

    def __post_init__(self) -> None:
        identifier(self.pipeline_id, "pipeline ID")
        identifier(self.version, "pipeline version")
        if not isinstance(self.processors, (tuple, list)) or not 1 <= len(self.processors) <= MAX_STEPS:
            raise AnnotationExecutionError("remote pipeline must contain between 1 and 128 processors")
        if any(not isinstance(worker, RemoteAnnotationProcessor) for worker in self.processors):
            raise AnnotationExecutionError("remote pipeline requires pinned RemoteAnnotationProcessor values")
        object.__setattr__(self, "processors", tuple(self.processors))
        # Validate duplicate names now; exact dependency compatibility is checked
        # for every selected input document before any worker can be invoked.
        AnnotationPipeline(worker.description.as_processor(lambda _: ()) for worker in self.processors)

    def plan(self, document: AnnotationDocument) -> tuple[RemoteAnnotationProcessor, ...]:
        ordered = AnnotationPipeline(worker.description.as_processor(lambda _: ()) for worker in self.processors).plan(
            document
        )
        by_name = {worker.description.name: worker for worker in self.processors}
        return tuple(by_name[name] for name in ordered)


@dataclass(frozen=True, slots=True)
class AnnotationOperation:
    """Immutable journal snapshot, excluding raw documents and credentials."""

    _state: Mapping[str, Any]

    def __post_init__(self) -> None:
        validated = validate_state(_thaw(_freeze(self._state)))
        if validated["version"] == 1:
            validate_transition(None, validated)
        object.__setattr__(self, "_state", _freeze(validated))

    @property
    def operation_id(self) -> str:
        return str(self._state["operation_id"])

    @property
    def version(self) -> int:
        return int(self._state["version"])

    @property
    def status(self) -> str:
        return str(self._state["status"])

    @property
    def completed_steps(self) -> int:
        return len(self._state["completed"])

    def to_dict(self) -> dict[str, Any]:
        return dict(_thaw(self._state))


class AnnotationExecutor:
    """A thread-affine coordinator over an explicitly upgraded AnnotationStore.

    Use independent stores/executors for concurrent threads or processes. The
    operation ID is bound to the complete source revision, DAG, schemas, worker
    configuration and endpoint hashes. Credential rotation does not change that
    identity. ``begin`` only plans and verifies; ``resume`` performs processing.
    """

    def __init__(self, store: AnnotationStore) -> None:
        if not isinstance(store, AnnotationStore) or not store.execution_enabled:
            raise AnnotationExecutionError("explicitly enable the store execution journal first")
        self.store = store
        self._journal = ExecutionJournal(store)

    def _plan(
        self, source: AnnotationRevision, pipelines: Mapping[str, RemoteAnnotationPipeline]
    ) -> tuple[dict[str, Any], tuple[RemoteAnnotationProcessor, ...]]:
        if not isinstance(pipelines, Mapping) or not 1 <= len(pipelines) <= MAX_STEPS:
            raise AnnotationExecutionError("pipelines must select between 1 and 128 documents")
        selected = dict(pipelines)
        for document_id, pipeline in selected.items():
            _name(document_id, "selected document ID")
            if not isinstance(pipeline, RemoteAnnotationPipeline):
                raise AnnotationExecutionError("selected pipelines must be RemoteAnnotationPipeline values")
        steps: list[dict[str, Any]] = []
        workers = []
        for document_id, pipeline in sorted(selected.items()):
            document = source.event.get_document(document_id)
            for worker in pipeline.plan(document):
                steps.append(
                    {
                        "step_id": f"s{len(steps):03d}",
                        "document_id": document_id,
                        "pipeline_id": pipeline.pipeline_id,
                        "pipeline_version": pipeline.version,
                        "worker": worker.identity,
                    }
                )
                workers.append(worker)
                if len(steps) > MAX_STEPS:
                    raise AnnotationExecutionError("complete execution exceeds 128 steps")
        request = validate_request(
            {
                "event_id": source.event_id,
                "expected_revision": source.revision,
                "expected_digest": source.digest,
                "steps": steps,
            }
        )
        return request, tuple(workers)

    def _check_source(self, request: dict[str, Any]) -> bool:
        current = self.store._get(request["event_id"], None)
        return (current.revision, current.digest) == (request["expected_revision"], request["expected_digest"])

    def begin(
        self,
        operation_id: str,
        event_id: str,
        pipelines: Mapping[str, RemoteAnnotationPipeline],
        *,
        expected_revision: int,
        expected_digest: str,
    ) -> AnnotationOperation:
        """Bind an idempotency key after full DAG and live worker identity checks.

        Repeating the same bound request returns its durable state without
        contacting workers, even after a successful commit changed the event.
        A changed request cannot reuse the operation ID.
        """
        identifier(operation_id, "operation ID")
        _name(event_id, "event ID")
        _revision(expected_revision)
        sha256_text(expected_digest, "source revision digest")
        source = self.store.get(event_id, expected_revision)
        if source.digest != expected_digest:
            raise AnnotationExecutionConflict("source revision digest does not match")
        request, workers = self._plan(source, pipelines)
        with self.store._transaction():
            try:
                existing = self._journal.load(operation_id)
            except KeyError:
                existing = None
            if existing is not None:
                if existing["request"] != request:
                    raise AnnotationExecutionConflict("operation ID is already bound to a different request")
                self._materialize(existing)
                return AnnotationOperation(existing)
            if not self._check_source(request):
                raise AnnotationExecutionConflict("source event changed before execution began")
        # No writer lock is held over network verification. Recheck everything
        # under the writer lock before publishing the durable initial state.
        for worker in workers:
            worker.verify()
        state = {
            "format": FORMAT,
            "operation_id": operation_id,
            "version": 1,
            "request": request,
            "request_digest": _digest(request),
            "status": "ready",
            "completed": [],
            "attempts": [0] * len(workers),
            "reservation": None,
            "error": None,
            "result": None,
        }
        with self.store._transaction(write=True):
            try:
                existing = self._journal.load(operation_id)
            except KeyError:
                existing = None
            if existing is not None:
                if existing["request"] != request:
                    raise AnnotationExecutionConflict("operation ID is already bound to a different request")
                self._materialize(existing)
                return AnnotationOperation(existing)
            if not self._check_source(request):
                raise AnnotationExecutionConflict("source event changed during worker preflight")
            self._journal.save(state, None)
        return AnnotationOperation(state)

    def _materialize(self, state: dict[str, Any]) -> tuple[AnnotationEvent, dict[str, Any]]:
        """Verify saved successors against schemas and every prior immutable layer."""
        binding = state["request"]
        source = self.store._get(binding["event_id"], binding["expected_revision"])
        if source.digest != binding["expected_digest"]:
            raise AnnotationExecutionError("execution source revision no longer matches its binding")
        documents = {document.document_id: document for document in source.event.documents}
        for index, saved in enumerate(state["completed"]):
            step = binding["steps"][index]
            current = documents[step["document_id"]]
            output = self.store._document(saved["output_digest"])
            description = ProcessorDescription.from_dict(step["worker"]["processor"])
            request = AnnotationRequest(state["operation_id"], step["step_id"], description, current)
            prior_ids = {annotation.annotation_id for annotation in current.annotations}
            response = AnnotationResponse(
                state["operation_id"],
                step["step_id"],
                description,
                saved["input_digest"],
                tuple(annotation for annotation in output.annotations if annotation.annotation_id not in prior_ids),
                saved["duration_ms"],
            )
            if response.apply(request).document.digest != output.digest:
                raise AnnotationExecutionError("saved step does not preserve its immutable input document")
            documents[step["document_id"]] = output
        event = AnnotationEvent(source.event_id, tuple(documents.values()), source.event.metadata)
        provenance = {
            "annotation_execution": {
                "operation_id": state["operation_id"],
                "request_digest": state["request_digest"],
                "request": binding,
                "completed": state["completed"],
            }
        }
        if state["status"] == "committed":
            final = self.store._get(source.event_id, state["result"]["revision"])
            if (
                final.digest != state["result"]["digest"]
                or final.event.digest != event.digest
                or _digest(_thaw(final.provenance)) != _digest(provenance)
            ):
                raise AnnotationExecutionError("committed event and execution journal do not match")
        return event, provenance

    def get(self, operation_id: str) -> AnnotationOperation:
        """Verify journal ancestry, saved documents and any committed event."""
        with self.store._transaction():
            state = self._journal.load(operation_id)
            self._materialize(state)
            return AnnotationOperation(state)

    def list(
        self,
        *,
        after_operation_id: str | None = None,
        limit: int = 100,
    ) -> tuple[AnnotationOperation, ...]:
        """Discover verified operation heads, including unresolved reservations.

        Operation IDs use binary ascending order and an exclusive cursor. This
        is a page of complete metadata snapshots, not a constant-memory scan.
        """
        _limit(limit)
        if after_operation_id is not None:
            identifier(after_operation_id, "operation cursor")
        with self.store._transaction():
            identifiers = [
                row[0]
                for row in self.store._connection.execute(
                    "SELECT operation_id FROM annotation_operations "
                    "WHERE (? IS NULL OR operation_id>?) ORDER BY operation_id LIMIT ?",
                    (after_operation_id, after_operation_id, limit),
                )
            ]
            result = []
            for operation_id in identifiers:
                state = self._journal.load(operation_id)
                self._materialize(state)
                result.append(AnnotationOperation(state))
            return tuple(result)

    def history(
        self,
        operation_id: str,
        *,
        after_version: int = 0,
        limit: int = 100,
    ) -> tuple[AnnotationOperation, ...]:
        """Read a verified exclusive-cursor page of durable state transitions."""
        _revision(after_version, zero=True)
        _limit(limit)
        with self.store._transaction():
            state = self._journal.load(operation_id)
            self._materialize(state)
            rows = self.store._connection.execute(
                "SELECT body FROM annotation_operation_events WHERE operation_id=? AND version>? "
                "ORDER BY version LIMIT ?",
                (operation_id, after_version, limit),
            )
            return tuple(AnnotationOperation(_loads(row[0])) for row in rows)

    def _reserve(self, operation_id: str) -> tuple[dict[str, Any], AnnotationRequest]:
        with self.store._transaction(write=True):
            state = self._journal.load(operation_id)
            if state["status"] != "ready":
                raise AnnotationExecutionConflict("execution state changed before step reservation")
            cursor = len(state["completed"])
            if cursor == len(state["request"]["steps"]):
                raise AnnotationExecutionError("execution has no remaining step to reserve")
            event, _ = self._materialize(state)
            step = state["request"]["steps"][cursor]
            request = AnnotationRequest(
                operation_id,
                step["step_id"],
                ProcessorDescription.from_dict(step["worker"]["processor"]),
                event.get_document(step["document_id"]),
            )
            attempts = list(state["attempts"])
            if attempts[cursor] == MAX_ATTEMPTS:
                raise AnnotationExecutionError("step retry limit reached")
            attempts[cursor] += 1
            reserved = successor(
                state,
                status="reserved",
                attempts=attempts,
                reservation={
                    "step_id": step["step_id"],
                    "attempt": attempts[cursor],
                    "nonce": secrets.token_hex(32),
                },
            )
            self._journal.save(reserved, state)
        return reserved, request

    def _uncertain(self, reserved: dict[str, Any]) -> None:
        with self.store._transaction(write=True):
            actual = self._journal.load(reserved["operation_id"])
            if actual == reserved:
                self._journal.save(successor(reserved, status="uncertain", error="worker_result_unavailable"), reserved)

    def _save_result(
        self,
        reserved: dict[str, Any],
        request: AnnotationRequest,
        response: AnnotationResponse,
    ) -> None:
        output = response.apply(request).document
        body, digest = _json(output.to_dict()), output.digest
        saved = {
            "step_id": request.step_id,
            "attempt": reserved["reservation"]["attempt"],
            "input_digest": request.document.digest,
            "output_digest": digest,
            "duration_ms": response.duration_ms,
        }
        with self.store._transaction(write=True):
            if self._journal.load(request.operation_id) != reserved:
                raise AnnotationExecutionConflict("late worker result no longer owns its reservation")
            connection = self.store._connection
            previous = connection.execute("SELECT body FROM documents WHERE digest=?", (digest,)).fetchone()
            if previous is not None and previous[0] != body:
                raise AnnotationExecutionError("saved successor conflicts with an existing document digest")
            connection.execute("INSERT OR IGNORE INTO documents(digest,body) VALUES(?,?)", (digest, body))
            self._journal.save(
                successor(
                    reserved,
                    status="ready",
                    reservation=None,
                    completed=[*reserved["completed"], saved],
                ),
                reserved,
            )

    def _commit(self, operation_id: str) -> AnnotationOperation:
        # Pre-serialize all user payloads outside the final writer lock.
        with self.store._transaction():
            state = self._journal.load(operation_id)
            if state["status"] == "committed":
                self._materialize(state)
                return AnnotationOperation(state)
            if state["status"] != "ready" or len(state["completed"]) != len(state["request"]["steps"]):
                raise AnnotationExecutionConflict("execution is not ready to commit")
            event, provenance = self._materialize(state)
        prepared = self.store._prepare_append(event, provenance)
        with self.store._transaction(write=True):
            if self._journal.load(operation_id) != state:
                raise AnnotationExecutionConflict("execution state changed before final commit")
            if not self._check_source(state["request"]):
                conflict = successor(state, status="conflict", error="source_revision_changed")
                self._journal.save(conflict, state)
                result = conflict
            else:
                revision = self.store._append_in_transaction(
                    prepared,
                    expected_revision=state["request"]["expected_revision"],
                )
                result = successor(
                    state, status="committed", result={"revision": revision.revision, "digest": revision.digest}
                )
                self._journal.save(result, state)
        if result["status"] == "conflict":
            raise AnnotationExecutionConflict("source event changed; no execution event revision was published")
        return AnnotationOperation(result)

    def resume(
        self,
        operation_id: str,
        pipelines: Mapping[str, RemoteAnnotationPipeline],
        *,
        retry_uncertain: bool = False,
    ) -> AnnotationOperation:
        """Resume saved work without replay; retry uncertain calls only explicitly.

        ``retry_uncertain=True`` acknowledges that a prior worker may still be
        active or may already have performed external effects. Its late result
        cannot overwrite the new reservation. Ctrl-C/process death leaves the
        durable reservation intact; a default resume refuses to replay it.
        """
        if type(retry_uncertain) is not bool:
            raise AnnotationExecutionError("retry_uncertain must be an explicit boolean")
        with self.store._transaction():
            state = self._journal.load(operation_id)
            self._materialize(state)
            if state["status"] == "committed":
                return AnnotationOperation(state)
            if state["status"] == "conflict":
                raise AnnotationExecutionConflict(
                    "execution source conflicted; start a new operation on the latest event"
                )
            source = self.store._get(state["request"]["event_id"], state["request"]["expected_revision"])
            request, workers = self._plan(source, pipelines)
            if request != state["request"]:
                raise AnnotationExecutionConflict("configured pipeline no longer matches the bound execution")
        if state["status"] in ("reserved", "uncertain"):
            if not retry_uncertain:
                raise AnnotationExecutionUncertain("worker may have run; explicit retry is required")
            with self.store._transaction(write=True):
                if self._journal.load(operation_id) != state:
                    raise AnnotationExecutionConflict("execution changed before uncertain retry")
                self._journal.save(successor(state, status="ready", reservation=None, error=None), state)
        while True:
            state = self.get(operation_id).to_dict()
            if state["status"] == "committed":
                return AnnotationOperation(state)
            if state["status"] != "ready":
                raise AnnotationExecutionConflict("another coordinator owns the execution")
            cursor = len(state["completed"])
            if cursor == len(workers):
                return self._commit(operation_id)
            reserved, step_request = self._reserve(operation_id)
            try:
                response = workers[len(reserved["completed"])].execute(step_request)
                # Keep validation failures in the same uncertainty policy even
                # for application-provided subclasses of the transport adapter.
                response.apply(step_request)
            except Exception:
                self._uncertain(reserved)
                raise AnnotationExecutionUncertain("worker result unavailable; explicit retry is required") from None
            self._save_result(reserved, step_request, response)
