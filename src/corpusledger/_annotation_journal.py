"""Private, hash-chained execution snapshots sharing an annotation store transaction.

This is corruption detection, not authentication against an administrator able
to rewrite SQLite. No worker response, credential or raw exception is journaled.
"""

from __future__ import annotations

from typing import Any

from .annotation_protocol import ProcessorDescription, encode_wire, identifier, sha256_text
from .annotation_store import AnnotationStore, AnnotationStoreError, _digest, _json, _loads, _revision
from .annotations import _name, _object

FORMAT = "corpusledger.annotation-operation.v1"
MAX_STEPS = 128
MAX_ATTEMPTS = 1000
_KEYS = {
    "format",
    "operation_id",
    "version",
    "request",
    "request_digest",
    "status",
    "completed",
    "attempts",
    "reservation",
    "error",
    "result",
}


class AnnotationExecutionError(AnnotationStoreError):
    """An execution request or durable execution history is invalid."""


class AnnotationExecutionConflict(AnnotationExecutionError):
    """An operation ID, reservation or source event changed concurrently."""


class AnnotationExecutionUncertain(AnnotationExecutionError):
    """A worker may have run; explicit retry must acknowledge duplicate effects."""


def validate_request(value: Any) -> dict[str, Any]:
    request = dict(_object(value, {"event_id", "expected_revision", "expected_digest", "steps"}, "execution request"))
    _name(request["event_id"], "event ID")
    _revision(request["expected_revision"])
    sha256_text(request["expected_digest"], "source revision digest")
    steps = request["steps"]
    if not isinstance(steps, list) or not 1 <= len(steps) <= MAX_STEPS:
        raise AnnotationExecutionError("execution must contain between 1 and 128 steps")
    for index, item in enumerate(steps):
        step = _object(item, {"step_id", "document_id", "pipeline_id", "pipeline_version", "worker"}, "execution step")
        if step["step_id"] != f"s{index:03d}":
            raise AnnotationExecutionError("execution step IDs must follow their canonical order")
        _name(step["document_id"], "document ID")
        identifier(step["pipeline_id"], "pipeline ID")
        identifier(step["pipeline_version"], "pipeline version")
        worker = _object(step["worker"], {"processor", "endpoint_sha256"}, "worker identity")
        ProcessorDescription.from_dict(worker["processor"])
        sha256_text(worker["endpoint_sha256"], "worker endpoint digest")
    encode_wire(request)
    return request


def validate_state(value: Any) -> dict[str, Any]:
    state = dict(_object(value, _KEYS, "execution state"))
    if state["format"] != FORMAT:
        raise AnnotationExecutionError("unsupported execution journal format")
    identifier(state["operation_id"], "operation ID")
    _revision(state["version"])
    request = validate_request(state["request"])
    if state["request_digest"] != _digest(request):
        raise AnnotationExecutionError("execution request digest does not match")
    steps, done, attempts = request["steps"], state["completed"], state["attempts"]
    if not isinstance(done, list) or len(done) > len(steps):
        raise AnnotationExecutionError("invalid completed step sequence")
    if not isinstance(attempts, list) or len(attempts) != len(steps):
        raise AnnotationExecutionError("invalid execution attempt counters")
    if any(type(n) is not int or not 0 <= n <= MAX_ATTEMPTS for n in attempts):
        raise AnnotationExecutionError("execution attempts exceed the bounded retry limit")
    if any(attempts[len(done) + 1 :]):
        raise AnnotationExecutionError("future execution steps cannot have attempts")
    for index, item in enumerate(done):
        completed = _object(
            item, {"step_id", "attempt", "input_digest", "output_digest", "duration_ms"}, "completed step"
        )
        if completed["step_id"] != steps[index]["step_id"]:
            raise AnnotationExecutionError("completed step order does not match the plan")
        if type(completed["attempt"]) is not int or not 1 <= completed["attempt"] == attempts[index]:
            raise AnnotationExecutionError("completed step attempt does not match its counter")
        sha256_text(completed["input_digest"], "step input digest")
        sha256_text(completed["output_digest"], "step output digest")
        if type(completed["duration_ms"]) is not int or not 0 <= completed["duration_ms"] < 2**63:
            raise AnnotationExecutionError("invalid completed step duration")
    status, reservation, error, result = (state[key] for key in ("status", "reservation", "error", "result"))
    if status not in ("ready", "reserved", "uncertain", "committed", "conflict"):
        raise AnnotationExecutionError("unsupported execution status")
    if status in ("reserved", "uncertain"):
        reserved = _object(reservation, {"step_id", "attempt", "nonce"}, "step reservation")
        if len(done) == len(steps) or reserved["step_id"] != steps[len(done)]["step_id"]:
            raise AnnotationExecutionError("reservation does not match the next planned step")
        if type(reserved["attempt"]) is not int or not 1 <= reserved["attempt"] == attempts[len(done)]:
            raise AnnotationExecutionError("reservation attempt does not match its counter")
        sha256_text(reserved["nonce"], "reservation nonce")
    elif reservation is not None:
        raise AnnotationExecutionError("only an unresolved worker call can retain a reservation")
    expected_error = {"uncertain": "worker_result_unavailable", "conflict": "source_revision_changed"}.get(status)
    if error != expected_error:
        raise AnnotationExecutionError("execution error code does not match its state")
    if status == "committed":
        final = _object(result, {"revision", "digest"}, "execution result")
        _revision(final["revision"])
        sha256_text(final["digest"], "execution result digest")
        if len(done) != len(steps) or final["revision"] != request["expected_revision"] + 1:
            raise AnnotationExecutionError("execution result does not match its complete plan")
    elif result is not None:
        raise AnnotationExecutionError("only committed operations can contain an event result")
    encode_wire(state)
    return state


def validate_transition(previous: dict[str, Any] | None, current: dict[str, Any]) -> None:
    if previous is None:
        if current["version"] != 1 or current["status"] != "ready" or current["completed"] or any(current["attempts"]):
            raise AnnotationExecutionError("invalid initial execution state")
        return
    if (current["operation_id"], current["request"], current["request_digest"], current["version"]) != (
        previous["operation_id"],
        previous["request"],
        previous["request_digest"],
        previous["version"] + 1,
    ):
        raise AnnotationExecutionError("execution history changed its immutable request or version")
    before, after = previous["status"], current["status"]
    unchanged_done = current["completed"] == previous["completed"]
    unchanged_attempts = current["attempts"] == previous["attempts"]
    cursor = len(previous["completed"])
    if before == "ready" and after == "reserved":
        attempts = list(previous["attempts"])
        if cursor < len(attempts):
            attempts[cursor] += 1
        valid = unchanged_done and current["attempts"] == attempts
    elif before == "reserved" and after == "uncertain":
        valid = unchanged_done and unchanged_attempts and current["reservation"] == previous["reservation"]
    elif before in ("reserved", "uncertain") and after == "ready":
        # An explicit retry clears a reservation without adding a result. A
        # validated result adds exactly one entry, only from a reserved state.
        saved = current["completed"]
        valid = unchanged_attempts and (
            unchanged_done
            or (before == "reserved" and len(saved) == cursor + 1 and saved[:cursor] == previous["completed"])
        )
    elif before == "ready" and after in ("committed", "conflict"):
        valid = unchanged_done and unchanged_attempts
    else:
        valid = False
    if not valid:
        raise AnnotationExecutionError("invalid execution state transition")


class ExecutionJournal:
    """Internal transaction participant; every caller must own a store transaction."""

    def __init__(self, store: AnnotationStore) -> None:
        self.store = store

    def _check(self) -> None:
        self.store._ensure_open()
        if not self.store._connection.in_transaction:
            raise AnnotationExecutionError("execution journal requires an active transaction")
        if not self.store.execution_enabled:
            raise AnnotationExecutionError("explicitly enable the execution journal before using it")

    def load(self, operation_id: str) -> dict[str, Any]:
        self._check()
        identifier(operation_id, "operation ID")
        connection = self.store._connection
        head = connection.execute(
            "SELECT version,digest,body FROM annotation_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        rows = connection.execute(
            "SELECT version,digest,parent_digest,body FROM annotation_operation_events "
            "WHERE operation_id=? ORDER BY version",
            (operation_id,),
        )
        previous: dict[str, Any] | None = None
        parent: str | None = None
        latest = None
        for version, digest, prior, raw in rows:
            state = validate_state(_loads(raw))
            if state["operation_id"] != operation_id or type(version) is not int or state["version"] != version:
                raise AnnotationExecutionError("execution journal identity does not match its index")
            if _digest(state) != digest or prior != parent:
                raise AnnotationExecutionError("execution journal digest or ancestry does not match")
            validate_transition(previous, state)
            previous, parent, latest = state, digest, (version, digest, raw)
        if head is None and latest is None:
            raise KeyError(operation_id)
        if head != latest or previous is None:
            raise AnnotationExecutionError("execution head does not match its append-only history")
        return previous

    def save(self, state: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
        self._check()
        validate_state(state)
        validate_transition(previous, state)
        operation_id = state["operation_id"]
        try:
            actual = self.load(operation_id)
        except KeyError:
            actual = None
        if actual != previous:
            raise AnnotationExecutionConflict("execution reservation or journal version changed")
        body, digest = _json(state), _digest(state)
        parent = _digest(previous) if previous is not None else None
        connection = self.store._connection
        connection.execute(
            "INSERT INTO annotation_operation_events(operation_id,version,digest,parent_digest,body) VALUES(?,?,?,?,?)",
            (operation_id, state["version"], digest, parent, body),
        )
        if previous is None:
            connection.execute(
                "INSERT INTO annotation_operations(operation_id,version,digest,body) VALUES(?,?,?,?)",
                (operation_id, state["version"], digest, body),
            )
        else:
            changed = connection.execute(
                "UPDATE annotation_operations SET version=?,digest=?,body=? "
                "WHERE operation_id=? AND version=? AND digest=?",
                (state["version"], digest, body, operation_id, previous["version"], parent),
            ).rowcount
            if changed != 1:
                raise AnnotationExecutionConflict("execution head compare-and-swap failed")
        return state


def successor(state: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {**state, "version": state["version"] + 1, **changes}
