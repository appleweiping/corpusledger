"""Independent state-machine and corruption tests for the private execution journal."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationType, InputError
from corpusledger._annotation_journal import (
    FORMAT,
    AnnotationExecutionConflict,
    AnnotationExecutionError,
    ExecutionJournal,
    successor,
    validate_request,
    validate_state,
    validate_transition,
)
from corpusledger.annotation_protocol import ProcessorDescription
from corpusledger.annotation_store import AnnotationStore, AnnotationStoreError


def raw(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(raw(value).encode("utf-8")).hexdigest()


def request(steps: int = 2) -> dict[str, Any]:
    worker = ProcessorDescription("test.worker", "1", "a" * 64, produces=(AnnotationType("tokens"),)).to_dict()
    return {
        "event_id": "event-雪",
        "expected_revision": 1,
        "expected_digest": "b" * 64,
        "steps": [
            {
                "step_id": f"s{index:03d}",
                "document_id": f"document-{index}",
                "pipeline_id": "pipeline",
                "pipeline_version": "1",
                "worker": {"processor": copy.deepcopy(worker), "endpoint_sha256": "c" * 64},
            }
            for index in range(steps)
        ],
    }


def initial(steps: int = 2) -> dict[str, Any]:
    plan = request(steps)
    return {
        "format": FORMAT,
        "operation_id": "operation-1",
        "version": 1,
        "request": plan,
        "request_digest": digest(plan),
        "status": "ready",
        "completed": [],
        "attempts": [0] * steps,
        "reservation": None,
        "error": None,
        "result": None,
    }


def reserve(before: dict[str, Any]) -> dict[str, Any]:
    current = copy.deepcopy(before)
    index = len(before["completed"])
    current["version"] += 1
    current["attempts"][index] += 1
    current["status"] = "reserved"
    current["reservation"] = {"step_id": f"s{index:03d}", "attempt": current["attempts"][index], "nonce": "d" * 64}
    return current


def complete(before: dict[str, Any]) -> dict[str, Any]:
    current = copy.deepcopy(before)
    current["version"] += 1
    current["status"] = "ready"
    current["completed"].append(
        {
            "step_id": before["reservation"]["step_id"],
            "attempt": before["reservation"]["attempt"],
            "input_digest": "e" * 64,
            "output_digest": "f" * 64,
            "duration_ms": 17,
        }
    )
    current["reservation"] = None
    return current


def completed_chain(steps: int = 2) -> list[dict[str, Any]]:
    states = [initial(steps)]
    for _ in range(steps):
        states.append(reserve(states[-1]))
        states.append(complete(states[-1]))
    states.append(
        {
            **copy.deepcopy(states[-1]),
            "version": states[-1]["version"] + 1,
            "status": "committed",
            "result": {"revision": 2, "digest": "0" * 64},
        }
    )
    return states


def replace_path(value: dict[str, Any], path: tuple[str | int, ...], replacement: Any) -> dict[str, Any]:
    result = copy.deepcopy(value)
    target: Any = result
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = replacement
    return result


@pytest.fixture
def store(tmp_path: Path) -> Iterator[AnnotationStore]:
    with AnnotationStore(tmp_path / "journal.sqlite") as opened:
        opened.enable_execution_journal()
        yield opened


def save_chain(store: AnnotationStore, states: list[dict[str, Any]]) -> None:
    journal = ExecutionJournal(store)
    previous = None
    for state in states:
        with store._transaction(write=True):
            assert journal.save(state, previous) == state
        previous = state


@pytest.mark.parametrize("path", [(), ("steps", 0), ("steps", 0, "worker")])
@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_request_objects_are_closed(path: tuple[str | int, ...], mutation: str) -> None:
    value = request()
    target: Any = value
    for part in path:
        target = target[part]
    if mutation == "extra":
        target["unrecognized"] = "ignored?"
    else:
        target.pop(next(iter(target)))
    with pytest.raises(InputError):
        validate_request(value)


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        (("event_id",), ""),
        (("event_id",), False),
        (("expected_revision",), True),
        (("expected_revision",), 0),
        (("expected_revision",), 1.0),
        (("expected_revision",), 2**63 - 1),
        (("expected_digest",), "B" * 64),
        (("expected_digest",), "a" * 63),
        (("expected_digest",), True),
        (("steps",), []),
        (("steps",), ()),
        (("steps",), [None]),
        (("steps", 0, "step_id"), "s001"),
        (("steps", 1, "step_id"), "s000"),
        (("steps", 0, "document_id"), ""),
        (("steps", 0, "pipeline_id"), "spaces forbidden"),
        (("steps", 0, "pipeline_version"), "雪"),
        (("steps", 0, "worker", "endpoint_sha256"), "z" * 64),
        (("steps", 0, "worker", "processor", "format"), "future"),
    ],
)
def test_malformed_requests_are_rejected(path: tuple[str | int, ...], bad: Any) -> None:
    with pytest.raises(InputError):
        validate_request(replace_path(request(), path, bad))


def test_request_step_limits_and_unicode_document_identity() -> None:
    assert len(validate_request(request(128))["steps"]) == 128
    with pytest.raises(InputError):
        validate_request(request(129))
    with pytest.raises(InputError):
        validate_request(None)
    unicode_request = replace_path(request(), ("steps", 0, "document_id"), "原文😀")
    assert validate_request(unicode_request) == unicode_request


@pytest.mark.parametrize("field", sorted(initial()))
def test_state_fields_cannot_be_missing(field: str) -> None:
    value = initial()
    del value[field]
    with pytest.raises(InputError):
        validate_state(value)


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        (("format",), "future"),
        (("operation_id",), "invalid operation"),
        (("version",), True),
        (("version",), 0),
        (("version",), -1),
        (("version",), 1.0),
        (("version",), 2**63 - 1),
        (("request_digest",), "a" * 64),
        (("completed",), {}),
        (("completed",), [None, None, None]),
        (("attempts",), {}),
        (("attempts",), [0]),
        (("attempts",), [0, 0, 0]),
        (("attempts",), [False, 0]),
        (("attempts",), [-1, 0]),
        (("attempts",), [1.0, 0]),
        (("attempts",), [1001, 0]),
        (("attempts",), [0, 1]),
        (("status",), "done"),
        (("status",), []),
        (("reservation",), {}),
        (("error",), "raw exception text"),
        (("result",), {}),
    ],
)
def test_malformed_initial_state_fields(path: tuple[str | int, ...], bad: Any) -> None:
    with pytest.raises(InputError):
        validate_state(replace_path(initial(), path, bad))


def test_state_extra_fields_and_request_digest_are_not_ignored() -> None:
    with pytest.raises(InputError):
        validate_state(initial() | {"credential": "do not retain"})
    changed = initial()
    changed["request"]["steps"][0]["document_id"] = "different"
    with pytest.raises(AnnotationExecutionError, match="request digest"):
        validate_state(changed)
    changed["request_digest"] = digest(changed["request"])
    assert validate_state(changed) == changed


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        (("reservation",), None),
        (("reservation", "step_id"), "s001"),
        (("reservation", "attempt"), True),
        (("reservation", "attempt"), 0),
        (("reservation", "attempt"), 2),
        (("reservation", "nonce"), "A" * 64),
        (("reservation", "nonce"), None),
        (("attempts",), [1001, 0]),
        (("error",), "worker_result_unavailable"),
    ],
)
def test_reservations_require_exact_next_step_counter_and_identity(path: tuple[str | int, ...], bad: Any) -> None:
    with pytest.raises(InputError):
        validate_state(replace_path(reserve(initial()), path, bad))


def test_reserved_and_uncertain_error_contracts_and_retry_bound() -> None:
    state = reserve(initial())
    state["attempts"][0] = state["reservation"]["attempt"] = 1000
    assert validate_state(state) == state
    state["status"] = "uncertain"
    with pytest.raises(AnnotationExecutionError, match="error code"):
        validate_state(state)
    state["error"] = "worker_result_unavailable"
    assert validate_state(state) == state
    state["reservation"]["extra"] = "forbidden"
    with pytest.raises(InputError):
        validate_state(state)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("step_id", "s001"),
        ("attempt", True),
        ("attempt", 0),
        ("attempt", 2),
        ("input_digest", "not a digest"),
        ("output_digest", "F" * 64),
        ("duration_ms", True),
        ("duration_ms", -1),
        ("duration_ms", 1.0),
        ("duration_ms", 2**63),
    ],
)
def test_completed_steps_validate_timing_digest_and_order(field: str, bad: Any) -> None:
    state = complete(reserve(initial()))
    state["completed"][0][field] = bad
    with pytest.raises(InputError):
        validate_state(state)


@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate", "reservation_after_completion"])
def test_completed_steps_are_closed_ordered_and_cannot_be_reserved_again(mutation: str) -> None:
    state = completed_chain(1)[-2]
    if mutation == "missing":
        del state["completed"][0]["duration_ms"]
    elif mutation == "extra":
        state["completed"][0]["raw_response"] = "forbidden"
    elif mutation == "duplicate":
        state["completed"].append(copy.deepcopy(state["completed"][0]))
    else:
        state["status"] = "reserved"
        state["reservation"] = {"step_id": "s000", "attempt": 1, "nonce": "a" * 64}
    with pytest.raises(InputError):
        validate_state(state)


@pytest.mark.parametrize(
    ("path", "bad"),
    [
        (("result",), None),
        (("result",), {"revision": 2, "digest": "0" * 64, "extra": 1}),
        (("result", "revision"), True),
        (("result", "revision"), 1),
        (("result", "revision"), 3),
        (("result", "digest"), "A" * 64),
        (("completed",), []),
        (("error",), "source_revision_changed"),
    ],
)
def test_committed_result_matches_complete_plan(path: tuple[str | int, ...], bad: Any) -> None:
    with pytest.raises(InputError):
        validate_state(replace_path(completed_chain()[-1], path, bad))


def test_success_retry_and_conflict_transitions() -> None:
    chain = completed_chain()
    previous = None
    for state in chain:
        assert validate_state(state) == state
        validate_transition(previous, state)
        previous = state
    reserved = reserve(initial())
    uncertain = successor(reserved, status="uncertain", error="worker_result_unavailable")
    retry = successor(uncertain, status="ready", reservation=None, error=None)
    again = reserve(retry)
    done = complete(again)
    for before, after in ((reserved, uncertain), (uncertain, retry), (retry, again), (again, done)):
        validate_state(after)
        validate_transition(before, after)
    assert done["completed"][0]["attempt"] == 2
    reset_reserved = successor(reserved, status="ready", reservation=None)
    validate_state(reset_reserved)
    validate_transition(reserved, reset_reserved)
    for ready in (initial(), chain[2], chain[-2]):
        conflict = successor(ready, status="conflict", error="source_revision_changed")
        validate_state(conflict)
        validate_transition(ready, conflict)


@pytest.mark.parametrize("mode", ["version", "reserved", "completed", "attempted"])
def test_initial_transition_requires_pristine_ready_state(mode: str) -> None:
    state = initial()
    if mode == "version":
        state["version"] = 2
    elif mode == "reserved":
        state = reserve(state)
        state["version"] = 1
    elif mode == "completed":
        state = complete(reserve(state))
        state["version"] = 1
    else:
        state["attempts"][0] = 1
    validate_state(state)
    with pytest.raises(AnnotationExecutionError, match="initial"):
        validate_transition(None, state)


@pytest.mark.parametrize(
    "mode",
    [
        "operation",
        "request",
        "version",
        "ready_ready",
        "missing_attempt_increment",
        "uncertain_nonce",
        "uncertain_completed",
        "changed_completed",
        "attempt_decrease",
        "terminal",
    ],
)
def test_invalid_transitions_are_rejected(mode: str) -> None:
    before, after = initial(), reserve(initial())
    if mode == "operation":
        after["operation_id"] = "different"
    elif mode == "request":
        after["request"]["event_id"] = "different"
        after["request_digest"] = digest(after["request"])
    elif mode == "version":
        after["version"] = 3
    elif mode == "ready_ready":
        after = successor(before)
    elif mode == "missing_attempt_increment":
        before["attempts"][0] = 1
    elif mode == "uncertain_nonce":
        before = reserve(initial())
        after = successor(copy.deepcopy(before), status="uncertain", error="worker_result_unavailable")
        after["reservation"]["nonce"] = "f" * 64
    elif mode == "uncertain_completed":
        before = successor(reserve(initial()), status="uncertain", error="worker_result_unavailable")
        after = complete(before)
        after["error"] = None
    elif mode == "changed_completed":
        before = complete(reserve(initial()))
        after = reserve(before)
        after["completed"][0]["duration_ms"] = 18
    elif mode == "attempt_decrease":
        before = reserve(initial())
        after = successor(before, status="ready", reservation=None, attempts=[0, 0])
    else:
        before = completed_chain()[-1]
        after = successor(before)
    validate_state(before)
    validate_state(after)
    with pytest.raises(AnnotationExecutionError):
        validate_transition(before, after)


def test_explicit_migration_and_caller_transaction_are_required(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "v1.sqlite") as store:
        journal = ExecutionJournal(store)
        for callback in (lambda: journal.load("operation-1"), lambda: journal.save(initial(), None)):
            with pytest.raises(AnnotationExecutionError, match="active transaction"):
                callback()
            with pytest.raises(AnnotationExecutionError, match="explicitly enable"), store._transaction(write=True):
                callback()
        assert not store.execution_enabled
        store.enable_execution_journal()
        with store._transaction():
            with pytest.raises(KeyError):
                journal.load("operation-1")
            with pytest.raises(InputError):
                journal.load("bad identifier")
    with pytest.raises(AnnotationStoreError, match="closed"):
        journal.load("operation-1")


def test_save_load_roundtrip_independent_hash_chain_and_input_snapshot(store: AnnotationStore) -> None:
    states = completed_chain()
    save_chain(store, states)
    journal = ExecutionJournal(store)
    with store._transaction():
        assert journal.load("operation-1") == states[-1]
        rows = store._connection.execute(
            "SELECT operation_id,version,digest,parent_digest,body FROM annotation_operation_events ORDER BY version"
        ).fetchall()
        assert rows == [
            (
                "operation-1",
                state["version"],
                digest(state),
                None if index == 0 else digest(states[index - 1]),
                raw(state),
            )
            for index, state in enumerate(states)
        ]
    states[-1]["completed"][0]["duration_ms"] = 999
    with store._transaction():
        loaded = journal.load("operation-1")
        assert loaded["completed"][0]["duration_ms"] == 17
        loaded["request"]["event_id"] = "caller mutation"
        assert journal.load("operation-1")["request"]["event_id"] == "event-雪"


@pytest.mark.parametrize("failure", ["application", "head_insert", "head_update", "cas_ignore"])
def test_save_failures_leave_no_half_committed_history(store: AnnotationStore, failure: str) -> None:
    journal = ExecutionJournal(store)
    previous = initial() if failure in {"head_update", "cas_ignore"} else None
    if previous is not None:
        save_chain(store, [previous])
    next_state = reserve(previous) if previous is not None else initial()
    before_events = store._connection.execute("SELECT * FROM annotation_operation_events").fetchall()
    before_heads = store._connection.execute("SELECT * FROM annotation_operations").fetchall()
    if failure != "application":
        action = "INSERT" if failure == "head_insert" else "UPDATE"
        fail_sql = "SELECT RAISE(IGNORE)" if failure == "cas_ignore" else "SELECT RAISE(ABORT,'injected')"
        store._connection.execute(
            f"CREATE TRIGGER fail_head BEFORE {action} ON annotation_operations BEGIN {fail_sql}; END"
        )
    expected = RuntimeError if failure == "application" else AnnotationStoreError
    with pytest.raises(expected), store._transaction(write=True):
        journal.save(next_state, previous)
        if failure == "application":
            raise RuntimeError("abort after participant save")
    assert store._connection.execute("SELECT * FROM annotation_operation_events").fetchall() == before_events
    assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == before_heads
    assert not store._connection.in_transaction


def test_stale_compare_and_swap_and_create_only_collision(store: AnnotationStore) -> None:
    journal = ExecutionJournal(store)
    start = initial()
    reserved = reserve(start)
    save_chain(store, [start, reserved])
    for state, previous in ((start, None), (reserve(start), start)):
        with pytest.raises(AnnotationExecutionConflict), store._transaction(write=True):
            journal.save(state, previous)
    with store._transaction():
        assert journal.load("operation-1") == reserved
        assert store._connection.execute("SELECT COUNT(*) FROM annotation_operation_events").fetchone()[0] == 2


def test_independent_connections_have_one_reservation_winner(store: AnnotationStore) -> None:
    save_chain(store, [initial()])
    barrier = threading.Barrier(2)

    def compete(nonce: str) -> str:
        with AnnotationStore(store.path, create=False) as contender:
            journal = ExecutionJournal(contender)
            with contender._transaction():
                observed = journal.load("operation-1")
            proposal = reserve(observed)
            proposal["reservation"]["nonce"] = nonce
            barrier.wait(timeout=10)
            try:
                with contender._transaction(write=True):
                    journal.save(proposal, observed)
                return "saved"
            except AnnotationExecutionConflict:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(compete, nonce) for nonce in ("a" * 64, "b" * 64)]
        assert sorted(future.result(timeout=20) for future in futures) == ["conflict", "saved"]
    with store._transaction():
        loaded = ExecutionJournal(store).load("operation-1")
        assert loaded["attempts"] == [1, 0]
        assert loaded["version"] == 2


def rewrite_event(connection: sqlite3.Connection, version: int, update: dict[str, Any]) -> None:
    value = json.loads(
        connection.execute("SELECT body FROM annotation_operation_events WHERE version=?", (version,)).fetchone()[0]
    )
    value.update(update)
    rendered, checksum = raw(value), digest(value)
    connection.execute(
        "UPDATE annotation_operation_events SET body=?,digest=? WHERE version=?", (rendered, checksum, version)
    )
    connection.execute(
        "UPDATE annotation_operations SET body=?,digest=? WHERE version=?", (rendered, checksum, version)
    )


@pytest.mark.parametrize(
    "mode",
    [
        "body",
        "bool_version",
        "wrong_identity",
        "index_version",
        "digest",
        "parent",
        "gap",
        "initial_attempts",
        "transition",
        "immutable_request",
        "head_missing",
        "events_missing",
        "head_version",
        "head_digest",
        "head_body",
    ],
)
def test_corrupt_history_or_head_is_detected_even_when_state_hash_is_recomputed(
    store: AnnotationStore, mode: str
) -> None:
    states = [initial(), reserve(initial())]
    save_chain(store, states)
    connection = store._connection
    with store._transaction(write=True):
        # Corruption tests deliberately bypass guards as a DB administrator,
        # then restore them exactly so the journal rather than schema check runs.
        guards = connection.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name").fetchall()
        for name, _statement in guards:
            assert name in {"annotation_operation_events_no_update", "annotation_operation_events_no_delete"}
            connection.execute(f"DROP TRIGGER {name}")
        if mode == "body":
            connection.execute("UPDATE annotation_operation_events SET body='{}' WHERE version=2")
        elif mode == "bool_version":
            rewrite_event(connection, 2, {"version": True})
        elif mode == "wrong_identity":
            rewrite_event(connection, 2, {"operation_id": "other"})
        elif mode == "index_version":
            connection.execute("UPDATE annotation_operation_events SET version=3 WHERE version=2")
        elif mode == "digest":
            connection.execute("UPDATE annotation_operation_events SET digest=? WHERE version=2", ("0" * 64,))
        elif mode == "parent":
            connection.execute("UPDATE annotation_operation_events SET parent_digest=? WHERE version=2", ("0" * 64,))
        elif mode == "gap":
            connection.execute("DELETE FROM annotation_operation_events WHERE version=1")
        elif mode == "initial_attempts":
            rewrite_event(connection, 1, {"attempts": [1, 0]})
        elif mode == "transition":
            rewrite_event(connection, 2, {"status": "ready", "reservation": None, "attempts": [0, 0]})
        elif mode == "immutable_request":
            changed = copy.deepcopy(states[1]["request"])
            changed["event_id"] = "another"
            rewrite_event(connection, 2, {"request": changed, "request_digest": digest(changed)})
        elif mode == "head_missing":
            connection.execute("DELETE FROM annotation_operations")
        elif mode == "events_missing":
            connection.execute("DELETE FROM annotation_operation_events")
        elif mode == "head_version":
            connection.execute("UPDATE annotation_operations SET version=1")
        elif mode == "head_digest":
            connection.execute("UPDATE annotation_operations SET digest=?", ("0" * 64,))
        else:
            connection.execute("UPDATE annotation_operations SET body=?", (json.dumps(states[1], indent=2),))
        for _name, statement in guards:
            connection.execute(statement)
    journal = ExecutionJournal(store)
    with pytest.raises(InputError), store._transaction():
        journal.load("operation-1")
    # Writing must validate existing history too, not cover up the corruption.
    with pytest.raises(InputError), store._transaction(write=True):
        journal.save(complete(states[1]), states[1])


@pytest.mark.parametrize("body", ["NaN", '{"x":1,"x":2}', "[" * 1200])
def test_malformed_stored_json_is_not_trusted(store: AnnotationStore, body: str) -> None:
    # Insert-only corruption fixture, so append-only guards remain in place.
    with store._transaction(write=True):
        store._connection.execute(
            "INSERT INTO annotation_operation_events VALUES('operation-1',1,?,NULL,?)", ("a" * 64, body)
        )
        store._connection.execute("INSERT INTO annotation_operations VALUES('operation-1',1,?,?)", ("a" * 64, body))
    with pytest.raises(InputError), store._transaction():
        ExecutionJournal(store).load("operation-1")


def test_successor_increments_version_without_mutating_input() -> None:
    state = initial()
    changed = successor(state, status="conflict", error="source_revision_changed")
    assert state == initial()
    assert changed["version"] == 2
    assert changed["status"] == "conflict"
