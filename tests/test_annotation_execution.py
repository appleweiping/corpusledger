"""Restart, race and atomic-publication tests with independently controlled workers."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationDocument, AnnotationField, AnnotationType, InputError, SpanAnnotation
from corpusledger._annotation_journal import AnnotationExecutionConflict, AnnotationExecutionUncertain, ExecutionJournal
from corpusledger.annotation_execution import (
    AnnotationExecutionError,
    AnnotationExecutor,
    AnnotationOperation,
    RemoteAnnotationPipeline,
)
from corpusledger.annotation_pipeline import AnnotationPipelineError
from corpusledger.annotation_protocol import AnnotationRequest, AnnotationResponse, ProcessorDescription
from corpusledger.annotation_remote import RemoteAnnotationError, RemoteAnnotationProcessor
from corpusledger.annotation_store import AnnotationEvent, AnnotationRevision, AnnotationStore, AnnotationStoreError

TOKEN = AnnotationType("token", {"text": AnnotationField(), "position": AnnotationField("integer")})
GROUP = AnnotationType(
    "group", {"members": AnnotationField("references", target_type="token"), "count": AnnotationField("integer")}
)
NOTE = AnnotationType("note", {"label": AnnotationField(), "payload": AnnotationField("object")})


def workers() -> tuple[RemoteAnnotationProcessor, RemoteAnnotationProcessor]:
    tokenizer = ProcessorDescription("tokenizer", "1", "a" * 64, produces=(TOKEN,))
    grouper = ProcessorDescription("grouper", "1", "b" * 64, requires=(TOKEN,), produces=(GROUP,))
    return (
        RemoteAnnotationProcessor("http://127.0.0.1:43121", tokenizer, "TEST_TOKENIZER_TOKEN"),
        RemoteAnnotationProcessor("http://127.0.0.1:43122", grouper, "TEST_GROUPER_TOKEN"),
    )


def pipelines(*, two_steps: bool = False) -> dict[str, RemoteAnnotationPipeline]:
    token, group = workers()
    # Intentionally reversed input: dependency planning must reorder to token→group.
    return {"a-original": RemoteAnnotationPipeline("linguistic", "1", (group, token) if two_steps else (token,))}


def source_event() -> AnnotationEvent:
    doc = AnnotationDocument(
        "a-original",
        "😀x\r\n beta",
        (NOTE,),
        (SpanAnnotation("note-1", "note", 0, 2, {"label": "keep", "payload": {"flag": True}}),),
    )
    sibling = AnnotationDocument("b-related", "雪\t雨")
    untouched = AnnotationDocument("z-untouched", "do not process")
    return AnnotationEvent(
        "conversation", (untouched, sibling, doc), {"labels": ["原文", "reviewed"], "flags": {"reviewed": True}}
    )


def answer(incoming: AnnotationRequest, *, duration: int = 7) -> AnnotationResponse:
    if incoming.processor.name == "tokenizer":
        annotations = tuple(
            SpanAnnotation(
                f"token-{index}", "token", match.start(), match.end(), {"text": match.group(), "position": index}
            )
            for index, match in enumerate(re.finditer(r"\S+", incoming.document.text))
        )
    else:
        members = [item.annotation_id for item in incoming.document.annotations if item.type_name == "token"]
        annotations = (
            SpanAnnotation(
                "group-1", "group", 0, len(incoming.document.text), {"members": members, "count": len(members)}
            ),
        )
    return AnnotationResponse(
        incoming.operation_id, incoming.step_id, incoming.processor, incoming.document.digest, annotations, duration
    )


@pytest.fixture
def store(tmp_path: Path) -> Iterator[AnnotationStore]:
    with AnnotationStore(tmp_path / "execution.sqlite") as opened:
        opened.put(source_event())
        opened.enable_execution_journal()
        yield opened


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch, store: AnnotationStore) -> dict[str, list[Any]]:
    records: dict[str, list[Any]] = {"verify": [], "execute": []}

    def verify(worker: RemoteAnnotationProcessor) -> None:
        assert not store._connection.in_transaction
        records["verify"].append(worker.identity)

    def execute(worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        assert not store._connection.in_transaction
        assert incoming.processor == worker.description
        # Prove another connection can acquire the writer lock during callbacks.
        with sqlite3.connect(store.path, timeout=0) as other:
            other.execute("BEGIN IMMEDIATE")
            other.rollback()
        records["execute"].append(incoming)
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", verify)
    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", execute)
    return records


def begin(
    executor: AnnotationExecutor, planned: dict[str, RemoteAnnotationPipeline] | None = None, *, key: str = "work"
) -> AnnotationOperation:
    source = executor.store.get("conversation", 1)
    return executor.begin(
        key,
        source.event_id,
        planned if planned is not None else pipelines(),
        expected_revision=1,
        expected_digest=source.digest,
    )


def test_multidocument_dag_order_preserves_siblings_layers_and_atomic_provenance(
    store: AnnotationStore, calls: dict[str, list[Any]]
) -> None:
    executor = AnnotationExecutor(store)
    selected = pipelines(two_steps=True)
    selected["b-related"] = selected["a-original"]
    original = store.get("conversation")
    queued = begin(executor, selected)
    assert queued.status == "ready" and queued.version == 1 and queued.completed_steps == 0
    assert len(calls["verify"]) == 4 and not calls["execute"]
    result = executor.resume("work", selected)
    assert result.status == "committed" and result.version == 10 and result.completed_steps == 4
    assert [entry.processor.name for entry in calls["execute"]] == ["tokenizer", "grouper", "tokenizer", "grouper"]
    assert [entry.step_id for entry in calls["execute"]] == ["s000", "s001", "s002", "s003"]
    assert [entry.document.document_id for entry in calls["execute"]] == [
        "a-original",
        "a-original",
        "b-related",
        "b-related",
    ]
    final = store.get("conversation")
    assert final.revision == 2 and final.parent_digest == original.digest
    assert final.event.metadata == original.event.metadata
    assert final.event.get_document("z-untouched") == original.event.get_document("z-untouched")
    doc = final.event.get_document("a-original")
    assert doc.get("note-1") == original.event.get_document("a-original").get("note-1")
    assert doc.text == "😀x\r\n beta"
    assert [doc.span_text(name) for name in ("token-0", "token-1")] == ["😀x", "beta"]
    assert doc.get("group-1").features["members"] == ("token-0", "token-1")
    evidence = final.to_dict()["provenance"]["annotation_execution"]
    assert evidence["operation_id"] == "work"
    assert evidence["request_digest"] == result.to_dict()["request_digest"]
    assert evidence["completed"] == result.to_dict()["completed"]
    assert result.to_dict()["result"] == {"revision": 2, "digest": final.digest}
    assert executor.get("work") == result
    assert store.get("conversation", 1) == original
    assert store.verify().revisions == 2


def test_operation_snapshot_is_immutable_and_contains_no_text_credentials_or_raw_errors(
    store: AnnotationStore, calls: dict[str, list[Any]]
) -> None:
    operation = begin(AnnotationExecutor(store))
    view = operation.to_dict()
    view["request"]["steps"][0]["document_id"] = "changed"
    view["attempts"][0] = 91
    assert operation.to_dict()["request"]["steps"][0]["document_id"] == "a-original"
    assert operation.to_dict()["attempts"] == [0]
    with pytest.raises(TypeError):
        operation._state["attempts"][0] = 2
    rendered = json.dumps(operation.to_dict(), ensure_ascii=False)
    assert "😀x" not in rendered and "TEST_TOKENIZER_TOKEN" not in rendered
    assert "http://" not in rendered and "Authorization" not in rendered


@pytest.mark.parametrize("change", ["empty", "bool_version", "unknown_status", "extra"])
def test_public_operation_constructor_validates_its_snapshot(
    store: AnnotationStore, calls: dict[str, list[Any]], change: str
) -> None:
    value = begin(AnnotationExecutor(store)).to_dict()
    if change == "empty":
        value = {}
    elif change == "bool_version":
        value["version"] = True
    elif change == "unknown_status":
        value["status"] = "made_up"
    else:
        value["private"] = "extra field"
    with pytest.raises(InputError):
        AnnotationOperation(value)


def test_public_operation_version_one_must_be_a_pristine_initial_state(
    store: AnnotationStore, calls: dict[str, list[Any]]
) -> None:
    value = begin(AnnotationExecutor(store)).to_dict()
    value["status"] = "reserved"
    value["attempts"] = [1]
    value["reservation"] = {"step_id": "s000", "attempt": 1, "nonce": "a" * 64}
    with pytest.raises(AnnotationExecutionError, match="initial execution state"):
        AnnotationOperation(value)


def test_same_request_key_reuses_ready_and_committed_state_without_worker_replay(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    queued = begin(executor)
    assert begin(executor) == queued
    assert len(calls["verify"]) == 1
    final = executor.resume("work", pipelines())
    assert len(calls["execute"]) == 1

    def forbidden(*_args: Any) -> None:
        raise AssertionError("idempotent request unexpectedly contacted worker")

    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", forbidden)
    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", forbidden)
    assert begin(executor) == final
    assert executor.resume("work", pipelines()) == final
    assert store.get("conversation").revision == 2


@pytest.mark.parametrize("change", ["pipeline_version", "pipeline_id", "endpoint", "worker_version", "config"])
def test_bound_request_rejects_configuration_drift_before_callbacks(
    store: AnnotationStore, calls: dict[str, list[Any]], change: str
) -> None:
    executor = AnnotationExecutor(store)
    initial = begin(executor)
    selected = pipelines()
    pipeline = selected["a-original"]
    worker = pipeline.processors[0]
    if change == "pipeline_version":
        pipeline = replace(pipeline, version="2")
    elif change == "pipeline_id":
        pipeline = replace(pipeline, pipeline_id="different")
    else:
        if change == "endpoint":
            worker = replace(worker, endpoint="http://127.0.0.1:43123")
        elif change == "worker_version":
            worker = replace(worker, description=replace(worker.description, version="2"))
        else:
            worker = replace(worker, description=replace(worker.description, config_sha256="e" * 64))
        pipeline = replace(pipeline, processors=(worker,))
    selected["a-original"] = pipeline
    for attempt in (lambda: begin(executor, selected), lambda: executor.resume("work", selected)):
        with pytest.raises(AnnotationExecutionConflict):
            attempt()
    assert executor.get("work") == initial
    assert len(calls["verify"]) == 1 and not calls["execute"]


def test_credential_and_transport_budget_rotation_do_not_change_bound_identity(
    store: AnnotationStore, calls: dict[str, list[Any]]
) -> None:
    executor = AnnotationExecutor(store)
    initial = begin(executor)
    worker = replace(workers()[0], token_env="ROTATED_SECRET_SOURCE", timeout=2.0, max_response_bytes=4096)
    rotated = {"a-original": RemoteAnnotationPipeline("linguistic", "1", (worker,))}
    assert begin(executor, rotated) == initial
    assert executor.resume("work", rotated).status == "committed"
    assert len(calls["verify"]) == 1 and len(calls["execute"]) == 1


def test_explicit_upgrade_and_argument_validation_do_not_create_operations(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "v1") as store:
        with pytest.raises(AnnotationExecutionError, match="explicitly enable"):
            AnnotationExecutor(store)
        assert not store.execution_enabled
    with pytest.raises(AnnotationExecutionError):
        AnnotationExecutor(None)  # type: ignore[arg-type]
    token = workers()[0]
    for args in (
        ("bad id", "1", (token,)),
        ("p", "", (token,)),
        ("p", "1", ()),
        ("p", "1", (1,)),
        ("p", "1", (token, token)),
    ):
        with pytest.raises((InputError, AnnotationPipelineError)):
            RemoteAnnotationPipeline(*args)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [True, 0, -1, 1.0, "1", 2**63 - 1])
def test_begin_rejects_malformed_revision(store: AnnotationStore, calls: dict[str, list[Any]], bad: Any) -> None:
    source = store.get("conversation")
    with pytest.raises(InputError):
        AnnotationExecutor(store).begin(
            "work", "conversation", pipelines(), expected_revision=bad, expected_digest=source.digest
        )
    assert not calls["verify"] and not calls["execute"]
    assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == []


@pytest.mark.parametrize("bad", [False, "", "A" * 64, "0" * 63])
def test_begin_rejects_malformed_source_digest(store: AnnotationStore, calls: dict[str, list[Any]], bad: Any) -> None:
    with pytest.raises(InputError):
        AnnotationExecutor(store).begin("work", "conversation", pipelines(), expected_revision=1, expected_digest=bad)
    assert not calls["verify"]


@pytest.mark.parametrize("mode", ["missing_document", "missing_dependency", "bad_mapping", "bad_value", "wrong_digest"])
def test_all_selected_dags_are_preflighted_before_any_verification(
    store: AnnotationStore, calls: dict[str, list[Any]], mode: str
) -> None:
    executor = AnnotationExecutor(store)
    selected: Any = pipelines()
    expected: Any = InputError
    if mode == "missing_document":
        selected["missing"] = selected["a-original"]
        expected = KeyError
    elif mode == "missing_dependency":
        selected["b-related"] = RemoteAnnotationPipeline("invalid", "1", (workers()[1],))
        expected = AnnotationPipelineError
    elif mode == "bad_mapping":
        selected = []
    elif mode == "bad_value":
        selected["b-related"] = "not a pipeline"
    with pytest.raises(expected):
        if mode == "wrong_digest":
            executor.begin("work", "conversation", selected, expected_revision=1, expected_digest="f" * 64)
        else:
            begin(executor, selected)
    assert not calls["verify"] and not calls["execute"]
    assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == []


def test_failed_live_identity_preflight_publishes_no_journal(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    def unavailable(_worker: RemoteAnnotationProcessor) -> None:
        raise RemoteAnnotationError("private worker failure")

    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", unavailable)
    with pytest.raises(RemoteAnnotationError):
        begin(AnnotationExecutor(store))
    assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == []
    assert not calls["execute"]


def test_source_write_during_preflight_wins_without_execution(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    def concurrent_update(_worker: RemoteAnnotationProcessor) -> None:
        with AnnotationStore(store.path, create=False) as other:
            other.put(AnnotationEvent("conversation", (AnnotationDocument("winner", "external"),)), expected_revision=1)

    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", concurrent_update)
    with pytest.raises(AnnotationExecutionConflict, match="during worker preflight"):
        begin(AnnotationExecutor(store))
    assert not calls["execute"]
    assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == []
    assert store.get("conversation").event.get_document("winner").text == "external"


@pytest.mark.parametrize("failure", ["exception", "keyboard_interrupt", "system_exit"])
def test_worker_failure_restart_does_not_replay_without_explicit_acknowledgement(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    invocations = []
    exception = {"exception": RuntimeError, "keyboard_interrupt": KeyboardInterrupt, "system_exit": SystemExit}[failure]

    def broken(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        invocations.append(incoming.step_id)
        raise exception("raw credentials must not be journaled")

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", broken)
    expected = AnnotationExecutionUncertain if failure == "exception" else exception
    with pytest.raises(expected):
        executor.resume("work", pipelines())
    state = executor.get("work")
    assert state.status == ("uncertain" if failure == "exception" else "reserved")
    assert state.version == (3 if failure == "exception" else 2)
    assert state.completed_steps == 0 and store.get("conversation").revision == 1
    assert "credentials" not in json.dumps(state.to_dict())
    with AnnotationStore(store.path, create=False) as reopened:
        restarted = AnnotationExecutor(reopened)
        with pytest.raises(AnnotationExecutionUncertain, match="explicit retry"):
            restarted.resume("work", pipelines())
        assert invocations == ["s000"]

        def succeeds(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
            invocations.append(incoming.step_id)
            return answer(incoming)

        monkeypatch.setattr(RemoteAnnotationProcessor, "execute", succeeds)
        final = restarted.resume("work", pipelines(), retry_uncertain=True)
        assert final.status == "committed" and final.to_dict()["attempts"] == [2]
        assert final.to_dict()["completed"][0]["attempt"] == 2
    assert invocations == ["s000", "s000"]
    assert store.get("conversation").revision == 2


@pytest.mark.parametrize("bad", [0, 1, None, "yes"])
def test_retry_acknowledgement_must_be_boolean(store: AnnotationStore, calls: dict[str, list[Any]], bad: Any) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    with pytest.raises(AnnotationExecutionError, match="explicit boolean"):
        executor.resume("work", pipelines(), retry_uncertain=bad)
    assert not calls["execute"]


def test_retry_limit_does_not_dispatch_an_extra_worker_call(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    invocations = []

    def failed(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        invocations.append(incoming.step_id)
        raise RuntimeError("unavailable")

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", failed)
    monkeypatch.setattr("corpusledger.annotation_execution.MAX_ATTEMPTS", 1)
    with pytest.raises(AnnotationExecutionUncertain):
        executor.resume("work", pipelines())
    with pytest.raises(AnnotationExecutionError, match="retry limit reached"):
        executor.resume("work", pipelines(), retry_uncertain=True)
    with pytest.raises(AnnotationExecutionError, match="retry limit reached"):
        executor.resume("work", pipelines())
    assert invocations == ["s000"]
    assert executor.get("work").to_dict()["attempts"] == [1]
    assert store.get("conversation").revision == 1


def test_completed_step_survives_restart_without_replay(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    selected = pipelines(two_steps=True)
    begin(executor, selected)
    invoked = []

    def interrupt_second(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        invoked.append(incoming.processor.name)
        if incoming.processor.name == "grouper":
            raise KeyboardInterrupt
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", interrupt_second)
    with pytest.raises(KeyboardInterrupt):
        executor.resume("work", selected)
    state = executor.get("work")
    assert state.status == "reserved" and state.completed_steps == 1
    assert state.to_dict()["attempts"] == [1, 1]
    assert store.get("conversation").revision == 1

    def finish(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        invoked.append(incoming.processor.name)
        assert incoming.processor.name == "grouper"
        assert incoming.document.get("token-0").features["text"] == "😀x"
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", finish)
    with AnnotationStore(store.path, create=False) as reopened:
        final = AnnotationExecutor(reopened).resume("work", selected, retry_uncertain=True)
        assert final.status == "committed" and final.to_dict()["attempts"] == [1, 2]
    assert invoked == ["tokenizer", "grouper", "grouper"]


def test_explicit_retry_replaces_nonce_and_rejects_late_original_result(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    invoked = []
    nonces = []

    def race(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        invoked.append(incoming.step_id)
        with AnnotationStore(store.path, create=False) as other:
            retrying = AnnotationExecutor(other)
            nonces.append(retrying.get("work").to_dict()["reservation"]["nonce"])
            if len(invoked) == 1:
                retrying.resume("work", pipelines(), retry_uncertain=True)
                return answer(incoming, duration=999)
        return answer(incoming, duration=11)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", race)
    with pytest.raises(AnnotationExecutionConflict, match="late worker result"):
        executor.resume("work", pipelines())
    final = executor.get("work")
    assert final.status == "committed"
    assert final.to_dict()["attempts"] == [2]
    assert final.to_dict()["completed"][0]["duration_ms"] == 11
    assert len(nonces) == 2 and nonces[0] != nonces[1]
    assert store.get("conversation").revision == 2


@pytest.mark.parametrize("failure", ["step_state", "event_insert", "committed_state"])
def test_save_and_final_publication_failure_roll_back_the_whole_participant_transaction(
    store: AnnotationStore, calls: dict[str, list[Any]], failure: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    initial_documents = store._connection.execute("SELECT * FROM documents ORDER BY digest").fetchall()
    if failure == "step_state":
        statement = (
            "CREATE TRIGGER injected BEFORE INSERT ON annotation_operation_events WHEN NEW.version=3 "
            "BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    elif failure == "event_insert":
        statement = (
            "CREATE TRIGGER injected BEFORE INSERT ON revisions WHEN NEW.revision=2 "
            "BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    else:
        statement = (
            "CREATE TRIGGER injected BEFORE INSERT ON annotation_operation_events WHEN NEW.version=4 "
            "BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    store._connection.execute(statement)
    with pytest.raises(AnnotationStoreError, match="transaction failed"):
        executor.resume("work", pipelines())
    state = executor.get("work")
    assert store.get("conversation").revision == 1
    assert state.status == ("reserved" if failure == "step_state" else "ready")
    assert state.version == (2 if failure == "step_state" else 3)
    if failure == "step_state":
        assert store._connection.execute("SELECT * FROM documents ORDER BY digest").fetchall() == initial_documents
        assert state.completed_steps == 0
    else:
        assert state.completed_steps == 1
        assert store._connection.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == 1
    store._connection.execute("DROP TRIGGER injected")
    if failure == "step_state":
        with pytest.raises(AnnotationExecutionUncertain):
            executor.resume("work", pipelines())
    final = executor.resume("work", pipelines(), retry_uncertain=failure == "step_state")
    assert final.status == "committed"
    assert len(calls["execute"]) == (2 if failure == "step_state" else 1)
    assert store.get("conversation").revision == 2


def test_concurrent_source_event_write_is_not_overwritten_by_final_commit(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    winner: list[AnnotationRevision] = []

    def competing_event(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        with AnnotationStore(store.path, create=False) as other:
            winner.append(
                other.put(
                    AnnotationEvent("conversation", (AnnotationDocument("external", "winner"),)), expected_revision=1
                )
            )
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", competing_event)
    with pytest.raises(AnnotationExecutionConflict, match="no execution event revision"):
        executor.resume("work", pipelines())
    state = executor.get("work")
    assert state.status == "conflict" and state.completed_steps == 1
    assert state.to_dict()["error"] == "source_revision_changed"
    assert store.get("conversation") == winner[0]
    with pytest.raises(AnnotationExecutionConflict, match="start a new operation"):
        executor.resume("work", pipelines(), retry_uncertain=True)


@pytest.mark.parametrize("mode", ["wrong_identity", "wrong_input", "invalid_span", "missing_outputs"])
def test_invalid_worker_responses_become_uncertain_without_publishing_documents(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    original_documents = store._connection.execute("SELECT * FROM documents ORDER BY digest").fetchall()

    def invalid(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        response = answer(incoming)
        if mode == "wrong_identity":
            return replace(response, operation_id="another")
        if mode == "wrong_input":
            return replace(response, input_digest="0" * 64)
        if mode == "invalid_span":
            return replace(
                response, annotations=(SpanAnnotation("bad", "token", 0, 999, {"text": "bad", "position": 0}),)
            )
        return replace(response, annotations=(SpanAnnotation("bad", "note", 0, 1, {"label": "wrong declared output"}),))

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", invalid)
    with pytest.raises(AnnotationExecutionUncertain):
        executor.resume("work", pipelines())
    assert executor.get("work").status == "uncertain"
    assert store.get("conversation").revision == 1
    assert store._connection.execute("SELECT * FROM documents ORDER BY digest").fetchall() == original_documents


@pytest.mark.parametrize("mode", ["missing_successor", "changed_successor", "changed_source", "committed_provenance"])
def test_materialization_rejects_corrupt_durable_documents_and_committed_result(
    store: AnnotationStore, calls: dict[str, list[Any]], mode: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    result = executor.resume("work", pipelines())
    output_digest = result.to_dict()["completed"][0]["output_digest"]
    if mode == "missing_successor":
        store._connection.execute("DELETE FROM documents WHERE digest=?", (output_digest,))
    elif mode == "changed_successor":
        store._connection.execute("UPDATE documents SET body='{}' WHERE digest=?", (output_digest,))
    elif mode == "changed_source":
        source_digest = result.to_dict()["completed"][0]["input_digest"]
        store._connection.execute("UPDATE documents SET body='{}' WHERE digest=?", (source_digest,))
    else:
        row = store._connection.execute("SELECT body FROM revisions WHERE revision=2").fetchone()[0]
        data = json.loads(row)
        data["provenance"] = {"different": True}
        encoded = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        checksum = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        store._connection.execute("UPDATE revisions SET body=?,digest=? WHERE revision=2", (encoded, checksum))
    for callback in (
        lambda: executor.get("work"),
        lambda: executor.resume("work", pipelines()),
        lambda: begin(executor),
    ):
        with pytest.raises(InputError):
            callback()


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def checksum(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def rewrite_consistent_journal(store: AnnotationStore, states: list[dict[str, Any]]) -> None:
    """Rehash modified snapshots so tests target semantic validation, not hashes."""
    connection = store._connection
    with store._transaction(write=True):
        guards = connection.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name").fetchall()
        for name, _statement in guards:
            assert name in {"annotation_operation_events_no_update", "annotation_operation_events_no_delete"}
            connection.execute(f"DROP TRIGGER {name}")
        parent = None
        for state in states:
            current_digest = checksum(state)
            connection.execute(
                "UPDATE annotation_operation_events SET body=?,digest=?,parent_digest=? WHERE version=?",
                (canonical(state), current_digest, parent, state["version"]),
            )
            parent = current_digest
        connection.execute(
            "UPDATE annotation_operations SET body=?,digest=? WHERE operation_id='work'",
            (canonical(states[-1]), checksum(states[-1])),
        )
        for _name, statement in guards:
            connection.execute(statement)
    with store._transaction():
        # These fixtures must pass all journal and index/ancestry validation.
        assert ExecutionJournal(store).load("work") == states[-1]


def test_saved_successor_boolean_to_integer_seed_mutation_is_detected_after_rehash(
    store: AnnotationStore, calls: dict[str, list[Any]]
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    store._connection.execute(
        "CREATE TRIGGER stop_commit BEFORE INSERT ON revisions WHEN NEW.revision=2 "
        "BEGIN SELECT RAISE(ABORT,'keep ready checkpoint'); END"
    )
    with pytest.raises(AnnotationStoreError):
        executor.resume("work", pipelines())
    store._connection.execute("DROP TRIGGER stop_commit")
    state = executor.get("work").to_dict()
    assert state["status"] == "ready" and len(state["completed"]) == 1
    original = store._document(state["completed"][0]["output_digest"])
    changed = original.to_dict()
    seed = next(item for item in changed["annotations"] if item["id"] == "note-1")
    seed["features"]["payload"]["flag"] = 1
    replacement = AnnotationDocument.from_dict(changed)
    assert original == replacement and original.digest != replacement.digest
    store._connection.execute(
        "INSERT INTO documents VALUES(?,?)", (replacement.digest, canonical(replacement.to_dict()))
    )
    states = [
        json.loads(row[0])
        for row in store._connection.execute("SELECT body FROM annotation_operation_events ORDER BY version")
    ]
    states[-1]["completed"][0]["output_digest"] = replacement.digest
    rewrite_consistent_journal(store, states)
    assert store._document(replacement.digest).digest == replacement.digest
    with pytest.raises(AnnotationExecutionError, match="immutable input"):
        executor.get("work")


@pytest.mark.parametrize("mode", ["metadata", "provenance"])
def test_committed_boolean_integer_change_is_detected_with_consistent_digest_indexes(
    store: AnnotationStore, calls: dict[str, list[Any]], mode: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    executor.resume("work", pipelines())
    final = store.get("conversation")
    body = json.loads(store._connection.execute("SELECT body FROM revisions WHERE revision=2").fetchone()[0])
    if mode == "metadata":
        body["metadata"]["flags"]["reviewed"] = 1
    else:
        body["provenance"]["annotation_execution"]["request"]["expected_revision"] = True
    changed_digest = checksum(body)
    store._connection.execute(
        "UPDATE revisions SET body=?,digest=? WHERE revision=2", (canonical(body), changed_digest)
    )
    replacement = store.get("conversation")
    assert replacement.digest == changed_digest
    if mode == "metadata":
        assert replacement.event == final.event and replacement.event.digest != final.event.digest
    else:
        assert replacement.provenance == final.provenance
        assert checksum(replacement.to_dict()["provenance"]) != checksum(final.to_dict()["provenance"])
    states = [
        json.loads(row[0])
        for row in store._connection.execute("SELECT body FROM annotation_operation_events ORDER BY version")
    ]
    states[-1]["result"]["digest"] = changed_digest
    rewrite_consistent_journal(store, states)
    with pytest.raises(AnnotationExecutionError, match="committed event and execution journal"):
        executor.get("work")


def test_operation_discovery_and_audit_history_use_binary_exclusive_pages(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    assert executor.list() == ()
    for key in ("z", "a", "B", "a-1", "0"):
        begin(executor, key=key)

    def interrupted(_worker: RemoteAnnotationProcessor, _incoming: AnnotationRequest) -> AnnotationResponse:
        raise KeyboardInterrupt

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", interrupted)
    with pytest.raises(KeyboardInterrupt):
        executor.resume("a", pipelines())
    assert [item.operation_id for item in executor.list(limit=2)] == ["0", "B"]
    assert [item.operation_id for item in executor.list(after_operation_id="B", limit=2)] == ["a", "a-1"]
    assert [item.operation_id for item in executor.list(after_operation_id="a-1")] == ["z"]
    assert executor.list(after_operation_id="z") == ()
    assert executor.list(after_operation_id="a", limit=1)[0].operation_id == "a-1"
    assert next(item for item in executor.list() if item.operation_id == "a").status == "reserved"
    before = executor.history("a", limit=1)
    assert len(before) == 1 and before[0].version == 1 and before[0].status == "ready"
    assert [(item.version, item.status) for item in executor.history("a", after_version=1)] == [(2, "reserved")]
    assert executor.history("a", after_version=2) == ()
    assert executor.history("a", after_version=1000) == ()
    with pytest.raises(KeyError):
        executor.history("missing")


@pytest.mark.parametrize("bad", [True, False, 0, -1, 1001, 1.0, "2", None])
def test_list_and_history_reject_malformed_page_limits(
    store: AnnotationStore, calls: dict[str, list[Any]], bad: Any
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    for callback in (lambda: executor.list(limit=bad), lambda: executor.history("work", limit=bad)):
        with pytest.raises(InputError):
            callback()


@pytest.mark.parametrize("bad", [True, "", "a b", "雪", "a" * 129, 7])
def test_list_and_history_reject_malformed_operation_cursors(
    store: AnnotationStore, calls: dict[str, list[Any]], bad: Any
) -> None:
    executor = AnnotationExecutor(store)
    for callback in (lambda: executor.list(after_operation_id=bad), lambda: executor.history(bad)):
        with pytest.raises(InputError):
            callback()


@pytest.mark.parametrize("bad", [True, False, -1, 1.0, "2", None, 2**63 - 1])
def test_history_requires_exact_nonnegative_bounded_version(
    store: AnnotationStore, calls: dict[str, list[Any]], bad: Any
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    with pytest.raises(InputError):
        executor.history("work", after_version=bad)


@pytest.mark.parametrize("mode", ["head", "history", "document"])
def test_discovery_and_history_validate_requested_operations_before_reporting_them(
    store: AnnotationStore, calls: dict[str, list[Any]], mode: str
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    final = executor.resume("work", pipelines())
    if mode == "head":
        store._connection.execute("UPDATE annotation_operations SET digest=?", ("0" * 64,))
    elif mode == "history":
        # A head-less appended history row must not be silently hidden by paging.
        store._connection.execute("INSERT INTO annotation_operation_events VALUES('work',5,?,'bad','{}')", ("0" * 64,))
    else:
        output = final.to_dict()["completed"][0]["output_digest"]
        store._connection.execute("DELETE FROM documents WHERE digest=?", (output,))
    for callback in (
        executor.list,
        lambda: executor.history("work", limit=1),
        lambda: executor.history("work", after_version=999),
    ):
        with pytest.raises(InputError):
            callback()


@pytest.mark.parametrize("different_request", [False, True])
def test_second_begin_during_network_preflight_rechecks_idempotency_binding(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch, different_request: bool
) -> None:
    executor = AnnotationExecutor(store)
    verification_count = 0
    winning: list[AnnotationOperation] = []

    def another_coordinator(_worker: RemoteAnnotationProcessor) -> None:
        nonlocal verification_count
        verification_count += 1
        if verification_count == 1:
            selected = pipelines()
            if different_request:
                selected["a-original"] = replace(selected["a-original"], version="2")
            with AnnotationStore(store.path, create=False) as other:
                winning.append(begin(AnnotationExecutor(other), selected))

    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", another_coordinator)
    if different_request:
        with pytest.raises(AnnotationExecutionConflict, match="already bound to a different request"):
            begin(executor)
    else:
        assert begin(executor) == winning[0]
    assert verification_count == 2
    assert executor.get("work") == winning[0]
    assert not calls["execute"]
    assert store._connection.execute("SELECT COUNT(*) FROM annotation_operation_events").fetchone()[0] == 1


@pytest.mark.parametrize("other_finishes", [False, True])
def test_coordinator_race_between_ready_read_and_reserve_never_duplicates_dispatch(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch, other_finishes: bool
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    original_reserve = executor._reserve
    dispatched = []

    def other_worker(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        dispatched.append(incoming.step_id)
        if not other_finishes:
            raise KeyboardInterrupt
        return answer(incoming)

    def before_reserve(operation_id: str) -> Any:
        with AnnotationStore(store.path, create=False) as other:
            contender = AnnotationExecutor(other)
            if other_finishes:
                contender.resume(operation_id, pipelines())
            else:
                with pytest.raises(KeyboardInterrupt):
                    contender.resume(operation_id, pipelines())
        return original_reserve(operation_id)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", other_worker)
    monkeypatch.setattr(executor, "_reserve", before_reserve)
    with pytest.raises(AnnotationExecutionConflict, match="before step reservation"):
        executor.resume("work", pipelines())
    assert dispatched == ["s000"]
    state = executor.get("work")
    assert state.status == ("committed" if other_finishes else "reserved")
    assert state.to_dict()["attempts"] == [1]
    assert store.get("conversation").revision == (2 if other_finishes else 1)


@pytest.mark.parametrize("competing_operation", [False, True])
def test_final_publication_rechecks_after_payload_preparation_race(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch, competing_operation: bool
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)

    def stop_before_final(_operation_id: str) -> AnnotationOperation:
        raise RuntimeError("pause before final commit")

    with monkeypatch.context() as patch:
        patch.setattr(executor, "_commit", stop_before_final)
        with pytest.raises(RuntimeError, match="pause before final"):
            executor.resume("work", pipelines())
    assert executor.get("work").status == "ready"
    assert executor.get("work").completed_steps == 1
    original_prepare = store._prepare_append

    def concurrent_commit(event: AnnotationEvent, provenance: Any = None) -> Any:
        prepared = original_prepare(event, provenance)
        assert not store._connection.in_transaction
        with AnnotationStore(store.path, create=False) as other:
            if competing_operation:
                other.put(
                    AnnotationEvent("conversation", (AnnotationDocument("external", "won"),)), expected_revision=1
                )
            else:
                assert AnnotationExecutor(other).resume("work", pipelines()).status == "committed"
        return prepared

    monkeypatch.setattr(store, "_prepare_append", concurrent_commit)
    with pytest.raises(AnnotationExecutionConflict):
        executor.resume("work", pipelines())
    state = executor.get("work")
    assert state.status == ("conflict" if competing_operation else "committed")
    assert store.get("conversation").revision == 2
    assert len(calls["execute"]) == 1
    assert len(executor.history("work")) == 4
    assert store.verify().revisions == 2


def test_two_operations_on_same_source_have_exactly_one_event_publication(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor, key="op-a")
    begin(executor, key="op-b")
    barrier = threading.Barrier(2)
    dispatched = []

    def synchronized_worker(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        dispatched.append(incoming.operation_id)
        barrier.wait(timeout=10)
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", synchronized_worker)

    def run(key: str) -> str:
        with AnnotationStore(store.path, create=False) as independent:
            contender = AnnotationExecutor(independent)
            try:
                return contender.resume(key, pipelines()).status
            except AnnotationExecutionConflict:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, key) for key in ("op-a", "op-b")]
        assert sorted(future.result(timeout=30) for future in futures) == ["committed", "conflict"]
    states = executor.list()
    assert sorted(item.status for item in states) == ["committed", "conflict"]
    assert sorted(dispatched) == ["op-a", "op-b"]
    winner = next(item for item in states if item.status == "committed")
    assert winner.to_dict()["result"]["digest"] == store.get("conversation").digest
    assert store.get("conversation").revision == 2
    assert store.verify().revisions == 2


def test_late_worker_exception_cannot_overwrite_successful_explicit_retry(
    store: AnnotationStore, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = AnnotationExecutor(store)
    begin(executor)
    invocations = 0

    def late_failure(_worker: RemoteAnnotationProcessor, incoming: AnnotationRequest) -> AnnotationResponse:
        nonlocal invocations
        invocations += 1
        if invocations == 1:
            with AnnotationStore(store.path, create=False) as other:
                AnnotationExecutor(other).resume("work", pipelines(), retry_uncertain=True)
            raise RuntimeError("stale old call failed after new result committed")
        return answer(incoming)

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", late_failure)
    with pytest.raises(AnnotationExecutionUncertain):
        executor.resume("work", pipelines())
    final = executor.get("work")
    assert final.status == "committed" and final.to_dict()["error"] is None
    assert final.to_dict()["attempts"] == [2] and final.completed_steps == 1
    assert invocations == 2 and store.get("conversation").revision == 2
