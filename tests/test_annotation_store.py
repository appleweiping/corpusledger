"""Transactional, concurrency and corruption tests for durable annotation events."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from corpusledger import (
    AnnotationDocument,
    AnnotationField,
    AnnotationPipeline,
    AnnotationProcessor,
    AnnotationType,
    InputError,
    SpanAnnotation,
)
from corpusledger.annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationStore,
    AnnotationStoreError,
)


def event(event_id: str = "e", text: str = "Hello 😀") -> AnnotationEvent:
    doc = AnnotationDocument("original", text)
    translation = AnnotationDocument("translation", "你好\uff01")
    return AnnotationEvent(event_id, (translation, doc), {"source": {"tags": ["checked"]}})


def test_event_immutable_roundtrip_and_exact_content() -> None:
    snapshot = event()
    assert AnnotationEvent.from_dict(snapshot.to_dict()) == snapshot
    assert snapshot.get_document("original").text == "Hello 😀"
    reversed_event = AnnotationEvent("e", tuple(reversed(snapshot.documents)), snapshot.metadata)
    assert reversed_event.digest == snapshot.digest
    assert event(text="hello 😀").digest != snapshot.digest
    data = snapshot.to_dict()
    data["metadata"]["source"]["tags"].append("mutated")
    data["documents"][0]["text"] = "mutated"
    assert snapshot.metadata["source"]["tags"] == ("checked",)
    with pytest.raises(TypeError):
        snapshot.metadata["source"]["tags"][0] = "mutated"
    with pytest.raises(KeyError):
        snapshot.get_document("absent")


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(event_id=""),
        dict(documents=None),
        dict(documents=("bad",)),
        dict(metadata=[]),
        dict(metadata={"bad": float("nan")}),
    ],
)
def test_invalid_event(kwargs: dict[str, Any]) -> None:
    with pytest.raises(InputError):
        AnnotationEvent(**({"event_id": "e"} | kwargs))
    doc = AnnotationDocument("d", "")
    with pytest.raises(AnnotationStoreError, match="unique"):
        AnnotationEvent("e", (doc, doc))


def test_serialized_event_is_closed_and_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    value = event().to_dict()
    for change in ({"format": "future"}, {"documents": {}}, {"extra": True}):
        with pytest.raises(InputError):
            AnnotationEvent.from_dict(value | change)
    monkeypatch.setattr("corpusledger.annotation_store.MAX_EVENT_DOCUMENTS", 1)
    with pytest.raises(AnnotationStoreError, match="exceeds 1"):
        AnnotationEvent.from_dict(value)
    with pytest.raises(AnnotationStoreError, match="at most 1"):
        event()


def test_create_reopen_history_and_content_addressed_documents(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite"
    with AnnotationStore(path) as store:
        first = store.put(event())
        assert first.revision == 1 and first.parent_digest is None
        assert first.documents == ("original", "translation")
        assert first.event == event()
        assert first.to_dict()["event"] == event().to_dict()
        second = store.put(event(text="changed"), expected_revision=1, provenance={"reviewed": True})
        assert second.revision == 2 and second.parent_digest == first.digest
        assert store.get("e", 1) == first
        assert store.get("e") == second
        assert store.history("e", after_revision=1, limit=1)[0].digest == second.digest
        assert store.history("e", after_revision=2) == ()
        assert store.list(limit=1)[0].to_dict()["documents"] == ["original", "translation"]
        assert store.verify().to_dict() == {
            "format": "corpusledger.annotation-store-verification.v1",
            "events": 1,
            "revisions": 2,
            "documents": 3,
            "valid": True,
        }
    # Unchanged translation document is stored once across two event versions.
    with sqlite3.connect(path) as database:
        assert database.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 3
    with AnnotationStore(path, create=False) as reopened:
        assert reopened.get("e").event.get_document("original").text == "changed"
        assert reopened.get("e", 1).event.get_document("original").text == "Hello 😀"
        assert reopened.verify("e").revisions == 2
    reopened.close()
    with pytest.raises(AnnotationStoreError, match="closed"):
        reopened.get("e")
    with pytest.raises(AnnotationStoreError, match="closed"):
        reopened.__enter__()


def test_put_requires_explicit_revision_and_keeps_state_on_conflict(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        with pytest.raises(AnnotationConflictError) as missing:
            store.put(event(), expected_revision=1)
        assert missing.value.actual_revision == 0
        assert store.verify().revisions == 0
        initial = store.put(event())
        with pytest.raises(AnnotationConflictError) as stale:
            store.put(event(text="stale"))
        assert stale.value.expected_revision == 0 and stale.value.actual_revision == 1
        assert store.get("e") == initial
        assert store.verify().documents == 2


def test_atomic_commit_rolls_back_document_inserts(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        initial = store.put(event())
        with sqlite3.connect(path) as database:
            database.execute(
                "CREATE TRIGGER fail_revision BEFORE INSERT ON revisions BEGIN SELECT RAISE(ABORT,'injected'); END"
            )
        with pytest.raises(AnnotationStoreError, match="transaction failed"):
            store.put(event(text="new document"), expected_revision=1)
        assert store.get("e") == initial
        with sqlite3.connect(path) as database:
            assert database.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 2


def test_independent_concurrent_writers_have_one_winner(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
    barrier = threading.Barrier(2)

    def update(text: str) -> tuple[str, int]:
        with AnnotationStore(path, create=False) as writer:
            assert writer.get("e").revision == 1
            barrier.wait(timeout=10)
            try:
                return "success", writer.put(event(text=text), expected_revision=1).revision
            except AnnotationConflictError as exc:
                return "conflict", exc.actual_revision

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(update, text) for text in ("A", "B")]
        assert sorted(future.result(timeout=20) for future in futures) == [("conflict", 2), ("success", 2)]
    with AnnotationStore(path, create=False) as store:
        assert store.get("e").event.get_document("original").text in ("A", "B")
        assert store.verify().revisions == 2
        assert store.verify().documents == 3


def test_binary_pagination_and_independent_event_histories(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        for identifier in ("b", "a", "z", "é"):
            store.put(event(identifier))
        store.put(event("a", "again"), expected_revision=1)
        assert [item.event_id for item in store.list(limit=2)] == ["a", "b"]
        assert [item.event_id for item in store.list(after_event_id="b", limit=2)] == ["z", "é"]
        assert store.list(after_event_id="é") == ()
        assert store.verify().events == 4
        assert store.verify("a").revisions == 2
        for method in (store.get, store.history, store.verify):
            with pytest.raises(KeyError):
                method("unknown")
        with pytest.raises(KeyError):
            store.get("a", 3)


@pytest.mark.parametrize("value", [True, -1, 1.0, "1", 2**63])
def test_invalid_revisions(tmp_path: Path, value: Any) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        for operation in (
            lambda: store.get("e", value),
            lambda: store.put(event(), expected_revision=value),
            lambda: store.history("e", after_revision=value),
        ):
            with pytest.raises(AnnotationStoreError):
                operation()


@pytest.mark.parametrize("value", [False, 0, 1001, 1.0, "1"])
def test_invalid_page_limits(tmp_path: Path, value: Any) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        with pytest.raises(AnnotationStoreError):
            store.list(limit=value)
        with pytest.raises(AnnotationStoreError):
            store.history("e", limit=value)


def test_argument_errors_and_no_accidental_creation(tmp_path: Path) -> None:
    missing = tmp_path / "missing.db"
    with pytest.raises(AnnotationStoreError, match="cannot open"):
        AnnotationStore(missing, create=False)
    assert not missing.exists()
    for kwargs in (dict(create=1), dict(timeout=True), dict(timeout=-1), dict(timeout=float("inf"))):
        with pytest.raises(AnnotationStoreError):
            AnnotationStore(missing, **kwargs)
    with AnnotationStore(tmp_path / "db") as store:
        with pytest.raises(AnnotationStoreError):
            store.put(None)  # type: ignore[arg-type]
        with pytest.raises(AnnotationStoreError):
            store.put(event(), provenance=[])  # type: ignore[arg-type]
        for operation in (
            lambda: store.get(""),
            lambda: store.history(""),
            lambda: store.list(after_event_id=""),
            lambda: store.verify(""),
        ):
            with pytest.raises(InputError):
                operation()


def test_unknown_databases_not_modified(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.write_bytes(b"not a sqlite database")
    with pytest.raises(AnnotationStoreError):
        AnnotationStore(raw)
    assert raw.read_bytes() == b"not a sqlite database"
    path = tmp_path / "other.db"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE unrelated(value TEXT)")
        database.execute("INSERT INTO unrelated VALUES('keep')")
    before = path.read_bytes()
    with pytest.raises(AnnotationStoreError, match="not a supported"):
        AnnotationStore(path)
    assert path.read_bytes() == before
    with AnnotationStore(tmp_path / "version.db"):
        pass
    with sqlite3.connect(tmp_path / "version.db") as database:
        database.execute("PRAGMA user_version=99")
    with pytest.raises(AnnotationStoreError, match="identity/version"):
        AnnotationStore(tmp_path / "version.db")


@pytest.mark.parametrize(
    "statement",
    ["PRAGMA application_id=91", "PRAGMA user_version=44", "CREATE VIEW reserved AS SELECT 1"],
)
def test_initialization_refuses_owned_tableless_databases(tmp_path: Path, statement: str) -> None:
    path = tmp_path / "reserved.db"
    database = sqlite3.connect(path)
    try:
        database.execute(statement)
        database.commit()
    finally:
        database.close()
    before = path.read_bytes()
    with pytest.raises(AnnotationStoreError, match="not a supported"), AnnotationStore(path):
        pass
    assert path.read_bytes() == before


def test_corrupted_document_rejected_on_get_verify_and_reuse(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
        with sqlite3.connect(path) as database:
            database.execute("UPDATE documents SET body='{}'")
        for operation in (lambda: store.get("e"), store.verify, lambda: store.put(event(), expected_revision=1)):
            with pytest.raises(InputError):
                operation()


def _rewrite_revision(database: sqlite3.Connection, number: int, update: dict[str, Any]) -> None:
    body = json.loads(database.execute("SELECT body FROM revisions WHERE revision=?", (number,)).fetchone()[0])
    body.update(update)
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    database.execute("UPDATE revisions SET body=?,digest=? WHERE revision=?", (raw, digest, number))


@pytest.mark.parametrize("mode", ["body", "gap", "parent", "missing_document", "wrong_document_id", "bool_revision"])
def test_corruption_failures_are_not_silent_success(tmp_path: Path, mode: str) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
        store.put(event(text="second"), expected_revision=1)
        with sqlite3.connect(path) as database:
            if mode == "body":
                database.execute("UPDATE revisions SET body='{}' WHERE revision=2")
            elif mode == "gap":
                database.execute("DELETE FROM revisions WHERE revision=1")
            elif mode == "parent":
                database.execute("UPDATE revisions SET parent_digest='bad' WHERE revision=2")
            elif mode == "missing_document":
                database.execute("DELETE FROM documents")
            elif mode == "bool_revision":
                _rewrite_revision(database, 2, {"revision": True})
            else:
                refs = json.loads(database.execute("SELECT body FROM revisions WHERE revision=2").fetchone()[0])[
                    "documents"
                ]
                refs[0]["id"] = "fake"
                _rewrite_revision(database, 2, {"documents": refs})
        with pytest.raises(InputError):
            store.get("e")
        with pytest.raises(InputError):
            store.verify()


def test_document_digest_detects_semantically_valid_replacement(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        original = event()
        store.put(original)
        with sqlite3.connect(path) as database:
            database.execute(
                "UPDATE documents SET body=? WHERE digest=?",
                (
                    json.dumps(AnnotationDocument("original", "replaced").to_dict()),
                    original.get_document("original").digest,
                ),
            )
        with pytest.raises(AnnotationStoreError, match="content digest"):
            store.get("e")


def test_store_byte_limits_preserve_prior_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        first = store.put(event())
        monkeypatch.setattr("corpusledger.annotation_store.MAX_EVENT_BYTES", 40)
        with pytest.raises(AnnotationStoreError, match="byte limit"):
            store.put(event(text="many text words"), expected_revision=1)
        with pytest.raises(AnnotationStoreError, match="complete event"):
            store.put(AnnotationEvent("empty"))
        monkeypatch.setattr("corpusledger.annotation_store.MAX_EVENT_BYTES", 128 * 1024 * 1024)
        assert store.get("e") == first
        assert store.verify().events == 1


def _pipeline(callback: Any = None) -> AnnotationPipeline:
    schema = AnnotationType("whole", {"label": AnnotationField()})

    def annotate(document: AnnotationDocument) -> tuple[SpanAnnotation, ...]:
        if callback is not None:
            callback()
        return (SpanAnnotation("whole", "whole", 0, len(document.text), {"label": "checked"}),)

    return AnnotationPipeline((AnnotationProcessor("annotate", "1", annotate, produces=(schema,)),))


def test_process_multidocument_provenance_is_atomic_and_persistent(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        initial = store.put(event())
        result = store.process("e", {"original": _pipeline(), "translation": _pipeline()}, expected_revision=1)
        assert result.revision == 2
        assert all(document.annotations for document in result.event.documents)
        assert initial.event.get_document("original").annotations == ()
        reports = result.to_dict()["provenance"]["annotation_pipelines"]
        assert reports["original"]["input_digest"] == initial.event.get_document("original").digest
        assert reports["translation"]["output_digest"] == result.event.get_document("translation").digest
        assert store.get("e").provenance == result.provenance
        assert store.verify().documents == 4


def test_process_preflight_failure_has_zero_callbacks(tmp_path: Path) -> None:
    called = []
    with AnnotationStore(tmp_path / "db") as store:
        store.put(event())
        with pytest.raises(KeyError):
            store.process(
                "e", {"original": _pipeline(lambda: called.append(1)), "missing": _pipeline()}, expected_revision=1
            )
        assert called == [] and store.get("e").revision == 1
        for bad in ({}, {"original": 3}, None):
            with pytest.raises(AnnotationStoreError):
                store.process("e", bad, expected_revision=1)  # type: ignore[arg-type]
        with pytest.raises(AnnotationConflictError):
            store.process("e", {"original": _pipeline(lambda: called.append(1))}, expected_revision=2)
        assert called == []


def test_process_callback_failure_and_intervening_writer(tmp_path: Path) -> None:
    path = tmp_path / "db"

    def fail() -> None:
        raise RuntimeError("application failure")

    def intervening_write() -> None:
        with AnnotationStore(path, create=False) as other:
            other.put(event(text="concurrent winner"), expected_revision=1)

    with AnnotationStore(path) as store:
        initial = store.put(event())
        with pytest.raises(RuntimeError, match="application failure"):
            store.process("e", {"original": _pipeline(), "translation": _pipeline(fail)}, expected_revision=1)
        assert store.get("e") == initial
        with pytest.raises(AnnotationConflictError):
            store.process("e", {"original": _pipeline(intervening_write)}, expected_revision=1)
        current = store.get("e")
        assert current.event.get_document("original").text == "concurrent winner"
        assert current.event.get_document("original").annotations == ()
        assert current.revision == 2 and store.verify().revisions == 2


def test_process_snapshots_preflight_declarations_before_callbacks(tmp_path: Path) -> None:
    called: list[str] = []
    planned: dict[str, AnnotationPipeline] = {}

    def mutate_input() -> None:
        called.append("first")
        planned["translation"] = _pipeline(lambda: called.append("replacement"))

    planned.update({"original": _pipeline(mutate_input), "translation": _pipeline(lambda: called.append("original"))})
    with AnnotationStore(tmp_path / "db") as store:
        store.put(event())
        result = store.process("e", planned, expected_revision=1)
        assert result.revision == 2 and called == ["first", "original"]
        assert all(document.annotations for document in result.event.documents)


def test_existing_store_open_and_read_do_not_reserve_writer_lock(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        first = store.put(event())
    with sqlite3.connect(path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with AnnotationStore(path, create=False, timeout=0) as reader:
            assert reader.get("e") == first
            assert reader.verify().revisions == 1
        writer.rollback()


def test_boolean_revision_one_is_rejected_even_with_matching_index_and_hash(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
        with sqlite3.connect(path) as database:
            _rewrite_revision(database, 1, {"revision": True})
        with pytest.raises(AnnotationStoreError, match="positive bounded integer"):
            store.get("e", 1)


def test_durable_json_extensions_and_format_errors(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
        with sqlite3.connect(path) as database:
            saved = database.execute("SELECT body FROM revisions").fetchone()[0]
        for bad in ("NaN", '{"x":1,"x":2}', "[" * 1200):
            with sqlite3.connect(path) as database:
                database.execute("UPDATE revisions SET body=?", (bad,))
            with pytest.raises(AnnotationStoreError, match="invalid or ambiguous JSON"):
                store.get("e")
        with sqlite3.connect(path) as database:
            database.execute("UPDATE revisions SET body=?", (saved,))
            _rewrite_revision(database, 1, {"format": "future"})
        with pytest.raises(AnnotationStoreError, match="identity"):
            store.get("e")


@pytest.mark.parametrize(
    "change",
    [
        {"documents": {}},
        {"documents": [{"id": "d", "digest": "bad"}]},
        {"documents": [{"id": "d", "digest": "a" * 64}, {"id": "d", "digest": "b" * 64}]},
        {"metadata": []},
        {"provenance": []},
    ],
)
def test_descriptor_structure_revalidated_on_every_read(tmp_path: Path, change: dict[str, Any]) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        store.put(event())
        with sqlite3.connect(tmp_path / "db") as database:
            _rewrite_revision(database, 1, change)
        with pytest.raises(AnnotationStoreError):
            store.get("e")


def test_read_only_schemas_and_byte_limit_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.put(event())
        monkeypatch.setattr("corpusledger.annotation_store.MAX_EVENT_BYTES", 3)
        with pytest.raises(AnnotationStoreError, match="event byte limit"):
            store.get("e")
    with sqlite3.connect(path) as database:
        database.execute("ALTER TABLE documents ADD COLUMN surprise TEXT")
    with pytest.raises(AnnotationStoreError, match="schema"):
        AnnotationStore(path, create=False)
