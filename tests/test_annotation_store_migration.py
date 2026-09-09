"""Explicit journal migration, descriptor compatibility and composed transactions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationDocument
from corpusledger.annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationStore,
    AnnotationStoreError,
)


def event(text: str = '雪 😀 "quoted" \\ end') -> AnnotationEvent:
    return AnnotationEvent(
        "event-雪", (AnnotationDocument("doc", text),), {"nested": {"false": False, "array": [1, None, "é"]}}
    )


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def history_bytes(connection: sqlite3.Connection) -> tuple[list[Any], list[Any]]:
    return (
        connection.execute("SELECT digest,CAST(body AS BLOB) FROM documents ORDER BY digest").fetchall(),
        connection.execute(
            "SELECT event_id,revision,digest,parent_digest,CAST(body AS BLOB) FROM revisions ORDER BY event_id,revision"
        ).fetchall(),
    )


def test_new_store_and_ordinary_reopen_stay_v1_without_writes(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite"
    with AnnotationStore(path) as store:
        store.put(event())
        assert not store.execution_enabled
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert store._tables() == {"documents", "revisions"}
    before = path.read_bytes()
    with sqlite3.connect(path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with AnnotationStore(path, create=False, timeout=0) as reader:
            assert not reader.execution_enabled
            assert reader.get("event-雪").revision == 1
        writer.rollback()
    assert path.read_bytes() == before


def test_migration_preserves_exact_history_bytes_and_remains_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite"
    with AnnotationStore(path) as store:
        first = store.put(event(), provenance={"source": ["checked", {"n": 1.5}]})
        second = store.put(event("second"), expected_revision=1)
        before = history_bytes(store._connection)
        store.enable_execution_journal()
        assert store.execution_enabled
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert store._tables() == {"documents", "revisions", "annotation_operations", "annotation_operation_events"}
        assert history_bytes(store._connection) == before
        store.enable_execution_journal()
        assert history_bytes(store._connection) == before
        assert store.get("event-雪", 1) == first
        assert store.get("event-雪") == second
        assert store.verify().revisions == 2
    before_reopen = path.read_bytes()
    with AnnotationStore(path, create=False) as reopened:
        assert reopened.execution_enabled
        assert history_bytes(reopened._connection) == before
    assert path.read_bytes() == before_reopen


def test_v2_open_and_property_do_not_take_writer_lock(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        first = store.put(event())
        store.enable_execution_journal()
    with sqlite3.connect(path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with AnnotationStore(path, create=False, timeout=0) as reader:
            assert reader.execution_enabled
            assert reader.get("event-雪") == first
        writer.rollback()


def test_event_descriptors_equal_independent_historical_serialization(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        parent = None
        for number in range(1, 13):
            # Cross the one/two-digit revision boundary and the schema upgrade.
            if number == 6:
                store.enable_execution_journal()
            snapshot = event(f"document {number} 😀")
            provenance = {"a": ["雪", None], "z": {"bool": False}}
            document_raw = canonical(snapshot.documents[0].to_dict()).encode("utf-8")
            descriptor_raw = canonical(
                {
                    "format": "corpusledger.annotation-store.v1",
                    "event_id": snapshot.event_id,
                    "revision": number,
                    "parent_digest": parent,
                    "metadata": snapshot.to_dict()["metadata"],
                    "provenance": provenance,
                    "documents": [{"id": "doc", "digest": hashlib.sha256(document_raw).hexdigest()}],
                }
            ).encode("utf-8")
            written = store.put(snapshot, expected_revision=number - 1, provenance=provenance)
            assert written.digest == hashlib.sha256(descriptor_raw).hexdigest()
            row = store._connection.execute(
                "SELECT CAST(body AS BLOB) FROM revisions WHERE revision=?", (number,)
            ).fetchone()
            assert row[0] == descriptor_raw
            assert store.get(snapshot.event_id, number) == written
            parent = written.digest


@pytest.mark.parametrize("stage", ["first_table", "second_table", "first_guard", "second_guard", "version"])
def test_every_migration_ddl_failure_rolls_back_to_v1(tmp_path: Path, stage: str) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        original = store.put(event())
        before = history_bytes(store._connection)
        schema = store._connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()

        def deny(action: int, arg1: str | None, arg2: str | None, _db: str | None, _source: str | None) -> int:
            rejected = (
                (stage == "first_table" and action == sqlite3.SQLITE_CREATE_TABLE and arg1 == "annotation_operations")
                or (
                    stage == "second_table"
                    and action == sqlite3.SQLITE_CREATE_TABLE
                    and arg1 == "annotation_operation_events"
                )
                or (
                    stage == "first_guard"
                    and action == sqlite3.SQLITE_CREATE_TRIGGER
                    and arg1 == "annotation_operation_events_no_update"
                )
                or (
                    stage == "second_guard"
                    and action == sqlite3.SQLITE_CREATE_TRIGGER
                    and arg1 == "annotation_operation_events_no_delete"
                )
                or (stage == "version" and action == sqlite3.SQLITE_PRAGMA and arg1 == "user_version" and arg2 == "2")
            )
            return sqlite3.SQLITE_DENY if rejected else sqlite3.SQLITE_OK

        store._connection.set_authorizer(deny)
        try:
            with pytest.raises(AnnotationStoreError, match="transaction failed"):
                store.enable_execution_journal()
        finally:
            # Python 3.10 cannot disable an authorizer with None. Replace the
            # injected fault with an allow-all hook before verifying rollback.
            store._connection.set_authorizer(lambda *_args: sqlite3.SQLITE_OK)
        assert not store._connection.in_transaction
        assert not store.execution_enabled
        assert history_bytes(store._connection) == before
        assert store._connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall() == schema
        assert store.get("event-雪") == original
        store.enable_execution_journal()
        assert store.execution_enabled


def test_failure_after_version_change_also_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        store.put(event())
        before = history_bytes(store._connection)
        original = store._validate_schema

        def fail_after_upgrade() -> int:
            version = original()
            if version == 2:
                raise RuntimeError("injected final validation fault")
            return version

        with monkeypatch.context() as patch:
            patch.setattr(store, "_validate_schema", fail_after_upgrade)
            with pytest.raises(RuntimeError, match="final validation"):
                store.enable_execution_journal()
        assert not store.execution_enabled
        assert store._tables() == {"documents", "revisions"}
        assert history_bytes(store._connection) == before


def test_concurrent_upgrades_and_existing_connection_observe_same_v2(tmp_path: Path) -> None:
    path = tmp_path / "db"
    barrier = threading.Barrier(2)
    with AnnotationStore(path) as observer:
        observer.put(event())
        before = history_bytes(observer._connection)

        def upgrade() -> bool:
            with AnnotationStore(path, create=False) as writer:
                assert not writer.execution_enabled
                barrier.wait(timeout=10)
                writer.enable_execution_journal()
                writer.enable_execution_journal()
                return writer.execution_enabled

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(upgrade) for _ in range(2)]
            assert [future.result(timeout=20) for future in futures] == [True, True]
        assert observer.execution_enabled
        assert history_bytes(observer._connection) == before
        with observer._transaction(write=True):
            assert observer.execution_enabled


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE annotation_operation_events SET body='changed'",
        "DELETE FROM annotation_operation_events",
    ],
)
def test_operation_history_is_append_only_but_current_state_is_mutable(tmp_path: Path, statement: str) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        store.enable_execution_journal()
        with store._transaction(write=True):
            store._connection.execute("INSERT INTO annotation_operations VALUES('operation',1,'first','{}')")
            store._connection.execute("INSERT INTO annotation_operation_events VALUES('operation',1,'first',NULL,'{}')")
        with pytest.raises(AnnotationStoreError, match="transaction failed"), store._transaction(write=True):
            store._connection.execute(statement)
        with store._transaction(write=True):
            store._connection.execute(
                "UPDATE annotation_operations SET version=2,digest='second' WHERE operation_id='operation'"
            )
            store._connection.execute(
                "INSERT INTO annotation_operation_events VALUES('operation',2,'second','first','{}')"
            )
        assert store._connection.execute("SELECT version,digest FROM annotation_operations").fetchall() == [
            (2, "second")
        ]
        assert store._connection.execute(
            "SELECT version,body FROM annotation_operation_events ORDER BY version"
        ).fetchall() == [(1, "{}"), (2, "{}")]


@pytest.mark.parametrize(
    "change",
    [
        "missing_guard",
        "wrong_guard",
        "wrong_columns",
        "wrong_type",
        "wrong_key",
        "extra_table",
        "downgrade",
        "future",
        "identity",
    ],
)
def test_v2_identity_columns_and_guards_are_strict_and_not_repaired(tmp_path: Path, change: str) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store.enable_execution_journal()
    with sqlite3.connect(path) as database:
        if change in {"missing_guard", "wrong_guard"}:
            database.execute("DROP TRIGGER annotation_operation_events_no_update")
            if change == "wrong_guard":
                database.execute(
                    "CREATE TRIGGER annotation_operation_events_no_update BEFORE UPDATE ON annotation_operation_events "
                    "BEGIN SELECT 1; END"
                )
        elif change == "wrong_columns":
            database.execute("ALTER TABLE annotation_operations ADD COLUMN extra TEXT")
        elif change in {"wrong_type", "wrong_key"}:
            database.execute("DROP TABLE annotation_operations")
            if change == "wrong_type":
                database.execute(
                    "CREATE TABLE annotation_operations(operation_id TEXT PRIMARY KEY, version TEXT NOT NULL, "
                    "digest TEXT NOT NULL, body TEXT NOT NULL)"
                )
            else:
                database.execute(
                    "CREATE TABLE annotation_operations(operation_id TEXT, version INTEGER NOT NULL, "
                    "digest TEXT NOT NULL, body TEXT NOT NULL)"
                )
        elif change == "extra_table":
            database.execute("CREATE TABLE extra(value TEXT)")
        elif change == "downgrade":
            database.execute("PRAGMA user_version=1")
        elif change == "future":
            database.execute("PRAGMA user_version=3")
        else:
            database.execute("PRAGMA application_id=123")
    before = path.read_bytes()
    with pytest.raises(AnnotationStoreError), AnnotationStore(path):
        pass
    assert path.read_bytes() == before


def test_v1_rejects_partial_or_reserved_execution_schema(tmp_path: Path) -> None:
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        store._connection.execute("CREATE TABLE annotation_operations(operation_id TEXT)")
        with pytest.raises(AnnotationStoreError, match="not a supported"):
            store.enable_execution_journal()
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 1
    with pytest.raises(AnnotationStoreError), AnnotationStore(path):
        pass


def test_preparation_is_frozen_and_append_requires_active_transaction(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        provenance = {"nested": ["initial"]}
        prepared = store._prepare_append(event(), provenance)
        provenance["nested"].append("not included")
        with pytest.raises(TypeError):
            prepared.provenance["nested"][0] = "mutation"
        with pytest.raises(AnnotationStoreError, match="active transaction"):
            store._append_in_transaction(prepared, expected_revision=0)
        assert store.verify().revisions == 0
        with store._transaction(write=True):
            result = store._append_in_transaction(prepared, expected_revision=0)
            assert result.to_dict()["provenance"] == {"nested": ["initial"]}
            assert store._connection.in_transaction
        assert store.get("event-雪") == result
        with pytest.raises(AnnotationStoreError, match="prepared payload"), store._transaction(write=True):
            store._append_in_transaction(None, expected_revision=1)  # type: ignore[arg-type]
        with pytest.raises(AnnotationStoreError, match="AnnotationEvent"):
            store._prepare_append(None)  # type: ignore[arg-type]


def test_put_serializes_user_payload_before_writer_transaction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        original = AnnotationDocument.to_dict
        transactions = []

        def checked_to_dict(document: AnnotationDocument) -> dict[str, Any]:
            transactions.append(store._connection.in_transaction)
            return original(document)

        monkeypatch.setattr(AnnotationDocument, "to_dict", checked_to_dict)
        store.put(event())
        assert transactions == [False]
        # Invalid user provenance must not wait for an unrelated writer lock.
        with sqlite3.connect(store.path) as writer:
            writer.execute("BEGIN IMMEDIATE")
            with pytest.raises(AnnotationStoreError, match="provenance"):
                store.put(event(), expected_revision=1, provenance=[])  # type: ignore[arg-type]
            writer.rollback()


def test_actual_parent_descriptor_size_is_checked_before_any_insert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        first = store.put(event())
        prepared = store._prepare_append(event("different text"))
        # Preparation cannot know the CAS parent's digest: only the transaction
        # can enforce the additional 62 bytes of quoted SHA256 versus JSON null.
        limit = len(prepared.render(1, None).encode("utf-8")) + prepared.document_bytes
        before = history_bytes(store._connection)
        with monkeypatch.context() as patch:
            patch.setattr("corpusledger.annotation_store.MAX_EVENT_BYTES", limit)
            with pytest.raises(AnnotationStoreError, match="complete event"), store._transaction(write=True):
                store._append_in_transaction(prepared, expected_revision=1)
        assert history_bytes(store._connection) == before
        assert store.get("event-雪") == first


@pytest.mark.parametrize("failure", ["application", "operation_sql", "conflict"])
def test_composed_event_and_operation_updates_rollback_as_one(tmp_path: Path, failure: str) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        initial = store.put(event())
        store.enable_execution_journal()
        before = history_bytes(store._connection)
        prepared = store._prepare_append(event("new text"), {"operation": "work"})
        expected_error = RuntimeError if failure == "application" else AnnotationStoreError
        with pytest.raises(expected_error), store._transaction(write=True):
            store._connection.execute("INSERT INTO annotation_operations VALUES('work',1,'first','{}')")
            store._append_in_transaction(prepared, expected_revision=0 if failure == "conflict" else 1)
            store._connection.execute("INSERT INTO annotation_operation_events VALUES('work',1,'first',NULL,'{}')")
            if failure == "application":
                raise RuntimeError("injected post-append application failure")
            if failure == "operation_sql":
                store._connection.execute("INSERT INTO annotation_operations VALUES('work',1,'duplicate','{}')")
        assert history_bytes(store._connection) == before
        assert store._connection.execute("SELECT * FROM annotation_operations").fetchall() == []
        assert store._connection.execute("SELECT * FROM annotation_operation_events").fetchall() == []
        assert store.get("event-雪") == initial
        with store._transaction(write=True):
            committed = store._append_in_transaction(prepared, expected_revision=1)
            store._connection.execute("INSERT INTO annotation_operations VALUES('work',1,?,'{}')", (committed.digest,))
            store._connection.execute(
                "INSERT INTO annotation_operation_events VALUES('work',1,?,NULL,'{}')", (committed.digest,)
            )
        assert store.get("event-雪") == committed
        assert store._connection.execute("SELECT digest FROM annotation_operations").fetchone()[0] == committed.digest
        with pytest.raises(AnnotationConflictError), store._transaction(write=True):
            store._append_in_transaction(prepared, expected_revision=1)


def test_closed_store_journal_methods_fail_cleanly(tmp_path: Path) -> None:
    with AnnotationStore(tmp_path / "db") as store:
        prepared = store._prepare_append(event())
    with pytest.raises(AnnotationStoreError, match="closed"):
        _ = store.execution_enabled
    with pytest.raises(AnnotationStoreError, match="closed"):
        store.enable_execution_journal()
    with pytest.raises(AnnotationStoreError, match="closed"):
        store._append_in_transaction(prepared, expected_revision=0)
