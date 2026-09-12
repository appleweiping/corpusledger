"""Explicit v3 DDL rollback and preservation of existing event/execution bytes."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from corpusledger.annotation_attachments import AnnotationAttachments
from corpusledger.annotation_execution import AnnotationExecutor, RemoteAnnotationPipeline
from corpusledger.annotation_pipeline import AnnotationPipeline, AnnotationProcessor
from corpusledger.annotation_protocol import AnnotationResponse, ProcessorDescription
from corpusledger.annotation_remote import RemoteAnnotationProcessor
from corpusledger.annotation_store import AnnotationEvent, AnnotationStore, AnnotationStoreError
from corpusledger.annotations import AnnotationDocument, AnnotationType, SpanAnnotation
from corpusledger.attachment_types import AttachmentError


def rows(store):
    return {
        table: store._connection.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
        for table in ("documents", "revisions", "annotation_operations", "annotation_operation_events")
    }


def initialize(path):
    with AnnotationStore(path) as store:
        store.put(AnnotationEvent("event", (AnnotationDocument("doc", "original"),), {"a": True}))
        store.enable_execution_journal()


def test_ordinary_v1_v2_opens_never_migrate_and_explicit_v3_preserves_raw_history(tmp_path):
    path = tmp_path / "db"
    with AnnotationStore(path) as store:
        original = store.put(AnnotationEvent("event", (AnnotationDocument("doc", "雪"),)))
        with pytest.raises(AnnotationStoreError, match="execution journal"):
            store.enable_attachments()
        assert not store.attachments_enabled
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 1
        store.enable_execution_journal()
        before = rows(store)
    with AnnotationStore(path, create=False) as store:
        assert store.execution_enabled and not store.attachments_enabled
        before_schema = store._connection.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()
        store.enable_attachments()
        store.enable_attachments()
        store.enable_execution_journal()  # Must not downgrade v3.
        assert store.execution_enabled and store.attachments_enabled
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert rows(store) == before
        assert store.get("event") == original
        assert len(store._connection.execute("SELECT sql FROM sqlite_master").fetchall()) > len(before_schema)
    payload = path.read_bytes()
    with sqlite3.connect(path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with AnnotationStore(path, create=False, timeout=0) as reader:
            assert reader.attachments_enabled
            assert reader.get("event") == original
        writer.rollback()
    assert path.read_bytes() == payload


@pytest.mark.parametrize(
    "stage",
    [
        "annotation_blobs",
        "annotation_attachment_receipts",
        "annotation_blobs_no_update",
        "annotation_blobs_no_delete",
        "annotation_attachment_receipts_no_update",
        "annotation_attachment_receipts_no_delete",
        "version",
    ],
)
def test_every_migration_stage_rollback_is_exact(tmp_path, stage):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        before = rows(store)
        schema = store._connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()

        def deny(action, name, value, database, source):
            rejected = (action in (sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_TRIGGER) and name == stage) or (
                stage == "version" and action == sqlite3.SQLITE_PRAGMA and name == "user_version" and value == "3"
            )
            return sqlite3.SQLITE_DENY if rejected else sqlite3.SQLITE_OK

        store._connection.set_authorizer(deny)
        try:
            with pytest.raises(AnnotationStoreError, match="transaction failed"):
                store.enable_attachments()
        finally:
            store._connection.set_authorizer(lambda *_args: sqlite3.SQLITE_OK)
        assert rows(store) == before
        assert not store.attachments_enabled and store.execution_enabled
        assert store._connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall() == schema
        store.enable_attachments()
        assert store.attachments_enabled


def test_post_version_validation_fault_rolls_back_all_ddl(tmp_path, monkeypatch):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        before = rows(store)
        original = store._validate_schema

        def fail():
            value = original()
            if value == 3:
                raise RuntimeError("fault after version change")
            return value

        with monkeypatch.context() as context:
            context.setattr(store, "_validate_schema", fail)
            with pytest.raises(RuntimeError):
                store.enable_attachments()
        assert not store.attachments_enabled and rows(store) == before


def test_two_actual_connections_upgrade_idempotently(tmp_path):
    path = tmp_path / "db"
    initialize(path)
    barrier = threading.Barrier(2)

    def upgrade():
        with AnnotationStore(path, create=False) as store:
            assert not store.attachments_enabled
            barrier.wait(timeout=10)
            store.enable_attachments()
            store.enable_attachments()
            return store.attachments_enabled

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(upgrade) for _ in range(2)]
        assert [future.result(timeout=20) for future in futures] == [True, True]


@pytest.mark.parametrize("action", ["UPDATE", "DELETE"])
@pytest.mark.parametrize("table", ["annotation_blobs", "annotation_attachment_receipts"])
def test_v3_blob_and_receipt_update_delete_guards(tmp_path, action, table):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        store.enable_attachments()
        source = store.get("event")
        AnnotationAttachments(store).attach(
            "event", "raw", b"x", "text/plain", expected_revision=1, expected_digest=source.digest, command_id="a"
        )
        statement = f"UPDATE {table} SET body=body" if action == "UPDATE" else f"DELETE FROM {table}"
        with pytest.raises(AnnotationStoreError, match="transaction failed"), store._transaction(write=True):
            store._connection.execute(statement)


@pytest.mark.parametrize("change", ["version", "extra_column", "missing_guard"])
def test_v3_wrong_version_layout_or_guard_rejected_on_open(tmp_path, change):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        store.enable_attachments()
    with sqlite3.connect(path) as database:
        if change == "version":
            database.execute("PRAGMA user_version=2")
        elif change == "extra_column":
            database.execute("ALTER TABLE annotation_blobs ADD COLUMN path TEXT")
        else:
            database.execute("DROP TRIGGER annotation_blobs_no_update")
    with pytest.raises(AnnotationStoreError), AnnotationStore(path, create=False):
        pass


def test_local_processing_preserves_manifest_and_empty_v2_identity(tmp_path):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        store.enable_attachments()
        first = store.get("event")
        manager = AnnotationAttachments(store)
        attached = manager.attach(
            "event",
            "raw",
            b"\x00\xff",
            "application/octet-stream",
            expected_revision=1,
            expected_digest=first.digest,
            command_id="a",
        )
        token = AnnotationType("token")
        processor = AnnotationProcessor(
            "p", "1", lambda doc: (SpanAnnotation("token", "token", 0, 1),), produces=(token,)
        )
        output = store.process("event", {"doc": AnnotationPipeline((processor,))}, expected_revision=2)
        assert output.event.attachments == attached.event.attachments and output.event.version == 2
        assert manager.read("event", "raw", revision=3) == b"\x00\xff"
        detached = manager.detach("event", "raw", expected_revision=3, expected_digest=output.digest, command_id="d")
        other = AnnotationType("other")
        processor2 = AnnotationProcessor("q", "1", lambda doc: (), produces=(other,))
        processed = store.process("event", {"doc": AnnotationPipeline((processor2,))}, expected_revision=4)
        assert processed.event.version == detached.event.version == 2
        assert processed.event.attachments == ()


def test_remote_execution_after_migration_preserves_manifest_and_old_journal_bytes(tmp_path, monkeypatch):
    path = tmp_path / "db"
    initialize(path)
    token = AnnotationType("token")
    description = ProcessorDescription("p", "1", "a" * 64, produces=(token,))
    worker = RemoteAnnotationProcessor("http://127.0.0.1:9999", description, "TEST_ATTACHMENT_KEY")
    pipelines = {"doc": RemoteAnnotationPipeline("pipeline", "1", (worker,))}
    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", lambda worker: None)

    def answer(worker, request):
        return AnnotationResponse(
            request.operation_id,
            request.step_id,
            request.processor,
            request.document.digest,
            (SpanAnnotation("token", "token", 0, 1),),
            1,
        )

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", answer)
    with AnnotationStore(path, create=False) as store:
        executor = AnnotationExecutor(store)
        initial = store.get("event")
        reserved = executor.begin(
            "old-operation", "event", pipelines, expected_revision=1, expected_digest=initial.digest
        )
        before = rows(store)
        store.enable_attachments()
        assert rows(store) == before and executor.get("old-operation") == reserved
        attached = AnnotationAttachments(store).attach(
            "event",
            "raw",
            b"raw",
            "text/plain",
            expected_revision=1,
            expected_digest=initial.digest,
            command_id="attach",
        )
        from corpusledger._annotation_journal import AnnotationExecutionConflict

        with pytest.raises(AnnotationExecutionConflict):
            executor.resume("old-operation", pipelines)
        executor.begin("new-operation", "event", pipelines, expected_revision=2, expected_digest=attached.digest)
        completed = executor.resume("new-operation", pipelines)
        assert completed.status == "committed"
        final = store.get("event")
        assert final.event.attachments == attached.event.attachments and final.event.version == 2
        assert AnnotationAttachments(store).read("event", "raw", revision=3) == b"raw"
        assert executor.get("new-operation") == completed
        # Old prefix bytes still match exactly; the original operation only appends successors.
        old_first = store._connection.execute(
            "SELECT body FROM annotation_operation_events WHERE operation_id='old-operation' AND version=1"
        ).fetchone()[0]
        assert old_first == before["annotation_operation_events"][0][-1]


def test_v2_descriptor_has_independently_recomputed_digest(tmp_path):
    path = tmp_path / "db"
    initialize(path)
    with AnnotationStore(path, create=False) as store:
        store.enable_attachments()
        source = store.get("event")
        output = AnnotationAttachments(store).attach(
            "event", "raw", b"x", "text/plain", expected_revision=1, expected_digest=source.digest, command_id="a"
        )
        actual = store._connection.execute("SELECT body FROM revisions WHERE revision=2").fetchone()[0]
        expected = {
            "format": "corpusledger.annotation-store.v2",
            "event_id": "event",
            "revision": 2,
            "parent_digest": source.digest,
            "metadata": {"a": True},
            "documents": [{"id": "doc", "digest": source.event.documents[0].digest}],
            "attachments": [item.to_dict() for item in output.event.attachments],
            "provenance": output.to_dict()["provenance"],
        }
        encoded = json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        assert actual == encoded and output.digest == hashlib.sha256(encoded.encode()).hexdigest()
        with pytest.raises(AttachmentError):
            AnnotationAttachments(store, limits={})
