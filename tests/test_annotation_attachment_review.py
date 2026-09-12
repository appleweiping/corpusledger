"""Independent attachment corruption admission and postcommit publication probes."""

from __future__ import annotations

import argparse
import io
import json
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from corpusledger import annotation_attachment_cli as cli
from corpusledger.annotation_attachments import AnnotationAttachments, AttachmentCorruptionError
from corpusledger.annotation_store import _ATTACHMENT_GUARDS, AnnotationEvent, AnnotationStore
from corpusledger.annotations import AnnotationDocument
from corpusledger.errors import InputError


@pytest.fixture
def store(tmp_path: Path) -> Iterator[AnnotationStore]:
    with AnnotationStore(tmp_path / "review.sqlite") as opened:
        opened.put(AnnotationEvent("event"))
        opened.enable_execution_journal()
        opened.enable_attachments()
        yield opened


@pytest.mark.parametrize("absent_stderr", [False, True])
def test_unavailable_host_streams_do_not_turn_committed_attach_into_failure(
    store: AnnotationStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, absent_stderr: bool
) -> None:
    source = store.get("event")
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"\x00\xff")
    parser = argparse.ArgumentParser()
    cli.configure_annotation_attachment_parser(parser)
    args = parser.parse_args(
        [
            "attach",
            str(store.path),
            "event",
            "payload",
            str(payload),
            "--expected-revision",
            "1",
            "--expected-digest",
            source.digest,
            "--command-id",
            "upload",
        ]
    )
    # Model no-console embedding after parsing, not argparse's stream probing.
    closed = io.StringIO()
    closed.close()
    with monkeypatch.context() as patch:
        patch.setattr(sys, "stdout", closed if absent_stderr else None)
        if absent_stderr:
            patch.setattr(sys, "stderr", None)
        assert cli.run_annotation_attachment_command(args) == 0
    result = store.get("event")
    assert result.revision == 2
    assert AnnotationAttachments(store).read("event", "payload", revision=2) == b"\x00\xff"


def forbid_unadmitted_digest_materialization(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Small sentinel: don't return a corrupt would-be huge digest column.

    SQLite-side typeof/length admission returns scalar metadata and is permitted.
    Only 513 bytes are stored: this is an ordering test, not an allocation attack.
    """
    for description, value in zip(cursor.description, row, strict=True):
        if description[0] in ("sha256", "request_digest") and isinstance(value, str) and len(value) > 64:
            raise AssertionError("corrupt digest value reached the Python materialization boundary before admission")
    return row


def test_blob_digest_metadata_is_bounded_before_selecting_raw_column(store: AnnotationStore) -> None:
    source = store.get("event")
    store._connection.execute("INSERT INTO annotation_blobs(sha256,size,body) VALUES(?,?,?)", ("x" * 513, 1, b"a"))
    manager = AnnotationAttachments(store)
    store._connection.row_factory = forbid_unadmitted_digest_materialization
    try:
        with pytest.raises(AttachmentCorruptionError):
            manager.attach(
                "event",
                "valid",
                b"b",
                "text/plain",
                expected_revision=1,
                expected_digest=source.digest,
                command_id="next",
            )
    finally:
        store._connection.row_factory = None
    assert store.get("event").revision == 1
    assert store._connection.execute("SELECT count(*) FROM annotation_attachment_receipts").fetchone()[0] == 0


def test_receipt_digest_metadata_is_bounded_before_selecting_raw_column(store: AnnotationStore) -> None:
    manager = AnnotationAttachments(store)
    source = store.get("event")
    manager.attach(
        "event", "data", b"a", "text/plain", expected_revision=1, expected_digest=source.digest, command_id="upload"
    )
    # This disposable DB fault deliberately bypasses, then restores, the exact
    # append-only guard. Normal API writes cannot produce the corrupt text.
    store._connection.execute("DROP TRIGGER annotation_attachment_receipts_no_update")
    store._connection.execute("UPDATE annotation_attachment_receipts SET request_digest=?", ("x" * 513,))
    store._connection.execute(_ATTACHMENT_GUARDS["annotation_attachment_receipts_no_update"])
    store._connection.row_factory = forbid_unadmitted_digest_materialization
    try:
        with pytest.raises(AttachmentCorruptionError):
            manager.receipt("upload")
    finally:
        store._connection.row_factory = None
    assert store.get("event").revision == 2


def test_cli_stale_cas_diagnostic_does_not_echo_private_event_id(store: AnnotationStore, tmp_path: Path) -> None:
    identifier = "PRIVATE-PARTICIPANT-NOT-FOR-LOGS"
    initial = store.put(AnnotationEvent(identifier))
    AnnotationAttachments(store).attach(
        identifier,
        "first",
        b"a",
        "text/plain",
        expected_revision=1,
        expected_digest=initial.digest,
        command_id="first",
    )
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"b")
    parser = argparse.ArgumentParser()
    cli.configure_annotation_attachment_parser(parser)
    args = parser.parse_args(
        [
            "attach",
            str(store.path),
            identifier,
            "second",
            str(payload),
            "--expected-revision",
            "1",
            "--expected-digest",
            initial.digest,
            "--command-id",
            "stale",
        ]
    )
    with pytest.raises(InputError) as caught:
        cli.run_annotation_attachment_command(args)
    assert identifier not in str(caught.value)
    assert caught.value.__suppress_context__
    assert store.get(identifier).revision == 2


def test_cli_corrupt_annotation_diagnostic_does_not_echo_private_type(store: AnnotationStore, tmp_path: Path) -> None:
    secret_type = "PRIVATE-ANNOTATION-TYPE-NOT-FOR-LOGS"
    original = store.put(AnnotationEvent("with-document", (AnnotationDocument("doc", "x"),)))
    row = store._connection.execute("SELECT digest,body FROM documents").fetchone()
    body = json.loads(row[1])
    body["annotations"] = [{"id": "annotation", "type": secret_type, "start": 0, "end": 1, "features": {}}]
    # Temporary database corruption reaches typed validation before its final
    # content hash comparison; diagnostics must not echo the corrupt type name.
    store._connection.execute("UPDATE documents SET body=? WHERE digest=?", (json.dumps(body), row[0]))
    parser = argparse.ArgumentParser()
    cli.configure_annotation_attachment_parser(parser)
    args = parser.parse_args(
        [
            "get",
            str(store.path),
            "with-document",
            "absent",
            "--revision",
            "1",
            "--expected-digest",
            original.digest,
            "--output",
            str(tmp_path / "output.bin"),
        ]
    )
    with pytest.raises(InputError) as caught:
        cli.run_annotation_attachment_command(args)
    assert secret_type not in str(caught.value)
    assert caught.value.__suppress_context__
    assert not (tmp_path / "output.bin").exists()
