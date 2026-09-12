"""Local binary lifecycle, exclusive publication and post-commit output failures."""

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from corpusledger import AnnotationDocument, AnnotationEvent, AnnotationStore, InputError
from corpusledger import annotation_attachment_cli as cli
from corpusledger.annotation_attachment_snapshot import AnnotationAttachmentSnapshot
from corpusledger.annotation_attachments import AnnotationAttachments


def invoke(*arguments):
    parser = argparse.ArgumentParser()
    cli.configure_annotation_attachment_parser(parser)
    return cli.run_annotation_attachment_command(parser.parse_args([str(item) for item in arguments]))


def create(path):
    with AnnotationStore(path) as store:
        initial = store.put(AnnotationEvent("original", (AnnotationDocument("text", "Alpha\r\nβ😀"),)))
    return initial


def upgrade(path):
    with AnnotationStore(path, create=False) as store:
        store.enable_execution_journal()
        store.enable_attachments()


def test_full_local_binary_lifecycle_pinned_export_import_and_explicit_retry(tmp_path, capsys):
    database = tmp_path / "events.db"
    initial = create(database)
    original_database = database.read_bytes()
    with pytest.raises(InputError):
        invoke("enable", database)
    assert database.read_bytes() == original_database
    assert invoke("enable-execution", database) == 0
    assert invoke("enable", database) == 0
    capsys.readouterr()
    binary = bytes(range(256)) + b"\x00\xff\xfe\n"
    source = tmp_path / "blob.bin"
    source.write_bytes(binary)
    attach = (
        "attach",
        database,
        "original",
        "sample",
        source,
        "--expected-revision",
        1,
        "--expected-digest",
        initial.digest,
        "--command-id",
        "original-upload",
    )
    assert invoke(*attach) == 0
    added = json.loads(capsys.readouterr().out)
    assert added["committed"] is True and added["revision"] == 2
    pinned = ("--revision", 2, "--expected-digest", added["digest"])
    assert invoke("list", database, "original", *pinned) == 0
    listing = json.loads(capsys.readouterr().out)
    assert listing["attachments"][0]["size"] == len(binary)
    output = tmp_path / "exported.bin"
    assert invoke("get", database, "original", "sample", *pinned, "--output", output) == 0
    assert output.read_bytes() == binary
    snapshot_path = tmp_path / "snapshot.json"
    assert invoke("export", database, "original", *pinned, "--output", snapshot_path) == 0
    snapshot = AnnotationAttachmentSnapshot.from_dict(json.loads(snapshot_path.read_bytes()))
    destination = tmp_path / "destination.db"
    with AnnotationStore(destination) as store:
        store.enable_execution_journal()
        store.enable_attachments()
    assert (
        invoke(
            "import",
            destination,
            snapshot_path,
            "--snapshot-digest",
            snapshot.digest,
            "--expected-revision",
            0,
            "--command-id",
            "snapshot-import",
        )
        == 0
    )
    imported = json.loads(capsys.readouterr().out)
    with AnnotationStore(destination, create=False) as store:
        assert AnnotationAttachments(store).read("original", "sample", revision=1) == binary
        assert store.get("original").digest == imported["digest"]
    assert (
        invoke(
            "detach",
            database,
            "original",
            "sample",
            "--expected-revision",
            2,
            "--expected-digest",
            added["digest"],
            "--command-id",
            "remove-name",
        )
        == 0
    )
    removed = json.loads(capsys.readouterr().out)
    assert removed["revision"] == 3
    assert invoke(*attach) == 0
    assert json.loads(capsys.readouterr().out) == added
    with AnnotationStore(database, create=False) as store:
        assert store.get("original").revision == 3
        assert not store.get("original").event.attachments
        assert store.get("original").event.version == 2
        assert store.get("original", 1).digest == initial.digest
        assert AnnotationAttachments(store).read("original", "sample", revision=2) == binary


@pytest.mark.parametrize("target", ["database", "sidecar", "source", "existing", "directory"])
def test_export_paths_reject_existing_or_protected_targets_before_store_open(tmp_path, monkeypatch, target):
    database = tmp_path / "events.db"
    create(database)
    source = tmp_path / "source.bin"
    source.write_bytes(b"source")
    existing = tmp_path / "existing.bin"
    existing.write_bytes(b"other output")
    paths = {
        "database": database,
        "sidecar": Path(str(database) + "-wal"),
        "source": source,
        "existing": existing,
        "directory": tmp_path,
    }
    monkeypatch.setattr(cli, "AnnotationStore", lambda *a, **k: pytest.fail("must reject before opening SQLite"))
    with pytest.raises(InputError):
        invoke(
            "get",
            database,
            "original",
            "sample",
            "--revision",
            1,
            "--expected-digest",
            "a" * 64,
            "--output",
            paths[target],
        )
    assert existing.read_bytes() == b"other output"


def test_missing_database_and_oversized_or_aliased_inputs_do_not_write(tmp_path):
    missing = tmp_path / "missing.db"
    with pytest.raises(InputError):
        invoke("enable-execution", missing)
    assert not missing.exists()
    database = tmp_path / "events.db"
    first = create(database)
    upgrade(database)
    source = tmp_path / "input.bin"
    source.write_bytes(b"12345")
    before = database.read_bytes()
    options = ("--expected-revision", 1, "--expected-digest", first.digest, "--command-id", "one")
    with pytest.raises(InputError):
        invoke("attach", database, "original", "blob", source, *options, "--max-blob-bytes", 4)
    alias = tmp_path / "database-hardlink"
    os.link(database, alias)
    with pytest.raises(InputError):
        invoke("attach", database, "original", "blob", alias, *options)
    assert database.read_bytes() == before


@pytest.mark.parametrize("raw", [b'{"secret":1,"secret":2}', b"NaN", b'"\xff"', b"{}"])
def test_snapshot_parse_failure_is_private_and_does_not_mutate(tmp_path, raw):
    database = tmp_path / "events.db"
    create(database)
    upgrade(database)
    source = tmp_path / "snapshot.json"
    source.write_bytes(raw)
    before = database.read_bytes()
    with pytest.raises(InputError, match="invalid attachment snapshot") as error:
        invoke(
            "import", database, source, "--snapshot-digest", "a" * 64, "--expected-revision", 0, "--command-id", "one"
        )
    assert "secret" not in str(error.value)
    assert database.read_bytes() == before


def test_wrong_pinned_digest_never_publishes_bytes(tmp_path):
    database = tmp_path / "events.db"
    create(database)
    upgrade(database)
    output = tmp_path / "out.bin"
    with pytest.raises(InputError, match="pinned"):
        invoke(
            "get", database, "original", "sample", "--revision", 1, "--expected-digest", "a" * 64, "--output", output
        )
    assert not output.exists()


def test_racing_export_never_overwrites_and_cleanup_failure_reports_publication(tmp_path, monkeypatch, capsys):
    output = tmp_path / "output"

    def race(source, destination):
        destination.write_bytes(b"other")
        raise FileExistsError("PRIVATE PATH")

    with monkeypatch.context() as patch:
        patch.setattr(cli.os, "link", race)
        with pytest.raises(InputError):
            cli._publish(b"ours", output)
    assert output.read_bytes() == b"other"
    assert not list(tmp_path.glob(".corpusledger-attachment-*"))
    original_unlink = os.unlink
    private = []

    def unavailable(path, *args, **kwargs):
        if Path(path).name.startswith(".corpusledger-attachment-"):
            private.append(path)
            raise PermissionError("PRIVATE PATH")
        return original_unlink(path, *args, **kwargs)

    second = tmp_path / "published"
    try:
        with monkeypatch.context() as patch:
            patch.setattr(cli.os, "unlink", unavailable)
            cli._publish(b"complete", second)
        assert second.read_bytes() == b"complete"
        diagnostic = capsys.readouterr().err
        assert "published" in diagnostic and "private temporary copy" in diagnostic
        assert "PRIVATE PATH" not in diagnostic
    finally:
        for path in private:
            original_unlink(path)


@pytest.mark.parametrize("returned", [None, 0, 1])
def test_short_stdout_reports_failure_but_preserves_committed_success(monkeypatch, capsys, returned):
    writes = []
    output = SimpleNamespace(write=lambda text: writes.append(text) or returned, flush=lambda: None)
    with monkeypatch.context() as patch:
        patch.setattr(sys, "stdout", output)
        with pytest.raises(InputError):
            cli._stdout({"read": True})
        cli._stdout({"write": True}, committed=True)
    assert len(writes) == 2
    assert "transaction committed" in capsys.readouterr().err


def test_closed_summary_stream_cannot_undo_a_committed_attachment(tmp_path, monkeypatch, capsys):
    database = tmp_path / "events.db"
    first = create(database)
    upgrade(database)
    source = tmp_path / "empty.bin"
    source.write_bytes(b"")
    parser = argparse.ArgumentParser()
    cli.configure_annotation_attachment_parser(parser)
    args = parser.parse_args(
        [
            "attach",
            str(database),
            "original",
            "empty",
            str(source),
            "--expected-revision",
            "1",
            "--expected-digest",
            first.digest,
            "--command-id",
            "empty-upload",
        ]
    )
    closed = io.StringIO()
    closed.close()
    with monkeypatch.context() as patch:
        patch.setattr(sys, "stdout", closed)
        assert cli.run_annotation_attachment_command(args) == 0
    assert "transaction committed" in capsys.readouterr().err
    with AnnotationStore(database, create=False) as store:
        assert store.get("original").revision == 2
        assert AnnotationAttachments(store).read("original", "empty", revision=2) == b""


def test_actual_public_cli_reopens_store_and_exports_exact_binary(tmp_path):
    database = tmp_path / "events.db"
    first = create(database)
    source = tmp_path / "authored.bin"
    data = bytes(range(256)) * 17 + b"\x00\xff"
    source.write_bytes(data)

    def command(*args):
        result = subprocess.run(
            [sys.executable, "-m", "corpusledger", "annotations-attachments", *(str(item) for item in args)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout) if result.stdout.strip() else None

    command("enable-execution", database)
    command("enable", database)
    saved = command(
        "attach",
        database,
        "original",
        "binary",
        source,
        "--expected-revision",
        1,
        "--expected-digest",
        first.digest,
        "--command-id",
        "real-upload",
    )
    assert saved["revision"] == 2 and saved["committed"] is True
    restored = tmp_path / "restored.bin"
    assert (
        command(
            "get",
            database,
            "original",
            "binary",
            "--revision",
            2,
            "--expected-digest",
            saved["digest"],
            "--output",
            restored,
        )
        is None
    )
    assert restored.read_bytes() == data
    assert (
        command(
            "attach",
            database,
            "original",
            "binary",
            source,
            "--expected-revision",
            1,
            "--expected-digest",
            first.digest,
            "--command-id",
            "real-upload",
        )
        == saved
    )


def test_closed_actual_output_pipe_keeps_successful_attachment_exit_zero(tmp_path):
    database = tmp_path / "events.db"
    first = create(database)
    upgrade(database)
    source = tmp_path / "input.bin"
    source.write_bytes(b"authored binary\x00\xff")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "corpusledger",
            "annotations-attachments",
            "attach",
            str(database),
            "original",
            "binary",
            str(source),
            "--expected-revision",
            "1",
            "--expected-digest",
            first.digest,
            "--command-id",
            "closed-pipe-upload",
        ],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    process.stdout.close()
    process.stdout = None
    try:
        _output, diagnostic = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
    assert process.returncode == 0, diagnostic.decode("utf-8", errors="replace")
    assert b"transaction committed" in diagnostic and b"Exception ignored" not in diagnostic
    with AnnotationStore(database, create=False) as store:
        assert store.get("original").revision == 2
        assert AnnotationAttachments(store).read("original", "binary", revision=2) == source.read_bytes()
