from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationDocument, AnnotationField, AnnotationType, InputError, SpanAnnotation
from corpusledger.annotation_store import AnnotationEvent, AnnotationStore
from corpusledger.cli import run


def _event(event_id: str = "visit:1", *, stage: str = "source") -> AnnotationEvent:
    text = "Hi 😀!\r\nCafé e\u0301."
    schema = AnnotationType("mention", {"label": AnnotationField()})
    document = AnnotationDocument(
        "original", text, (schema,), (SpanAnnotation("emoji", "mention", 3, 4, {"label": "symbol"}),)
    )
    return AnnotationEvent(event_id, (document, AnnotationDocument("translation", "Bonjour !")), {"stage": stage})


def _source(path: Path, event: AnnotationEvent | None = None) -> Path:
    path.write_text(json.dumps((event or _event()).to_dict(), ensure_ascii=False), encoding="utf-8")
    return path


def _invoke(action: str, database: Path, *args: str) -> int:
    return run(["annotations-store", action, str(database), *args])


def test_event_revision_export_import_and_document_workflow(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    original = source.read_bytes()
    assert _invoke("put", database, str(source)) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["revision"] == 1 and AnnotationEvent.from_dict(first["event"]) == _event()
    _source(source, _event(stage="reviewed"))
    assert _invoke("put", database, str(source), "--expected-revision", "1") == 0
    second = json.loads(capsys.readouterr().out)
    assert second["revision"] == 2 and second["parent_digest"] == first["digest"]

    old = tmp_path / "old.json"
    assert _invoke("get", database, "visit:1", "--revision", "1", "--output", str(old)) == 0
    assert AnnotationEvent.from_dict(json.loads(old.read_text("utf-8"))) == _event()
    assert json.loads(original) == json.loads(old.read_text("utf-8"))
    document = tmp_path / "document.json"
    assert _invoke("get", database, "visit:1", "--document", "original", "--output", str(document)) == 0
    imported = AnnotationDocument.from_dict(json.loads(document.read_text("utf-8")))
    assert imported == _event().get_document("original")
    assert imported.span_text("emoji") == "😀" and "\r\n" in imported.text
    assert _invoke("get", database, "visit:1") == 0
    assert AnnotationEvent.from_dict(json.loads(capsys.readouterr().out)) == _event(stage="reviewed")

    replica = tmp_path / "replica.db"
    assert _invoke("put", replica, str(old)) == 0
    copied = json.loads(capsys.readouterr().out)
    assert copied["revision"] == 1 and copied["digest"] == first["digest"]
    assert _invoke("verify", database) == 0
    with AnnotationStore(database, create=False) as store:
        assert json.loads(capsys.readouterr().out) == store.verify().to_dict()
        assert store.get("visit:1").event == _event(stage="reviewed")


def test_listing_and_history_are_bounded_cursor_pages(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    database = tmp_path / "events.db"
    with AnnotationStore(database) as store:
        for event_id in ("c", "a", "b"):
            store.put(_event(event_id))
        store.put(_event("a", stage="reviewed"), expected_revision=1)
        store.put(_event("a", stage="complete"), expected_revision=2)
    assert _invoke("list", database, "--limit", "2") == 0
    first = json.loads(capsys.readouterr().out)
    with AnnotationStore(database, create=False) as store:
        assert first["events"] == [row.to_dict() for row in store.list(limit=2)]
    assert first["after_event_id"] is None and first["last_event_id"] == "b"
    assert _invoke("list", database, "--limit", "2", "--after-event-id", first["last_event_id"]) == 0
    page = json.loads(capsys.readouterr().out)
    assert len(page["events"]) == 1 and page["last_event_id"] == "c"
    assert _invoke("list", database, "--after-event-id", "c") == 0
    page = json.loads(capsys.readouterr().out)
    assert page["events"] == [] and page["last_event_id"] is None

    assert _invoke("history", database, "a", "--limit", "2") == 0
    history = json.loads(capsys.readouterr().out)
    assert [row["revision"] for row in history["revisions"]] == [1, 2]
    assert history["last_revision"] == 2 and history["after_revision"] == 0
    output = tmp_path / "audit" / "history.json"
    assert _invoke("history", database, "a", "--after-revision", "2", "--output", str(output)) == 0
    history = json.loads(output.read_text("utf-8"))
    assert [row["revision"] for row in history["revisions"]] == [3]
    assert _invoke("history", database, "a", "--after-revision", "3") == 0
    history = json.loads(capsys.readouterr().out)
    assert history["revisions"] == [] and history["last_revision"] is None
    assert _invoke("verify", database, "--event-id", "a") == 0
    with AnnotationStore(database, create=False) as store:
        assert json.loads(capsys.readouterr().out) == store.verify("a").to_dict()


def test_conflicts_and_lookup_errors_preserve_prior_output_and_history(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    with AnnotationStore(database) as store:
        store.put(_event())
    output = tmp_path / "output.json"
    output.write_text("prior", encoding="utf-8")
    cases = (
        ("put", str(source)),
        ("put", str(source), "--expected-revision", "2"),
        ("get", "missing"),
        ("get", "visit:1", "--revision", "2"),
        ("get", "visit:1", "--document", "missing"),
        ("get", "visit:1", "--revision", "0"),
        ("history", "visit:1", "--after-revision", "-1"),
        ("list", "--limit", "0"),
    )
    for action, *args in cases:
        with pytest.raises(InputError):
            _invoke(action, database, *args, "--output", str(output))
        assert output.read_text("utf-8") == "prior"
    with AnnotationStore(database, create=False) as store:
        assert len(store.history("visit:1")) == 1


@pytest.mark.parametrize("action,extra", [("list", []), ("get", ["e"]), ("history", ["e"]), ("verify", [])])
def test_reads_do_not_create_missing_databases(tmp_path: Path, action: str, extra: list[str]) -> None:
    database = tmp_path / "missing.db"
    with pytest.raises(InputError):
        _invoke(action, database, *extra)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("content", ['{"id":"a","id":"b"}', "NaN", "Infinity", "1e400", "[]", "{", "[" * 1100])
def test_strict_invalid_json_fails_before_database_creation(tmp_path: Path, content: str) -> None:
    source = tmp_path / "bad.json"
    source.write_text(content, encoding="utf-8")
    database = tmp_path / "events.db"
    output = tmp_path / "output.json"
    output.write_text("prior", encoding="utf-8")
    with pytest.raises(InputError):
        _invoke("put", database, str(source), "--output", str(output))
    assert not database.exists() and output.read_text("utf-8") == "prior"


def test_nested_document_validation_precedes_store_mutation(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    with AnnotationStore(database) as store:
        store.put(_event())
    payload = _event(stage="invalid").to_dict()
    payload["documents"][0]["annotations"][0]["end"] = 1000
    source = tmp_path / "invalid.json"
    source.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(InputError, match="length"):
        _invoke("put", database, str(source), "--expected-revision", "1")
    with AnnotationStore(database, create=False) as store:
        assert store.get("visit:1").event == _event()
        assert len(store.history("visit:1")) == 1


def test_input_encoding_and_size_limits_do_not_create_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = tmp_path / "events.db"
    source = tmp_path / "source.json"
    with pytest.raises(InputError, match="cannot read"):
        _invoke("put", database, str(source))
    source.write_bytes(b"\xff")
    with pytest.raises(InputError, match="UTF-8"):
        _invoke("put", database, str(source))
    _source(source)
    monkeypatch.setattr("corpusledger.annotation_store_cli.MAX_EVENT_BYTES", 3)
    with pytest.raises(InputError, match="input exceeds 3"):
        _invoke("put", database, str(source))
    assert not database.exists()


def test_invalid_expected_revision_does_not_create_store(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    for revision in ("-1", str(2**63 - 1)):
        with pytest.raises(InputError, match="expected revision"):
            _invoke("put", database, str(source), "--expected-revision", revision)
        assert not database.exists()


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", "-journal"])
def test_input_reserved_path_and_hardlink_are_rejected_before_open(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "events.db"
    reserved = _source(Path(str(database) + suffix))
    hardlink = tmp_path / "source.json"
    os.link(reserved, hardlink)
    before = reserved.read_bytes()
    for source in (reserved, hardlink):
        with pytest.raises(InputError, match="input must not alias"):
            _invoke("put", database, str(source))
        assert reserved.read_bytes() == hardlink.read_bytes() == before
    if suffix:
        assert not database.exists()


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", "-journal"])
def test_put_reserved_output_paths_rejected_even_before_sidecars_exist(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    with pytest.raises(InputError, match="output must not overwrite or alias"):
        _invoke("put", database, str(source), "--output", str(database) + suffix)
    assert not database.exists() and AnnotationEvent.from_dict(json.loads(source.read_text("utf-8"))) == _event()


@pytest.mark.parametrize("action,extra", [("list", []), ("get", ["visit:1"]), ("history", ["visit:1"]), ("verify", [])])
def test_read_outputs_cannot_alias_database(tmp_path: Path, action: str, extra: list[str]) -> None:
    database = tmp_path / "events.db"
    with AnnotationStore(database) as store:
        store.put(_event())
    alias = tmp_path / "hardlink.json"
    os.link(database, alias)
    before = database.read_bytes()
    for output in (database, alias, Path(str(database) + "-wal")):
        with pytest.raises(InputError, match="output must not overwrite or alias"):
            _invoke(action, database, *extra, "--output", str(output))
        assert database.read_bytes() == alias.read_bytes() == before


def test_input_output_hardlink_and_database_sidecar_aliases_are_rejected(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    output = tmp_path / "alias.json"
    os.link(source, output)
    for destination in (source, output):
        with pytest.raises(InputError, match="output must not overwrite or alias"):
            _invoke("put", database, str(source), "--output", str(destination))
    assert not database.exists()
    with AnnotationStore(database) as store:
        store.put(_event())
    sidecar = Path(str(database) + "-journal")
    os.link(database, sidecar)
    before = database.read_bytes()
    with pytest.raises(InputError, match="sidecars must not alias each other"):
        _invoke("verify", database)
    assert database.read_bytes() == sidecar.read_bytes() == before


def test_path_resolution_and_stat_failures_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = _source(tmp_path / "source.json")

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError("denied")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "resolve", fail)
        with pytest.raises(InputError, match="cannot resolve/check"):
            _invoke("put", tmp_path / "events.db", str(source))
    monkeypatch.setattr(Path, "stat", fail)
    with pytest.raises(InputError, match="cannot resolve/check"):
        _invoke("put", tmp_path / "events.db", str(source))


def test_directory_output_rejected_before_put(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    with pytest.raises(InputError, match="not a directory"):
        _invoke("put", database, str(source), "--output", str(tmp_path))
    assert not database.exists()


def test_output_failure_is_atomic_but_does_not_undo_committed_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "events.db"
    source = _source(tmp_path / "source.json")
    output = tmp_path / "report.json"
    output.write_text("prior", encoding="utf-8")

    def fail(*args: Any) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr("corpusledger.annotation_cli.os.replace", fail)
    with pytest.raises(InputError, match="cannot write"):
        _invoke("put", database, str(source), "--output", str(output))
    assert output.read_text("utf-8") == "prior" and list(tmp_path.glob(".*.tmp")) == []
    with AnnotationStore(database, create=False) as store:
        assert store.get("visit:1").event == _event()
        assert len(store.history("visit:1")) == 1


def test_export_size_failure_preserves_existing_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = tmp_path / "events.db"
    with AnnotationStore(database) as store:
        store.put(_event())
    output = tmp_path / "event.json"
    output.write_text("prior", encoding="utf-8")
    monkeypatch.setattr("corpusledger.annotation_cli.MAX_DOCUMENT_BYTES", 3)
    with pytest.raises(InputError, match="output exceeds 3"):
        _invoke("get", database, "visit:1", "--output", str(output))
    assert output.read_text("utf-8") == "prior"
