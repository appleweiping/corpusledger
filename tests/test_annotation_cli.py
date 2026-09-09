from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationDocument, AnnotationType, InputError, SpanAnnotation
from corpusledger.cli import run


def test_full_cli_workflow_preserves_unicode_and_newlines(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    text = "Hello 😀!\r\nCafé e\u0301."
    source = tmp_path / "text.txt"
    source.write_bytes(text.encode("utf-8"))
    empty = tmp_path / "empty.json"
    annotated = tmp_path / "tokens.json"
    assert run(["annotations", "create", str(source), str(empty), "--id", "d"]) == 0
    assert run(["annotations", "tokenize", str(empty), str(annotated)]) == 0
    doc = AnnotationDocument.from_dict(json.loads(annotated.read_text("utf-8")))
    assert doc.text == text and doc.document_id == "d"
    assert [(a.start, a.end, doc.span_text(a.annotation_id)) for a in doc.annotations] == [
        (0, 5, "Hello"),
        (6, 7, "😀"),
        (7, 8, "!"),
        (10, 14, "Café"),
        (15, 16, "e"),
        (16, 17, "\u0301"),
        (17, 18, "."),
    ]
    assert run(["annotations", "validate", str(annotated)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["valid"] and report["codepoints"] == 18 and report["annotations"] == 7
    assert report["digest"] == doc.digest
    query = tmp_path / "query.json"
    assert run(["annotations", "query", str(annotated), "6", "8", "--type", "token", "--output", str(query)]) == 0
    values = json.loads(query.read_text("utf-8"))
    assert [a["id"] for a in values["annotations"]] == ["token:1", "token:2"]
    assert values["document_digest"] == doc.digest
    assert source.read_bytes() == text.encode("utf-8")
    assert AnnotationDocument.from_dict(json.loads(empty.read_text("utf-8"))).annotations == ()


@pytest.mark.parametrize("action", ["create", "validate", "query", "tokenize"])
def test_source_aliases_rejected_for_all_commands(tmp_path: Path, action: str) -> None:
    source = tmp_path / "data.json"
    source.write_text(json.dumps(AnnotationDocument("d", "hello").to_dict()), encoding="utf-8")
    hardlink = tmp_path / "hardlink.json"
    os.link(source, hardlink)
    before = source.read_bytes()
    for destination in (source, hardlink):
        args = ["annotations", action, str(source)]
        if action == "create":
            args += [str(destination), "--id", "d"]
        elif action == "tokenize":
            args += [str(destination)]
        else:
            if action == "query":
                args += ["0", "1"]
            args += ["--output", str(destination)]
        with pytest.raises(InputError, match="alias input"):
            run(args)
        assert source.read_bytes() == hardlink.read_bytes() == before


@pytest.mark.parametrize("content", ['{"id":"a","id":"b"}', "NaN", "Infinity", "1e400", "[]", "{", "[" * 1100])
def test_malformed_input_keeps_prior_output(tmp_path: Path, content: str) -> None:
    source = tmp_path / "bad.json"
    source.write_text(content, encoding="utf-8")
    output = tmp_path / "report.json"
    output.write_text("prior", encoding="utf-8")
    with pytest.raises(InputError):
        run(["annotations", "validate", str(source), "--output", str(output)])
    assert output.read_text("utf-8") == "prior"


def test_input_read_limits_and_encoding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "text"
    output = tmp_path / "out.json"
    args = ["annotations", "create", str(source), str(output), "--id", "d"]
    with pytest.raises(InputError, match="cannot read"):
        run(args)
    source.write_bytes(b"\xff")
    with pytest.raises(InputError, match="UTF-8"):
        run(args)
    source.write_text("hello", encoding="utf-8")
    monkeypatch.setattr("corpusledger.annotation_cli.MAX_DOCUMENT_BYTES", 3)
    with pytest.raises(InputError, match="input exceeds 3"):
        run(args)
    assert not output.exists()


def test_output_size_fails_before_replace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "text"
    source.write_text("x", encoding="utf-8")
    output = tmp_path / "out.json"
    output.write_text("prior", encoding="utf-8")
    monkeypatch.setattr("corpusledger.annotation_cli.MAX_DOCUMENT_BYTES", 20)
    with pytest.raises(InputError, match="output exceeds 20"):
        run(["annotations", "create", str(source), str(output), "--id", "d"])
    assert output.read_text("utf-8") == "prior"


def test_token_budget_stops_before_allocating_excess(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from corpusledger import annotation_cli

    # One existing annotation consumes one of the two allowed slots.
    doc = AnnotationDocument("d", "!!!", (AnnotationType("seed"),), (SpanAnnotation("seed", "seed", 0, 0),))
    source = tmp_path / "doc.json"
    source.write_text(json.dumps(doc.to_dict()), encoding="utf-8")
    output = tmp_path / "out.json"
    output.write_text("prior", encoding="utf-8")
    count = 0

    def counted(*args: Any, **kwargs: Any) -> SpanAnnotation:
        nonlocal count
        count += 1
        return SpanAnnotation(*args, **kwargs)

    monkeypatch.setattr(annotation_cli, "SpanAnnotation", counted)
    monkeypatch.setattr("corpusledger.annotations.MAX_ANNOTATIONS", 2)
    with pytest.raises(InputError, match="exceeds 2"):
        run(["annotations", "tokenize", str(source), str(output)])
    assert count == 1
    assert output.read_text("utf-8") == "prior"


def test_existing_token_layer_rejected_before_work(tmp_path: Path) -> None:
    doc = AnnotationDocument("d", "words", (AnnotationType("token"),))
    source = tmp_path / "doc.json"
    source.write_text(json.dumps(doc.to_dict()), encoding="utf-8")
    with pytest.raises(InputError, match="already exists"):
        run(["annotations", "tokenize", str(source), str(tmp_path / "out.json")])


def test_path_resolution_errors_are_cli_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError("denied")

    monkeypatch.setattr(Path, "resolve", fail)
    with pytest.raises(InputError, match="cannot resolve"):
        run(["annotations", "validate", "missing.json"])


def test_atomic_output_failure_preserves_existing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "text"
    source.write_text("safe", encoding="utf-8")
    output = tmp_path / "out.json"
    output.write_text("prior", encoding="utf-8")

    def fail(*args: Any) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr("corpusledger.annotation_cli.os.replace", fail)
    with pytest.raises(InputError, match="cannot write"):
        run(["annotations", "create", str(source), str(output), "--id", "d"])
    assert output.read_text("utf-8") == "prior"
    assert list(tmp_path.glob(".*.tmp")) == []
