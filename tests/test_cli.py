import json
from pathlib import Path

import pytest

from corpusledger.cli import run
from corpusledger.errors import InputError


def test_snapshot_verify_and_diff_end_to_end(tmp_path: Path, capsys: object) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    assert run(["snapshot", str(source), str(first)]) == 0
    assert run(["verify", str(first)]) == 0
    source.write_text('{"id":"a","text":"changed"}\n', encoding="utf-8")
    assert run(["verify", str(first)]) == 1
    assert run(["snapshot", str(source), str(second), "--algorithm", "blake2b"]) == 0
    # Produce a compatible second manifest for diff.
    assert run(["snapshot", str(source), str(second)]) == 0
    report = tmp_path / "report.json"
    assert run(["diff", str(first), str(second), "--format", "json", "--output", str(report)]) == 1
    assert json.loads(report.read_text(encoding="utf-8"))["changed_records"]["a"]


def test_verify_checks_all_derived_manifest_sections(tmp_path: Path) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest)]) == 0
    raw = json.loads(manifest.read_text(encoding="utf-8"))
    raw["records"][0]["field_hashes"]["/text"] = "0" * 64
    raw["schema"] = {"tampered": True}
    manifest.write_text(json.dumps(raw), encoding="utf-8")
    assert run(["verify", str(manifest)]) == 1


def test_directory_snapshot_excludes_its_output_and_rejects_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "corpus"
    source.mkdir()
    data = source / "data.jsonl"
    data.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    manifest = source / "snapshot.json"
    assert run(["snapshot", str(source), str(manifest)]) == 0
    assert run(["verify", str(manifest)]) == 0
    with pytest.raises(InputError, match="must not overwrite"):
        run(["snapshot", str(data), str(data)])
    other_data = source / "other.json"
    other_data.write_text('{"id":"b"}\n', encoding="utf-8")
    with pytest.raises(InputError, match="not a CorpusLedger manifest"):
        run(["snapshot", str(source), str(other_data)])
