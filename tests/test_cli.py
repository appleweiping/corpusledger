import json
from pathlib import Path

from corpusledger.cli import main


def write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), "utf-8")
    return path


def test_diff_data_files_exit_status(tmp_path, capsys):
    old = write_jsonl(tmp_path / "old.jsonl", [{"id": "a", "text": "x"}, {"id": "b", "text": "y"}])
    new = write_jsonl(tmp_path / "new.jsonl", [{"id": "a", "text": "x"}, {"id": "c", "text": "z"}])
    assert main(["diff", str(old), str(old)]) == 0
    assert "no differences" in capsys.readouterr().out
    assert main(["diff", str(old), str(new)]) == 1
    out = capsys.readouterr().out
    assert "records matched by id field 'id'" in out
    assert "removed" in out and "added" in out


def test_snapshot_then_diff_against_current_data(tmp_path, capsys):
    old = write_jsonl(tmp_path / "v1.jsonl", [{"q": "a", "a": "1"}, {"q": "b", "a": "2"}])
    new = write_jsonl(tmp_path / "v2.jsonl", [{"q": "a", "a": "1"}, {"q": "b", "a": "3"}])
    snap = tmp_path / "v1.snapshot.json"
    assert main(["snapshot", str(old), "-o", str(snap), "--key", "q"]) == 0
    assert "2 records matched by key q" in capsys.readouterr().out
    # the snapshot carries the matching settings, so they need not be repeated
    assert main(["diff", str(snap), str(new), "--json"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["counts"]["changed"] == 1
    assert result["fields_changed"] == {"/a": 1}
    assert result["changed"][0]["label"] == "b"
    assert main(["diff", str(snap), str(old)]) == 0


def test_errors_exit_with_two(tmp_path, capsys):
    data = write_jsonl(tmp_path / "d.jsonl", [{"text": "no id"}])
    snap = tmp_path / "s.json"
    assert main(["snapshot", str(data), "-o", str(snap), "--key", "text"]) == 0
    assert main(["diff", str(snap), str(data), "--key", "other"]) == 2
    assert "snapshot again with the same options" in capsys.readouterr().err
    assert main(["diff", str(data), str(tmp_path / "missing.jsonl")]) == 2
    assert main(["snapshot", str(data), "-o", str(data)]) == 2
    output = tmp_path / "report.txt"
    assert main(["diff", str(data), str(data), "-o", str(output)]) == 0
    assert "no differences" in output.read_text("utf-8")
