import io
import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from corpusledger.canonical import CanonicalPolicy
from corpusledger.cli import run
from corpusledger.errors import InputError
from corpusledger.readers import Record


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


def test_snapshot_supports_per_field_sort_paths(tmp_path: Path) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","labels":["z","a"],"turns":[2,1]}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest), "--sort-path", "labels"]) == 0
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["hash_metadata"]["policy"]["sort_paths"] == ["labels"]
    assert run(["verify", str(manifest)]) == 0


def test_snapshot_supports_privacy_pack(tmp_path: Path) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","email":"a@example.test"}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest), "--privacy-pack", "pii"]) == 0
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert "email" in payload["privacy_metadata"]["config"]["sensitive_names"]


def test_stream_cli_emits_one_response_per_request_and_writes_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    requests = "\n".join(
        [
            json.dumps({"processor": "identity", "payload": {"id": "a", "text": "hello"}}),
            json.dumps({"processor": "select", "payload": {"id": "b", "text": "bye"}}),
            "not-json",
        ]
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(requests + "\n"))
    report_path = tmp_path / "stream-report.json"
    assert run(["stream", "--field", "id", "--report-output", str(report_path)]) == 0
    output = capsys.readouterr()
    responses = [json.loads(line) for line in output.out.splitlines()]
    assert len(responses) == 3
    assert responses[0]["result"] == {"id": "a", "text": "hello"}
    assert responses[1]["result"] == {"id": "b"}
    assert responses[2]["ok"] is False
    summary = json.loads(report_path.read_text(encoding="utf-8"))
    assert summary["records"] == 3
    assert summary["successes"] == 2
    assert summary["failures"] == 1
    assert output.err == ""


def test_stream_cli_strict_mode_returns_error_for_malformed_request(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("not-json\n"))
    assert run(["stream", "--strict"]) == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False


def test_stream_cli_rejects_invalid_line_limit() -> None:
    with pytest.raises(InputError, match="must be positive"):
        run(["stream", "--max-line-bytes", "0"])


def test_pipeline_cli_runs_and_resumes_with_provenance(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "source.jsonl"
    output = tmp_path / "derived.jsonl"
    source.write_text('{"id":"a","text":"A","drop":1}\n', encoding="utf-8")
    assert run(["pipeline", str(source), str(output), "--select", "id,text", "--rename", "text=content"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["records"] == 1 and first["resumed"] is False
    assert output.read_text(encoding="utf-8") == '{"content":"A","id":"a"}\n'
    assert (
        run(["pipeline", str(source), str(output), "--select", "id,text", "--rename", "text=content", "--resume"]) == 0
    )
    second = json.loads(capsys.readouterr().out)
    assert second["resumed"] is True


def test_bundle_command_writes_reproducible_archive(tmp_path: Path, capsys: object) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    archive = tmp_path / "snapshot.zip"
    assert run(["snapshot", str(source), str(manifest)]) == 0
    assert run(["bundle", str(manifest), str(archive), "--store", str(tmp_path / "objects")]) == 0
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result["files"] == ["manifest.json", "source/corpus.jsonl"]
    assert archive.is_file() and result["bytes"] == archive.stat().st_size


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


def test_cli_explicit_reader_is_recorded_and_required_for_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PipeReader:
        name = "pipe"
        version = "1"
        extensions = frozenset({".pipe"})

        def iter_records(
            self,
            path: Path,
            *,
            id_field: str,
            policy: CanonicalPolicy,
        ) -> Iterator[Record]:
            del policy
            data = {id_field: "a", "text": path.read_text(encoding="utf-8")}
            yield Record("a", data, path.as_posix(), 1)

    reader = PipeReader()
    monkeypatch.setattr("corpusledger.cli.load_reader_adapter", lambda name: reader)
    source = tmp_path / "corpus.pipe"
    source.write_text("hello", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest), "--reader", "pipe"]) == 0
    assert run(["verify", str(manifest)]) == 0
    with pytest.raises(InputError, match="not requested"):
        run(["verify", str(manifest), "--reader", "other"])
    reader.version = "2"
    with pytest.raises(InputError, match="installed version"):
        run(["verify", str(manifest)])


def test_builtin_manifest_rejects_reader_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a"}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest)]) == 0
    monkeypatch.setattr("corpusledger.cli.load_reader_adapter", lambda name: object())
    with pytest.raises(InputError, match="does not record"):
        run(["verify", str(manifest), "--reader", "unexpected"])


def _hardlink_or_skip(source: Path, destination: Path) -> None:
    try:
        destination.hardlink_to(source)
    except OSError as exc:
        pytest.skip(f"hard links are unavailable: {exc}")


def test_cli_rejects_hardlink_output_aliases_without_mutating_inputs(tmp_path: Path) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a","text":"before"}\n', encoding="utf-8")
    snapshot_alias = tmp_path / "snapshot-alias.jsonl"
    _hardlink_or_skip(source, snapshot_alias)
    source_bytes = source.read_bytes()
    with pytest.raises(InputError, match="snapshot output must not overwrite"):
        run(["snapshot", str(source), str(snapshot_alias)])
    assert source.read_bytes() == source_bytes

    before = tmp_path / "before.manifest.json"
    after = tmp_path / "after.manifest.json"
    assert run(["snapshot", str(source), str(before)]) == 0
    source.write_text('{"id":"a","text":"after"}\n', encoding="utf-8")
    assert run(["snapshot", str(source), str(after)]) == 0
    report_alias = tmp_path / "report.json"
    _hardlink_or_skip(before, report_alias)
    before_bytes = before.read_bytes()
    with pytest.raises(InputError, match="diff output must not overwrite"):
        run(["diff", str(before), str(after), "--format", "json", "--output", str(report_alias)])
    assert before.read_bytes() == before_bytes


def test_verify_rejects_manifest_hardlink_as_source(tmp_path: Path) -> None:
    source = tmp_path / "corpus.jsonl"
    source.write_text('{"id":"a"}\n', encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    assert run(["snapshot", str(source), str(manifest)]) == 0
    alias = tmp_path / "manifest-alias.json"
    _hardlink_or_skip(manifest, alias)
    with pytest.raises(InputError, match="must not be the manifest"):
        run(["verify", str(manifest), "--input", str(alias)])
