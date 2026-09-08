from __future__ import annotations

import json

import pytest

from corpusledger import external_sort_jsonl
from corpusledger.cli import run


def test_external_sort_is_canonical_and_chunked(tmp_path) -> None:  # type: ignore[no-untyped-def]
    source = tmp_path / "records.jsonl"
    output = tmp_path / "sorted.jsonl"
    source.write_text(
        '{"id":"b","value":[2,1]}\n{"id":"a","value":{"z":1,"a":2}}\n{"id":"c","value":0}\n',
        encoding="utf-8",
    )
    report = external_sort_jsonl(source, output, chunk_size=1)
    lines = output.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["a", "b", "c"]
    assert report.records == 3
    assert report.chunks == 3
    assert len(report.output_digest) == 64


def test_external_sort_cli_and_safety_checks(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    source = tmp_path / "records.jsonl"
    output = tmp_path / "sorted.jsonl"
    source.write_text('{"id":"b"}\n{"id":"a"}\n', encoding="utf-8")
    assert run(["sort-jsonl", str(source), str(output), "--chunk-size", "1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["records"] == 2
    with pytest.raises(ValueError, match="positive"):
        external_sort_jsonl(source, output, chunk_size=0)
    with pytest.raises(ValueError, match="differ"):
        external_sort_jsonl(source, source)
