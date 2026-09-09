import json
from pathlib import Path

import pytest

from corpusledger import InputError, PrivacyConfig, scan_corpus


def test_scan_corpus_tracks_complete_population_and_nested_locations(tmp_path: Path) -> None:
    corpus = tmp_path / "data.jsonl"
    values = [
        {"id": "z", "people": [{"EMAIL": "z@example.test"}], "api_key": "short"},
        {"id": "a", "phone": "555-0100"},
        {"id": "clean", "text": "ordinary"},
    ]
    corpus.write_text("".join(json.dumps(value) + "\n" for value in values), encoding="utf-8")
    original = corpus.read_bytes()
    report = scan_corpus(corpus, config=PrivacyConfig.from_pack("pii"))
    assert report.records_checked == 3
    assert [(finding["record_id"], finding["path"]) for finding in report.findings] == [
        ("a", "/phone"),
        ("z", "/api_key"),
        ("z", "/people/0/EMAIL"),
    ]
    assert report.to_dict()["schema_version"] == 1
    assert all(value not in json.dumps(report.to_dict()) for value in ("z@example.test", "555-0100", "short"))
    assert corpus.read_bytes() == original
    copy = report.to_dict()
    copy["findings"][0]["path"] = "changed"
    assert report.findings[0]["path"] == "/phone"


def test_scan_corpus_empty_clean_and_duplicate_records(tmp_path: Path) -> None:
    corpus = tmp_path / "data.jsonl"
    corpus.write_text("", encoding="utf-8")
    assert scan_corpus(corpus).to_dict()["records_checked"] == 0
    corpus.write_text('{"id":"a","text":"hello world"}\n', encoding="utf-8")
    report = scan_corpus(corpus)
    assert report.records_checked == 1 and report.findings == ()
    corpus.write_text('{"id":"a"}\n{"id":"a"}\n', encoding="utf-8")
    with pytest.raises(InputError, match="duplicate ID"):
        scan_corpus(corpus)


@pytest.mark.parametrize("field", [None, 3, "", " "])
def test_scan_rejects_invalid_id_field(tmp_path: Path, field: object) -> None:
    with pytest.raises(ValueError, match="id_field"):
        scan_corpus(tmp_path / "unused.jsonl", id_field=field)  # type: ignore[arg-type]


def test_scan_rejects_invalid_config(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="PrivacyConfig"):
        scan_corpus(tmp_path / "unused.jsonl", config={})  # type: ignore[arg-type]
