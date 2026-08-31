import json
from pathlib import Path

import pytest

from corpusledger.errors import DuplicateIdError, InputError, ManifestError
from corpusledger.manifest import build_manifest
from corpusledger.readers import read_corpus


def write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_json_jsonl_and_directory_order(tmp_path: Path) -> None:
    write(tmp_path / "b.jsonl", '{"id":"b","text":"二"}\n\n')
    write(tmp_path / "a.json", json.dumps([{"id": "a", "text": "一"}], ensure_ascii=False))
    records = read_corpus(tmp_path)
    assert [record.record_id for record in records] == ["a", "b"]
    assert records[1].position == 1


def test_single_object_and_custom_id(tmp_path: Path) -> None:
    path = tmp_path / "one.json"
    write(path, '{"key":42,"text":"ok"}')
    assert read_corpus(path, "key")[0].record_id == "42"


def test_malformed_missing_id_and_extension(tmp_path: Path) -> None:
    bad = tmp_path / "bad.jsonl"
    write(bad, "{broken}\n")
    with pytest.raises(InputError, match="line 1"):
        read_corpus(bad)
    write(bad, '{"text":"missing"}\n')
    with pytest.raises(InputError, match="missing ID"):
        read_corpus(bad)
    txt = tmp_path / "data.txt"
    write(txt, "x")
    with pytest.raises(InputError, match="extension"):
        read_corpus(txt)


def test_duplicate_ids_across_files(tmp_path: Path) -> None:
    write(tmp_path / "a.json", '[{"id":"same"}]')
    write(tmp_path / "b.jsonl", '{"id":"same"}\n')
    with pytest.raises(DuplicateIdError, match="duplicate ID"):
        read_corpus(tmp_path)


def test_unicode_equivalent_ids_are_rejected(tmp_path: Path) -> None:
    write(tmp_path / "data.json", json.dumps([{"id": "é"}, {"id": "e\u0301"}], ensure_ascii=False))
    with pytest.raises(DuplicateIdError, match="duplicate ID"):
        read_corpus(tmp_path)


@pytest.mark.parametrize(
    "content",
    ['{"id":"first","id":"second"}\n', '{"id":"first","value":NaN}\n'],
)
def test_ambiguous_json_extensions_are_rejected(tmp_path: Path, content: str) -> None:
    path = tmp_path / "bad.jsonl"
    write(path, content)
    with pytest.raises(InputError, match="line 1"):
        read_corpus(path)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ('{"id":"first","value":1e400}\n', "finite binary64"),
        ('{"id":"first","value":' + "9" * 4_301 + "}\n", "4300-digit"),
    ],
)
def test_numeric_resource_boundaries_are_stable(tmp_path: Path, content: str, message: str) -> None:
    path = tmp_path / "numeric.jsonl"
    write(path, content)
    with pytest.raises(InputError, match=message):
        read_corpus(path)


def test_escaped_surrogate_errors_include_record_location(tmp_path: Path) -> None:
    invalid_id = tmp_path / "invalid-id.jsonl"
    write(invalid_id, '{"id":"\\ud800"}\n')
    with pytest.raises(InputError, match=r"record 1.*invalid ID.*Unicode scalar"):
        read_corpus(invalid_id)

    invalid_value = tmp_path / "invalid-value.jsonl"
    write(invalid_value, '{"id":"valid","text":"\\ud800"}\n')
    with pytest.raises(ManifestError, match=r"record 1.*Unicode scalar"):
        build_manifest(invalid_value)
