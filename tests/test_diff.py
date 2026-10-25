import json
from pathlib import Path

import pytest

from corpusledger import Match, Snapshot, build_snapshot, compare
from corpusledger.errors import InputError, SnapshotError
from corpusledger.readers import iter_rows, to_json_value
from corpusledger.report import excerpt, render_json, render_text


def write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), "utf-8")
    return path


OLD = [
    {"instruction": "Give three tips for staying healthy.", "input": "", "output": "1.Eat well"},
    {"instruction": "Translate to French", "input": "Hello", "output": "Bonjour"},
    {"instruction": "Add the numbers", "input": "2 and 3", "output": "6", "text": "x"},
    {"instruction": "Name a color", "input": "", "output": "Blue"},
]
NEW = [
    {"instruction": "Give three tips for staying healthy.", "input": "", "output": "1. Eat well"},
    {"instruction": "Translate to French", "input": "Hello!", "output": "Bonjour"},
    {"instruction": "Add the numbers", "input": "2 and 3", "output": "5", "text": "y"},
    {"instruction": "Write a haiku", "input": "", "output": "..."},
]


@pytest.fixture
def versions(tmp_path: Path) -> tuple[Path, Path]:
    return write_jsonl(tmp_path / "old.jsonl", OLD), write_jsonl(tmp_path / "new.jsonl", NEW)


def test_key_matching_classifies_every_record(versions):
    old, new = versions
    match = Match("key", ("instruction", "input"))
    result = compare(
        build_snapshot(old, match, ignore=("text",), keep_rows=True),
        build_snapshot(new, match, ignore=("text",), keep_rows=True),
    )
    assert result.unchanged == 0
    assert [item.after.label for item in result.changed] == [
        "Give three tips for staying healthy.",
        "Add the numbers | 2 and 3",
    ]
    assert [item.fields for item in result.changed] == [("/output",), ("/output",)]
    assert len(result.rekeyed) == 1 and result.rekeyed[0].fields == ("/input",)
    assert [entry.label for entry in result.removed] == ["Name a color"]
    assert [entry.label for entry in result.added] == ["Write a haiku"]
    assert result.field_counts == {"/output": 2, "/input": 1}
    assert result.has_changes
    text = render_text(result)
    assert "key edited" in text and "/output" in text
    assert '- "1.Eat well"' in text and '+ "1. Eat well"' in text
    data = json.loads(render_json(result))
    assert data["counts"] == {
        "unchanged": 0,
        "changed": 2,
        "key_edited": 1,
        "removed": 1,
        "added": 1,
        "duplicates_before": 0,
        "duplicates_after": 0,
    }


def test_ignored_fields_do_not_count(versions, tmp_path):
    old, _ = versions
    rows = [dict(row, text="changed") for row in OLD]
    other = write_jsonl(tmp_path / "other.jsonl", rows)
    match = Match("key", ("instruction", "input"))
    same = compare(
        build_snapshot(old, match, ignore=("text",)), build_snapshot(other, match, ignore=("text",))
    )
    assert not same.has_changes and same.unchanged == 4
    differ = compare(build_snapshot(old, match), build_snapshot(other, match))
    assert len(differ.changed) == 4
    assert differ.field_counts == {"/text": 4}


def test_id_matching_reordering_and_schema(tmp_path):
    old = write_jsonl(
        tmp_path / "a.jsonl", [{"id": 1, "t": "a"}, {"id": 2, "t": "b", "m": {"x": 1}}]
    )
    new = write_jsonl(
        tmp_path / "b.jsonl", [{"id": 2, "t": "b", "m": {"x": "1"}}, {"id": 1, "t": "a"}]
    )
    result = compare(build_snapshot(old), build_snapshot(new))
    assert build_snapshot(old).match == Match("id", ("id",))
    assert result.order_changed
    assert [item.fields for item in result.changed] == [("/m/x",)]
    assert result.schema["retyped"] == {"/m/x": {"before": ["integer"], "after": ["string"]}}


def test_content_matching_and_duplicates(tmp_path):
    path = write_jsonl(tmp_path / "d.jsonl", [{"t": "same"}, {"t": "same"}, {"t": "other"}])
    snapshot = build_snapshot(path)
    assert snapshot.match == Match("content")
    assert snapshot.duplicates == 1
    assert snapshot.entries[1].id.endswith("#2")


def test_snapshot_round_trip_and_tamper_check(versions, tmp_path):
    old, _ = versions
    snapshot = build_snapshot(old, Match("key", ("instruction", "input")))
    target = tmp_path / "old.snapshot.json"
    snapshot.save(target)
    loaded = Snapshot.load(target)
    assert loaded.entries == snapshot.entries and loaded.dataset_hash == snapshot.dataset_hash
    assert target.read_text("utf-8").count("\n") == len(snapshot.entries) + 2
    target.write_text(target.read_text("utf-8").replace('"label":"Name', '"label":"Nam'), "utf-8")
    assert Snapshot.load(target).entries[3].label.startswith("Nam")  # labels are not hashed
    first = snapshot.entries[0].hash
    target.write_text(target.read_text("utf-8").replace(first, "0" * 16), "utf-8")
    with pytest.raises(SnapshotError, match="dataset_hash"):
        Snapshot.load(target)


def test_mismatched_settings_are_refused(versions):
    old, new = versions
    with pytest.raises(SnapshotError):
        compare(
            build_snapshot(old, Match("content")), build_snapshot(new, Match("key", ("input",)))
        )
    with pytest.raises(SnapshotError):
        compare(
            build_snapshot(old, Match("content")),
            build_snapshot(new, Match("content"), ignore=("text",)),
        )


def test_missing_fields_explain_how_to_match(versions):
    old, _ = versions
    with pytest.raises(InputError, match="--key"):
        build_snapshot(old, Match("id", ("id",)))
    with pytest.raises(InputError, match="'nope'"):
        build_snapshot(old, Match("key", ("nope",)))


def test_readers(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "b.json").write_text('[{"id": 2}]', "utf-8")
    (tmp_path / "data" / "a.csv").write_text("id,text\n1,hi\n", "utf-8")
    rows = list(iter_rows(tmp_path / "data"))
    assert [(row.source, row.data["id"]) for row in rows] == [("a.csv", "1"), ("b.json", 2)]
    bad = tmp_path / "bad.jsonl"
    bad.write_text('{"a": 1, "a": 2}\n', "utf-8")
    with pytest.raises(InputError, match="line 1"):
        list(iter_rows(bad))
    with pytest.raises(InputError):
        list(iter_rows(tmp_path / "missing.jsonl"))
    assert to_json_value({"b": b"\x00", "n": [float("nan")]}) == {"b": "AA==", "n": ["nan"]}


def test_parquet(tmp_path):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"id": [1, 2], "text": ["a", "b"]}), tmp_path / "t.parquet")
    snapshot = build_snapshot(tmp_path / "t.parquet")
    assert [entry.id for entry in snapshot.entries] == ["1", "2"]


def test_excerpt_starts_near_the_first_difference():
    left, right = excerpt("x" * 100 + "old ending", "x" * 100 + "new ending", width=30)
    assert left.startswith('"...') and "old ending" in left and "new ending" in right
