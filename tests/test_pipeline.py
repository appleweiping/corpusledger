import json
from pathlib import Path

import pytest

from corpusledger import InputError, PipelineStep, drop_fields, rename_field, run_pipeline, select_fields


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_streaming_pipeline_checkpoint_and_resume(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    output = tmp_path / "derived.jsonl"
    state = tmp_path / "state.json"
    write_jsonl(
        source,
        [
            {"id": "a", "text": "A", "extra": 1},
            {"id": "b", "text": "B", "extra": 2},
        ],
    )
    steps = [select_fields(("id", "text")), rename_field("text", "content")]
    result = run_pipeline(source, output, steps, state=state)
    assert result.records == 2 and result.resumed is False
    assert output.read_text(encoding="utf-8") == '{"content":"A","id":"a"}\n{"content":"B","id":"b"}\n'
    resumed = run_pipeline(source, output, steps, state=state, resume=True)
    assert resumed.resumed and resumed.output_digest == result.output_digest
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["status"] == "complete" and saved["steps"] == ["select:id,text", "rename:text->content"]


def test_failure_is_atomic_and_does_not_leave_partial_output(tmp_path: Path) -> None:
    source, output, state = tmp_path / "source.jsonl", tmp_path / "out.jsonl", tmp_path / "state.json"
    write_jsonl(source, [{"id": "a", "value": 1}, {"id": "b", "value": 2}])
    output.write_text("old\n", encoding="utf-8")
    state.write_text('{"status":"old"}\n', encoding="utf-8")

    def fail(data: dict[str, object]) -> dict[str, object]:
        if data["id"] == "b":
            raise RuntimeError("bad record")
        return data

    with pytest.raises(RuntimeError, match="bad record"):
        run_pipeline(source, output, [rename_field("value", "new"), PipelineStep("fail", fail)], state=state)
    assert output.read_text(encoding="utf-8") == "old\n"
    assert state.read_text(encoding="utf-8") == '{"status":"old"}\n'


def test_preserves_ids_and_rejects_collisions(tmp_path: Path) -> None:
    source, output = tmp_path / "source.json", tmp_path / "out.jsonl"
    source.write_text('[{"id":"a","x":1}]', encoding="utf-8")
    with pytest.raises(InputError, match="preserve"):
        run_pipeline(source, output, [rename_field("id", "other")])
    assert not output.exists()
    with pytest.raises(ValueError, match="duplicates"):
        select_fields(("id", "id"))
    with pytest.raises(InputError, match="destination"):
        run_pipeline(source, output, [rename_field("x", "id")])


def test_drop_fields_and_source_change_invalidates_resume(tmp_path: Path) -> None:
    source, output = tmp_path / "source.jsonl", tmp_path / "out.jsonl"
    write_jsonl(source, [{"id": "a", "drop": True, "keep": 1}])
    steps = [drop_fields(("drop",))]
    result = run_pipeline(source, output, steps)
    assert result.resumed is False
    source.write_text('{"id":"a","drop":false,"keep":1}\n', encoding="utf-8")
    rerun = run_pipeline(source, output, steps, resume=True)
    assert rerun.resumed is False


def test_invalid_step_and_json_are_not_silenced(tmp_path: Path) -> None:
    source, output = tmp_path / "source.jsonl", tmp_path / "out.jsonl"
    source.write_text('{"id":"a","x":NaN}\n', encoding="utf-8")
    with pytest.raises(InputError):
        run_pipeline(source, output, [])
    with pytest.raises(ValueError, match="non-empty"):
        drop_fields(())
