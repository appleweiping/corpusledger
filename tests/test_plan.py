from __future__ import annotations

import json
from pathlib import Path

import pytest

from corpusledger import PLAN_FORMAT, PipelinePlan, PlanStep, load_pipeline_plan
from corpusledger.errors import InputError


def test_pipeline_plan_roundtrip_compiles_builtin_steps(tmp_path: Path) -> None:
    plan = PipelinePlan(
        (
            PlanStep("select", fields=("id", "text")),
            PlanStep("rename", old="text", new="content"),
            PlanStep("drop", fields=("unused",)),
        )
    )
    path = tmp_path / "plan.json"
    plan.save(path)
    assert json.loads(path.read_text(encoding="utf-8"))["format"] == PLAN_FORMAT
    loaded = load_pipeline_plan(path)
    assert loaded == plan
    assert [step.name for step in loaded.compile()] == [
        "select:id,text",
        "rename:text->content",
        "drop:unused",
    ]


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({}, "format"),
        ({"format": PLAN_FORMAT, "steps": []}, "at least one"),
        ({"format": PLAN_FORMAT, "steps": [{"kind": "nope"}]}, "unknown"),
        ({"format": PLAN_FORMAT, "steps": [{"kind": "select", "fields": ["id", "id"]}]}, "unique"),
        ({"format": PLAN_FORMAT, "steps": [{"kind": "rename", "old": "id"}]}, "old and new"),
        ({"format": PLAN_FORMAT, "steps": [{"kind": "drop", "fields": ["id"], "old": "x"}]}, "unknown"),
    ],
)
def test_pipeline_plan_rejects_invalid_shapes(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        PipelinePlan.from_dict(value)


def test_pipeline_plan_rejects_runtime_values_and_bad_files(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="PlanStep"):
        PipelinePlan((object(),))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown"):
        PlanStep("unknown")
    with pytest.raises(ValueError, match="fields"):
        PlanStep("select", fields=())
    with pytest.raises(ValueError, match="distinct"):
        PlanStep("rename", old="id", new="id")
    missing = tmp_path / "missing.json"
    with pytest.raises(InputError, match="cannot load"):
        load_pipeline_plan(missing)
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(InputError, match="cannot load"):
        load_pipeline_plan(malformed)


def test_pipeline_cli_accepts_plan_and_rejects_mixed_options(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    source.write_text('{"id":"a","text":"hello","drop":true}\n', encoding="utf-8")
    plan = tmp_path / "plan.json"
    PipelinePlan((PlanStep("select", fields=("id", "text")), PlanStep("rename", old="text", new="content"))).save(plan)
    output = tmp_path / "output.jsonl"
    from corpusledger.cli import run

    assert run(["pipeline", str(source), str(output), "--plan", str(plan)]) == 0
    assert output.read_text(encoding="utf-8") == '{"content":"hello","id":"a"}\n'
    with pytest.raises(InputError, match="cannot be combined"):
        run(["pipeline", str(source), str(output), "--plan", str(plan), "--drop", "drop"])
