from __future__ import annotations

import json

import pytest

from corpusledger import CorpusService, compare_json_schemas
from corpusledger.cli import run


def test_schema_compatibility_reports_required_and_type_changes() -> None:
    before = {
        "type": "object",
        "properties": {"score": {"type": ["number", "null"]}},
    }
    after = {
        "type": "object",
        "required": ["score"],
        "properties": {"score": {"type": "number"}},
    }
    report = compare_json_schemas(before, after, mode="backward")
    assert not report.compatible
    assert {issue.rule for issue in report.issues} == {"required", "type"}
    assert compare_json_schemas(before, after, mode="forward").compatible
    assert not compare_json_schemas(before, after, mode="full").compatible


def test_schema_compatibility_cli_and_service(tmp_path, capsys) -> None:
    before = tmp_path / "before.json"
    after = tmp_path / "after.json"
    before.write_text(json.dumps({"type": "object", "required": ["id"]}), encoding="utf-8")
    after.write_text(json.dumps({"type": "object", "required": ["id", "text"]}), encoding="utf-8")
    assert run(["schema-compat", str(before), str(after)]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["compatible"] is False
    service = CorpusService().dispatch({"operation": "schema_compat", "before": str(before), "after": str(after)})
    assert service["report"]["compatible"] is False


def test_schema_compatibility_checks_nested_constraints_and_direction() -> None:
    before = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "minItems": 1,
                "maxItems": 4,
                "items": {"type": "string"},
            },
            "kind": {"type": "string", "enum": ["a", "b"]},
        },
    }
    after = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "items": {"type": "array", "minItems": 2, "maxItems": 3},
            "kind": {"type": "string", "enum": ["a"]},
        },
    }
    report = compare_json_schemas(before, after, mode="backward")
    assert {issue.rule for issue in report.issues} >= {"minItems", "maxItems", "enum", "additionalProperties"}
    assert compare_json_schemas(before, after, mode="forward").compatible is True
    with pytest.raises(ValueError, match="mode"):
        compare_json_schemas(before, after, mode="sideways")
    with pytest.raises(TypeError, match="schemas"):
        compare_json_schemas([], after)  # type: ignore[arg-type]
