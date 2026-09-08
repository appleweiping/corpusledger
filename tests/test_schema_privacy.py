import math

import pytest

from corpusledger.privacy import PrivacyConfig, privacy_packs, scan_records, shannon_entropy
from corpusledger.schema import infer_schema, schema_drift, to_json_schema, validate_json_schema


def test_schema_is_conservative_and_summarizes_collections() -> None:
    schema = infer_schema(
        [
            {"text": "hi", "labels": ["x"], "meta": {"lang": "en"}, "score": None},
            {"text": "bye", "labels": [1, 2], "meta": {}, "extra": True},
        ]
    )
    assert schema["fields"]["/labels"]["item_types"] == ["integer", "string"]
    assert schema["fields"]["/score"]["nullable"] is True
    assert schema["fields"]["/extra"]["optional"] is True
    assert schema["fields"]["/meta"]["object_keys"] == ["lang"]


def test_schema_drift_classifies_changes() -> None:
    before = infer_schema([{"a": 1, "old": "x"}])
    after = infer_schema([{"a": "1", "new": "x"}])
    drift = schema_drift(before, after)
    assert drift["added_fields"] == ["/new"]
    assert drift["removed_fields"] == ["/old"]
    assert "/a" in drift["changed_fields"]


def test_schema_exports_nested_json_schema_with_required_and_mixed_types() -> None:
    inferred = infer_schema(
        [
            {"id": "a", "meta": {"lang": "en"}, "labels": ["x"]},
            {"id": "b", "meta": {}, "labels": [1, 2], "optional": True},
        ]
    )
    exported = to_json_schema(inferred, title="Demo", schema_id="urn:demo")
    assert exported["$schema"].endswith("draft/2020-12/schema")
    assert exported["title"] == "Demo"
    assert exported["$id"] == "urn:demo"
    assert exported["required"] == ["id", "labels", "meta"]
    assert exported["properties"]["labels"]["items"]["type"] == ["integer", "string"]
    assert exported["properties"]["meta"]["properties"]["lang"]["type"] == "string"
    assert exported["properties"]["optional"]["type"] == "boolean"


def test_schema_export_rejects_malformed_inferred_shape() -> None:
    with pytest.raises(ValueError, match="record_count"):
        to_json_schema({"record_count": True, "fields": {}})
    with pytest.raises(ValueError, match="JSON Pointers"):
        to_json_schema({"record_count": 1, "fields": {"name": {}}})


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ([], "object"),
        ({"record_count": 1, "fields": []}, "fields"),
        ({"record_count": 1, "fields": {"/x": []}}, "object"),
        ({"record_count": 1, "fields": {"/x": {"types": []}}}, "non-empty"),
        ({"record_count": 1, "fields": {"/x": {"types": ["date"]}}}, "unsupported"),
        (
            {"record_count": 1, "fields": {"/x": {"types": ["array"], "item_types": "str"}}},
            "item_types",
        ),
        (
            {"record_count": 1, "fields": {"/x": {"types": ["object"], "object_keys": [1]}}},
            "object_keys",
        ),
        (
            {"record_count": 1, "fields": {"/x": {"types": ["array"], "min_items": -1}}},
            "min_items",
        ),
    ],
)
def test_schema_export_rejects_invalid_metadata(value: object, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        to_json_schema(value)  # type: ignore[arg-type]


def test_schema_export_validates_titles_ids_and_path_collisions() -> None:
    valid = {"record_count": 1, "fields": {"/x": {"types": ["string"], "optional": False}}}
    for kwargs in ({"title": ""}, {"schema_id": ""}):
        with pytest.raises(ValueError, match="non-empty"):
            to_json_schema(valid, **kwargs)
    with pytest.raises(ValueError, match="invalid"):
        to_json_schema({"record_count": 1, "fields": {"//x": {"types": ["string"]}}})


def test_schema_validation_reports_nested_paths_and_bounds() -> None:
    schema = {
        "type": "object",
        "required": ["id", "tags"],
        "properties": {
            "id": {"type": "string", "minLength": 2},
            "tags": {"type": "array", "minItems": 1, "items": {"type": "string"}},
        },
        "additionalProperties": False,
    }
    issues = validate_json_schema(
        [{"id": "a", "tags": ["ok"]}, {"id": "bb", "tags": [1], "extra": True}],
        schema,
    )
    assert [(item.record, item.path) for item in issues] == [(1, "/id"), (2, "/tags/0"), (2, "/extra")]
    assert validate_json_schema([{"id": "ok", "tags": ["x"]}], schema) == ()


def test_schema_validation_supports_combinators_and_error_limit() -> None:
    schema = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
    assert validate_json_schema(["ok", 3], schema) == ()
    assert len(validate_json_schema([True, False], schema, max_errors=1)) == 1
    with pytest.raises(ValueError, match="max_errors"):
        validate_json_schema([], schema, max_errors=0)


def test_schema_validation_covers_scalar_constraints_and_schema_booleans() -> None:
    assert validate_json_schema([{"x": 1}], True) == ()
    false_errors = validate_json_schema([{"x": 1}, None], False)
    assert len(false_errors) == 2 and false_errors[0].message == "schema is false"
    with pytest.raises(TypeError, match="object or boolean"):
        validate_json_schema([], "bad")  # type: ignore[arg-type]
    schema = {
        "type": ["string", "integer"],
        "allOf": [{"anyOf": [{"type": "string"}, {"type": "integer"}]}],
        "enum": ["ok", 3],
        "const": "ok",
        "minLength": 2,
        "maxLength": 4,
        "pattern": "^[a-z]+$",
        "minimum": 1,
        "maximum": 3,
    }
    assert validate_json_schema(["ok"], schema) == ()
    findings = validate_json_schema(["", "TOOLONG", 5, True], schema)
    assert {item.path for item in findings} == {""}
    nested = {
        "type": "object",
        "properties": {"x": {"type": "integer"}},
        "additionalProperties": {"type": "string"},
    }
    assert validate_json_schema([{"x": 1, "extra": 2}], nested)[0].path == "/extra"


def test_privacy_scan_redacts_values() -> None:
    secret = "Z8x4Qm2Vn9P0rT7sK3jH5dF1"
    findings = scan_records([("r1", {"api_key": secret, "ordinary": "hello"})])
    assert {finding["kind"] for finding in findings} == {"sensitive_field_name", "high_entropy_token"}
    assert secret not in str(findings)
    assert len(findings[1].get("evidence_hash", findings[0].get("evidence_hash", ""))) in {0, 12}
    assert shannon_entropy("aaaa") == 0


def test_custom_privacy_config() -> None:
    findings = scan_records([("x", {"private_note": "short"})], PrivacyConfig(frozenset({"private_note"}), 100, 9.0))
    assert findings == [{"kind": "sensitive_field_name", "path": "/private_note", "record_id": "x"}]


def test_privacy_rule_packs_are_named_and_deterministic() -> None:
    assert privacy_packs() == ("credentials", "default", "pii")
    credentials = PrivacyConfig.from_pack("credentials")
    pii = PrivacyConfig.from_pack("pii")
    assert "client_secret" in credentials.sensitive_names
    assert "email" in pii.sensitive_names
    assert PrivacyConfig.from_pack().to_dict() == PrivacyConfig().to_dict()
    with pytest.raises(ValueError, match="unknown privacy pack"):
        PrivacyConfig.from_pack("unknown")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({"sensitive_names": [], "min_token_length": 0, "entropy_threshold": 1.0}, "at least 1"),
        (
            {"sensitive_names": [], "min_token_length": 1, "entropy_threshold": math.nan},
            "finite",
        ),
        ({"sensitive_names": "bad", "min_token_length": 1, "entropy_threshold": 1.0}, "list"),
        ({"sensitive_names": [], "min_token_length": True, "entropy_threshold": 1.0}, "integer"),
        ({"sensitive_names": [], "min_token_length": 1, "entropy_threshold": "bad"}, "numeric"),
        ({"sensitive_names": [], "min_token_length": 1}, "exactly"),
    ],
)
def test_invalid_privacy_config_is_rejected(value: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        PrivacyConfig.from_dict(value)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"sensitive_names": "token"}, "collection"),
        ({"sensitive_names": ["ok", 1]}, "non-empty strings"),
        ({"min_token_length": True}, "integer"),
        ({"min_token_length": 2.5}, "integer"),
        ({"entropy_threshold": True}, "numeric"),
        ({"entropy_threshold": "3.7"}, "numeric"),
    ],
)
def test_direct_privacy_config_types_are_strict(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        PrivacyConfig(**kwargs)  # type: ignore[arg-type]
