import math

import pytest

from corpusledger.privacy import PrivacyConfig, scan_records, shannon_entropy
from corpusledger.schema import infer_schema, schema_drift, to_json_schema


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
