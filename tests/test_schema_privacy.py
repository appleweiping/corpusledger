import math

import pytest

from corpusledger.privacy import PrivacyConfig, scan_records, shannon_entropy
from corpusledger.schema import infer_schema, schema_drift


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
