"""Independent expected results for document contracts and interval algebra."""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from corpusledger import (
    AnnotationDocument,
    AnnotationField,
    AnnotationIndex,
    AnnotationType,
    InputError,
    SpanAnnotation,
    codepoint_to_utf16,
    utf16_to_codepoint,
)


def document() -> AnnotationDocument:
    return AnnotationDocument(
        "sample",
        "A😀 e\u0301\r\nZ",
        (
            AnnotationType("token", {"text": AnnotationField(), "score": AnnotationField("number", required=False)}),
            AnnotationType("edge", {"from": AnnotationField("reference", target_type="token")}),
        ),
        (
            SpanAnnotation("b", "token", 1, 2, {"text": "😀"}),
            SpanAnnotation("a", "token", 0, 1, {"text": "A", "score": 1}),
            SpanAnnotation("link", "edge", 0, 2, {"from": "b"}),
        ),
    )


def test_document_roundtrip_exact_text_and_deterministic_digest() -> None:
    doc = document()
    raw = json.loads(json.dumps(doc.to_dict(), ensure_ascii=False))
    assert AnnotationDocument.from_dict(raw) == doc
    assert doc.span_text("b") == "😀"
    assert doc.text_sha256 == hashlib.sha256(b"A\xf0\x9f\x98\x80 e\xcc\x81\r\nZ").hexdigest()
    assert (
        doc.digest
        == hashlib.sha256(
            json.dumps(raw, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    )
    permuted = AnnotationDocument(
        doc.document_id, doc.text, tuple(reversed(doc.annotation_types)), tuple(reversed(doc.annotations))
    )
    assert permuted == doc and permuted.digest == doc.digest
    with pytest.raises(KeyError):
        doc.get("missing")
    with pytest.raises(InputError, match="unknown annotation type"):
        doc.index("missing")
    assert len(doc.index("token").annotations) == 2


def test_deep_snapshots_no_aliases() -> None:
    features = {"payload": {"values": [1, {"ok": True}]}}
    fields = {"payload": AnnotationField("object")}
    annotation = SpanAnnotation("a", "T", 0, 0, features)
    schema = AnnotationType("T", fields)
    doc = AnnotationDocument("d", "", (schema,), (annotation,))
    before = doc.digest
    features["payload"]["values"].append(3)  # type: ignore[union-attr]
    fields.clear()
    rendered = doc.to_dict()
    rendered["annotations"][0]["features"]["payload"]["values"][1]["ok"] = False
    assert doc.digest == before
    with pytest.raises(TypeError):
        annotation.features["payload"]["values"][1]["ok"] = False
    with pytest.raises(dataclasses.FrozenInstanceError):
        doc.text = "new"  # type: ignore[misc]


@pytest.mark.parametrize("start,end", [(-1, 0), (2, 1), (True, 1), (0, False), (0, 1.0)])
def test_invalid_offsets(start: Any, end: Any) -> None:
    with pytest.raises(InputError, match="offsets"):
        SpanAnnotation("x", "t", start, end)


@pytest.mark.parametrize("name", ["", " ", 4, "x" * 257, "\ud800"])
def test_invalid_names(name: Any) -> None:
    with pytest.raises(InputError):
        AnnotationType(name)
    with pytest.raises(InputError):
        SpanAnnotation(name, "t", 0, 0)
    with pytest.raises(InputError):
        AnnotationDocument(name, "")


@pytest.mark.parametrize(
    "bad",
    [float("nan"), float("inf"), {1: "x"}, {"bad": "\ud800"}, {1, 2}, object(), 10**5000],
    ids=["nan", "infinity", "integer-key", "surrogate", "set", "object", "large-int"],
)
def test_invalid_feature_values_rejected_before_digest(bad: Any) -> None:
    with pytest.raises(InputError):
        SpanAnnotation("x", "t", 0, 0, {"v": bad})


def test_cyclic_and_deep_features_rejected() -> None:
    cycle: list[Any] = []
    cycle.append(cycle)
    with pytest.raises(InputError, match="nesting"):
        SpanAnnotation("x", "t", 0, 0, {"v": cycle})


@pytest.mark.parametrize(
    "kind,value",
    [
        ("integer", True),
        ("integer", 1.0),
        ("number", False),
        ("boolean", 1),
        ("string", None),
        ("object", []),
        ("array", {}),
        ("reference", 0),
        ("references", [True]),
    ],
)
def test_features_do_not_coerce(kind: str, value: Any) -> None:
    with pytest.raises(InputError, match="must have type"):
        AnnotationDocument(
            "d",
            "",
            (AnnotationType("T", {"v": AnnotationField(kind)}),),
            (SpanAnnotation("a", "T", 0, 0, {"v": value}),),
        )


@pytest.mark.parametrize(
    "kind,value",
    [
        ("boolean", True),
        ("integer", 0),
        ("number", 1.25),
        ("string", ""),
        ("object", {"a": [None, False, 1]}),
        ("array", ["a"]),
    ],
)
def test_feature_kinds_roundtrip(kind: str, value: Any) -> None:
    doc = AnnotationDocument(
        "d", "", (AnnotationType("T", {"v": AnnotationField(kind)}),), (SpanAnnotation("a", "T", 0, 0, {"v": value}),)
    )
    assert AnnotationDocument.from_dict(doc.to_dict()) == doc


def test_required_nullable_closed_and_reference_contracts() -> None:
    schema = AnnotationType(
        "t", {"required": AnnotationField("integer", nullable=True), "optional": AnnotationField(required=False)}
    )
    AnnotationDocument("d", "", (schema,), (SpanAnnotation("a", "t", 0, 0, {"required": None}),))
    for features, message in [({}, "missing required"), ({"required": 1, "extra": 2}, "undeclared")]:
        with pytest.raises(InputError, match=message):
            AnnotationDocument("d", "", (schema,), (SpanAnnotation("a", "t", 0, 0, features),))
    references = AnnotationType("node", {"links": AnnotationField("references", target_type="node")})
    # Forward/self/cyclic references are graph relationships, not an execution DAG.
    doc = AnnotationDocument(
        "d",
        "ab",
        (references,),
        (
            SpanAnnotation("a", "node", 0, 1, {"links": ["b", "a"]}),
            SpanAnnotation("b", "node", 1, 2, {"links": ["a"]}),
        ),
    )
    assert AnnotationDocument.from_dict(doc.to_dict()) == doc
    with pytest.raises(InputError, match="dangling"):
        AnnotationDocument("d", "", (references,), (SpanAnnotation("a", "node", 0, 0, {"links": ["missing"]}),))
    typed = AnnotationType("edge", {"target": AnnotationField("reference", target_type="node")})
    with pytest.raises(InputError, match="does not target"):
        AnnotationDocument("d", "", (references, typed), (SpanAnnotation("a", "edge", 0, 0, {"target": "a"}),))
    with pytest.raises(InputError, match="unknown reference target"):
        AnnotationDocument("d", "", (typed,))


@pytest.mark.parametrize(
    "spec",
    [
        dict(kind="unknown"),
        dict(kind=[]),
        dict(required=1),
        dict(nullable="yes"),
        dict(target_type="T"),
        dict(kind="reference", target_type=""),
    ],
)
def test_bad_field_spec(spec: dict[str, Any]) -> None:
    with pytest.raises(InputError):
        AnnotationField(**spec)


def test_invalid_document_structure() -> None:
    t = AnnotationType("t")
    a = SpanAnnotation("a", "t", 0, 1)
    bad_arguments = [
        dict(text=4),
        dict(text="\ud800"),
        dict(annotation_types=(t, t)),
        dict(annotation_types=(t,), annotations=(a, a)),
        dict(annotations=(a,)),
        dict(annotation_types=(t,), annotations=(SpanAnnotation("x", "t", 0, 9),)),
        dict(annotation_types=("not a type",)),
        dict(annotations=("not a span",)),
        dict(annotations=None),
        dict(annotation_types=None),
    ]
    for changes in bad_arguments:
        with pytest.raises(InputError):
            AnnotationDocument(**({"document_id": "d", "text": "ab"} | changes))
    with pytest.raises(InputError):
        AnnotationType("t", {"a": "not a spec"})  # type: ignore[dict-item]
    with pytest.raises(InputError):
        AnnotationType("t", [])  # type: ignore[arg-type]
    with pytest.raises(InputError):
        SpanAnnotation("a", "t", 0, 0, [])  # type: ignore[arg-type]


def test_strict_serialized_document_fields_and_digest() -> None:
    original = document().to_dict()
    for changes in [
        {"format": "future"},
        {"offset_unit": "utf16"},
        {"text_sha256": "0" * 64},
        {"text": original["text"] + "x"},
        {"extra": 1},
        {"types": {}},
        {"annotations": {}},
        {"types": [{"name": "bad", "fields": []}]},
        {"types": [{"name": "bad", "fields": {"v": {"kind": "string"}}}]},
        {"annotations": [{"id": "a"}]},
    ]:
        with pytest.raises(InputError):
            AnnotationDocument.from_dict(original | changes)
    missing = copy.deepcopy(original)
    del missing["text_sha256"]
    with pytest.raises(InputError):
        AnnotationDocument.from_dict(missing)


def test_document_limits_before_materializing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("corpusledger.annotations.MAX_TEXT_CODEPOINTS", 1)
    with pytest.raises(InputError, match="at most 1"):
        AnnotationDocument("d", "xx")
    monkeypatch.setattr("corpusledger.annotations.MAX_ANNOTATIONS", 0)
    with pytest.raises(InputError, match="exceeds 0"):
        AnnotationDocument.from_dict(
            document_payload := {
                "format": "corpusledger.annotations.v1",
                "id": "d",
                "text": "",
                "text_sha256": "",
                "offset_unit": "unicode_codepoint",
                "types": [],
                "annotations": [{"malformed": True}],
            }
        )
    assert document_payload["annotations"] == [{"malformed": True}]
    with pytest.raises(InputError, match="exceeds 0"):
        AnnotationDocument("d", "", (), (SpanAnnotation("a", "t", 0, 0),))


def _oracle(span: SpanAnnotation, start: int, end: int, relation: str) -> bool:
    if relation == "exact":
        return (span.start, span.end) == (start, end)
    if relation == "inside":
        return start <= span.start <= span.end <= end
    if relation == "overlapping":
        return bool(set(range(span.start, span.end)) & set(range(start, end)))
    if start == end:
        return start in range(span.start, span.end) or span.start == span.end == start
    return set(range(start, end)).issubset(range(span.start, span.end))


def test_all_interval_relations_against_independent_set_oracle() -> None:
    spans = tuple(SpanAnnotation(f"s{i}-{j}", "t", i, j) for i in range(9) for j in range(i, 9))
    index = AnnotationIndex(tuple(reversed(spans)), 8)
    for relation in ("exact", "inside", "covering", "overlapping"):
        for start in range(9):
            for end in range(start, 9):
                expected = tuple(item for item in spans if _oracle(item, start, end, relation))
                assert index.query(start, end, relation=relation) == expected
    assert AnnotationIndex((), 0).query(0, 0, relation="covering") == ()
    with pytest.raises(InputError, match="relation"):
        index.query(0, 1, relation="approximate")
    with pytest.raises(InputError):
        index.query(0, 9)
    with pytest.raises(InputError):
        AnnotationIndex(("bad",), 1)  # type: ignore[arg-type]
    with pytest.raises(InputError):
        AnnotationIndex(None, 1)  # type: ignore[arg-type]


@given(st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=60))
def test_utf16_boundary_roundtrip_matches_encoding(text: str) -> None:
    for offset in range(len(text) + 1):
        expected = len(text[:offset].encode("utf-16-le")) // 2
        assert codepoint_to_utf16(text, offset) == expected
        assert utf16_to_codepoint(text, expected) == offset


def test_utf16_rejects_split_and_out_of_bounds() -> None:
    for text, offset in [("😀", 1), ("a😀b", 2), ("a", 2), ("", 1), ("a", -1), ("a", True), ("\ud800", 0)]:
        with pytest.raises(InputError):
            utf16_to_codepoint(text, offset)
    for function in (codepoint_to_utf16, utf16_to_codepoint):
        with pytest.raises(InputError):
            function(None, 0)  # type: ignore[arg-type]
