"""Independent wire, identity and successor-validation oracles."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from corpusledger import AnnotationDocument, AnnotationField, AnnotationType, InputError, SpanAnnotation
from corpusledger.annotation_pipeline import AnnotationPipelineError
from corpusledger.annotation_protocol import (
    MAX_WIRE_BYTES,
    AnnotationProtocolError,
    AnnotationRequest,
    AnnotationResponse,
    ProcessorDescription,
    decode_wire,
    encode_wire,
)

TOKEN = AnnotationType("token", {"text": AnnotationField(), "position": AnnotationField("integer")})
GROUP = AnnotationType(
    "token_group",
    {"members": AnnotationField("references", target_type="token"), "count": AnnotationField("integer")},
)


def description() -> ProcessorDescription:
    return ProcessorDescription("demo.go.tokens", "1", "a" * 64, produces=(TOKEN,))


def request() -> AnnotationRequest:
    return AnnotationRequest("op-1", "token-step", description(), AnnotationDocument("original", "😀x\r\ny"))


def response() -> AnnotationResponse:
    incoming = request()
    return AnnotationResponse(
        incoming.operation_id,
        incoming.step_id,
        incoming.processor,
        incoming.document.digest,
        (
            SpanAnnotation("t-1", "token", 0, 2, {"text": "😀x", "position": 0}),
            SpanAnnotation("t-2", "token", 4, 5, {"text": "y", "position": 1}),
        ),
        7,
    )


def test_real_schema_chain_and_roundtrip() -> None:
    incoming = request()
    original = incoming.to_dict()
    restored = AnnotationRequest.from_dict(decode_wire(encode_wire(original)))
    assert restored == incoming and restored.digest == incoming.digest
    output = AnnotationResponse.from_dict(decode_wire(encode_wire(response().to_dict())))
    first = output.apply(restored)
    assert first.document.text == incoming.document.text
    assert [first.document.span_text(name) for name in ("t-1", "t-2")] == ["😀x", "y"]
    grouping = ProcessorDescription("demo.java.group", "1", "b" * 64, (TOKEN,), (GROUP,))
    second_request = AnnotationRequest("op-1", "group-step", grouping, first.document)
    second_response = AnnotationResponse(
        "op-1",
        "group-step",
        grouping,
        first.document.digest,
        (SpanAnnotation("g-1", "token_group", 0, 5, {"members": ["t-1", "t-2"], "count": 2}),),
    )
    second = second_response.apply(second_request)
    assert second.document.get("g-1").features["members"] == ("t-1", "t-2")
    assert second.input_digest == first.output_digest
    assert second.steps[0].required_types == ("token",)
    assert incoming.to_dict() == original


@pytest.mark.parametrize("bad", [b'{"a":1,"a":2}', b'{"a":1,"\\u0061":2}', b"NaN", b"1e999", b'"\\ud800"', b"\xff"])
def test_wire_rejects_ambiguous_or_non_unicode_json(bad: bytes) -> None:
    with pytest.raises(AnnotationProtocolError):
        decode_wire(bad)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), "\udfff", {1: "x"}, (1, 2), b"bytes", object()])
def test_encoder_rejects_non_json_values(bad: Any) -> None:
    with pytest.raises(AnnotationProtocolError):
        encode_wire(bad)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, MAX_WIRE_BYTES + 1])
def test_wire_byte_limits_are_strict(limit: Any) -> None:
    for callback, value in ((encode_wire, {}), (decode_wire, b"{}")):
        with pytest.raises(AnnotationProtocolError):
            callback(value, limit=limit)


def test_wire_bounds_and_exact_json_numbers() -> None:
    assert decode_wire(encode_wire({"n": 2**200, "x": 1.0, "b": True})) == {"n": 2**200, "x": 1.0, "b": True}
    assert encode_wire("😀", limit=6) == b'"\xf0\x9f\x98\x80"'
    with pytest.raises(AnnotationProtocolError):
        encode_wire("😀", limit=5)
    with pytest.raises(AnnotationProtocolError):
        decode_wire(b"{}", limit=1)
    with pytest.raises(AnnotationProtocolError):
        decode_wire("{}")  # type: ignore[arg-type]
    nested: Any = 0
    for _ in range(66):
        nested = [nested]
    with pytest.raises(AnnotationProtocolError):
        encode_wire(nested)
    with pytest.raises(AnnotationProtocolError):
        decode_wire(b"[" * 66 + b"0" + b"]" * 66)


@pytest.mark.parametrize(
    "change",
    [
        {"name": "bad name"},
        {"version": ""},
        {"config_sha256": "A" * 64},
        {"produces": ()},
        {"produces": (TOKEN, TOKEN)},
        {"requires": (TOKEN,)},
        {"produces": (GROUP,)},
        {"requires": (GROUP,), "produces": (TOKEN,)},
        {"produces": ["token"]},
        {"requires": None},
    ],
)
def test_descriptor_validates_closed_schema_dependencies(change: dict[str, Any]) -> None:
    with pytest.raises(InputError):
        replace(description(), **change)


@pytest.mark.parametrize("field", ["format", "name", "version", "config_sha256", "requires", "produces"])
def test_missing_descriptor_fields_rejected(field: str) -> None:
    raw = description().to_dict()
    del raw[field]
    with pytest.raises(InputError):
        ProcessorDescription.from_dict(raw)


def test_declared_schema_codec_validates_every_field() -> None:
    raw = description().to_dict()
    raw["produces"][0]["fields"]["text"]["nullable"] = 0
    with pytest.raises(InputError):
        ProcessorDescription.from_dict(raw)
    raw = description().to_dict()
    raw["produces"] = [TOKEN.to_dict()] * 129
    with pytest.raises(InputError):
        ProcessorDescription.from_dict(raw)
    raw = description().to_dict()
    raw["produces"][0]["fields"] = []
    with pytest.raises(InputError):
        ProcessorDescription.from_dict(raw)


@pytest.mark.parametrize("change", [{"operation_id": "?"}, {"step_id": ""}, {"processor": None}, {"document": {}}])
def test_request_constructor_validation(change: dict[str, Any]) -> None:
    with pytest.raises(InputError):
        replace(request(), **change)


@pytest.mark.parametrize("field,value", [("input_digest", "f" * 64), ("format", "future"), ("document", {})])
def test_request_codec_rejects_changed_input(field: str, value: Any) -> None:
    raw = request().to_dict()
    raw[field] = value
    with pytest.raises(InputError):
        AnnotationRequest.from_dict(raw)


@pytest.mark.parametrize(
    "change",
    [
        {"operation_id": "other"},
        {"step_id": "other"},
        {"input_digest": "f" * 64},
        {"processor": replace(description(), config_sha256="c" * 64)},
    ],
)
def test_response_must_bind_exact_reserved_identity(change: dict[str, Any]) -> None:
    with pytest.raises(AnnotationProtocolError, match="identity"):
        replace(response(), **change).apply(request())


@pytest.mark.parametrize("duration", [True, -1, 1.0, 2**63])
def test_response_duration_is_not_coerced(duration: Any) -> None:
    with pytest.raises(InputError):
        replace(response(), duration_ms=duration)


@pytest.mark.parametrize(
    "mode", ["duplicate", "outside", "undeclared", "wrong_feature", "extra_text", "format", "not_array"]
)
def test_untrusted_worker_outputs_cannot_change_the_document(mode: str) -> None:
    raw = response().to_dict()
    if mode == "duplicate":
        raw["annotations"].append(raw["annotations"][0])
    elif mode == "outside":
        raw["annotations"][0]["end"] = 500
    elif mode == "undeclared":
        raw["annotations"][0]["type"] = "other"
    elif mode == "wrong_feature":
        raw["annotations"][0]["features"]["position"] = True
    elif mode == "extra_text":
        raw["text"] = "replacement"
    elif mode == "format":
        raw["format"] = "future"
    else:
        raw["annotations"] = {}
    incoming = request()
    before = incoming.document.digest
    with pytest.raises((InputError, AnnotationPipelineError)):
        AnnotationResponse.from_dict(raw).apply(incoming)
    assert incoming.document.digest == before
