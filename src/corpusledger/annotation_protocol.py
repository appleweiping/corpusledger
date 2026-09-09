"""Versioned, label-schema-aware contracts for independent annotation workers.

This protocol is independent of any other annotation platform. Workers return
only new annotations; the coordinator validates and constructs the successor.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .annotation_pipeline import AnnotationPipeline, AnnotationPipelineResult, AnnotationProcessor, AnnotationTransform
from .annotations import (
    MAX_ANNOTATIONS,
    AnnotationDocument,
    AnnotationField,
    AnnotationType,
    SpanAnnotation,
    _object,
)
from .errors import InputError
from .strictjson import bounded_int, finite_float, object_without_duplicates, reject_constant

PROCESSOR_FORMAT = "corpusledger.processor.v1"
REQUEST_FORMAT = "corpusledger.processor-request.v1"
RESULT_FORMAT = "corpusledger.processor-result.v1"
MAX_WIRE_BYTES = 16 * 1024 * 1024
MAX_DECLARED_TYPES = 128


class AnnotationProtocolError(InputError):
    """A remote annotation message violates its closed wire contract."""


def identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value) is None:
        raise AnnotationProtocolError(f"{label} must be a 1-128 character ASCII identifier")
    return value


def sha256_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise AnnotationProtocolError(f"{label} must be lowercase SHA-256 hexadecimal")
    return value


def _wire_values(value: Any, depth: int = 0) -> None:
    if depth > 64:
        raise AnnotationProtocolError("wire message exceeds 64 nesting levels")
    if value is None or type(value) is bool:
        return
    if isinstance(value, str):
        value.encode("utf-8")
    elif type(value) is int:
        bounded_int(str(value))
    elif type(value) is float:
        finite_float(repr(value))
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise AnnotationProtocolError("wire object keys must be strings")
            key.encode("utf-8")
            _wire_values(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _wire_values(item, depth + 1)
    else:
        raise AnnotationProtocolError("wire messages require JSON values")


def encode_wire(value: Any, *, limit: int = MAX_WIRE_BYTES) -> bytes:
    """Serialize strict JSON, retaining no more than the allowed output payload.

    Caller objects and an individual encoder chunk still occupy memory; this
    payload bound is not a process-memory limit or a parser sandbox.
    """
    if type(limit) is not int or not 1 <= limit <= MAX_WIRE_BYTES:
        raise AnnotationProtocolError("invalid wire byte limit")
    try:
        _wire_values(value)
        result = bytearray()
        encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        for chunk in encoder.iterencode(value):
            raw = chunk.encode("utf-8")
            if len(result) + len(raw) > limit:
                raise AnnotationProtocolError("wire message exceeds byte limit")
            result.extend(raw)
        return bytes(result)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise AnnotationProtocolError("wire message is invalid or exceeds its limits") from exc


def decode_wire(raw: bytes, *, limit: int = MAX_WIRE_BYTES) -> Any:
    if type(limit) is not int or not 1 <= limit <= MAX_WIRE_BYTES:
        raise AnnotationProtocolError("invalid wire byte limit")
    if not isinstance(raw, bytes) or len(raw) > limit:
        raise AnnotationProtocolError("wire message exceeds byte limit or is not bytes")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
            parse_int=bounded_int,
        )
        _wire_values(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise AnnotationProtocolError("wire message contains invalid or ambiguous JSON") from exc


def _schema(value: Any) -> AnnotationType:
    data = _object(value, {"name", "fields"}, "processor annotation type")
    if not isinstance(data["fields"], Mapping):
        raise AnnotationProtocolError("processor type fields must be an object")
    fields = {}
    for name, raw in data["fields"].items():
        spec = _object(raw, {"kind", "required", "nullable", "target_type"}, "processor field")
        fields[name] = AnnotationField(spec["kind"], spec["required"], spec["nullable"], spec["target_type"])
    return AnnotationType(data["name"], fields)


def _schemas(value: Any) -> tuple[AnnotationType, ...]:
    if not isinstance(value, list) or len(value) > MAX_DECLARED_TYPES:
        raise AnnotationProtocolError("processor schemas must be bounded arrays")
    return tuple(_schema(item) for item in value)


@dataclass(frozen=True, slots=True)
class ProcessorDescription:
    name: str
    version: str
    config_sha256: str
    requires: tuple[AnnotationType, ...] = ()
    produces: tuple[AnnotationType, ...] = ()

    def __post_init__(self) -> None:
        identifier(self.name, "processor name")
        identifier(self.version, "processor version")
        sha256_text(self.config_sha256, "processor configuration digest")
        for name in ("requires", "produces"):
            values = getattr(self, name)
            if (
                not isinstance(values, (tuple, list))
                or len(values) > MAX_DECLARED_TYPES
                or any(not isinstance(item, AnnotationType) for item in values)
            ):
                raise AnnotationProtocolError("processor declarations must contain bounded schema arrays")
            object.__setattr__(self, name, tuple(sorted(values, key=lambda item: item.name)))
        required = {item.name for item in self.requires}
        produced = {item.name for item in self.produces}
        if (
            not produced
            or len(required) != len(self.requires)
            or len(produced) != len(self.produces)
            or required & produced
        ):
            raise AnnotationProtocolError("processor types must be unique, disjoint and include outputs")
        for schema in (*self.requires, *self.produces):
            available = required if schema.name in required else required | produced
            if any(
                spec.target_type is not None and spec.target_type not in available for spec in schema.fields.values()
            ):
                raise AnnotationProtocolError("processor declarations omit a reference dependency")

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": PROCESSOR_FORMAT,
            "name": self.name,
            "version": self.version,
            "config_sha256": self.config_sha256,
            "requires": [item.to_dict() for item in self.requires],
            "produces": [item.to_dict() for item in self.produces],
        }

    @classmethod
    def from_dict(cls, value: Any) -> ProcessorDescription:
        data = _object(value, {"format", "name", "version", "config_sha256", "requires", "produces"}, "processor")
        if data["format"] != PROCESSOR_FORMAT:
            raise AnnotationProtocolError("unsupported processor protocol")
        return cls(
            data["name"], data["version"], data["config_sha256"], _schemas(data["requires"]), _schemas(data["produces"])
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(encode_wire(self.to_dict())).hexdigest()

    def as_processor(self, callback: AnnotationTransform) -> AnnotationProcessor:
        return AnnotationProcessor(self.name, self.version, callback, self.requires, self.produces)


@dataclass(frozen=True, slots=True)
class AnnotationRequest:
    operation_id: str
    step_id: str
    processor: ProcessorDescription
    document: AnnotationDocument

    def __post_init__(self) -> None:
        identifier(self.operation_id, "operation ID")
        identifier(self.step_id, "step ID")
        if not isinstance(self.processor, ProcessorDescription) or not isinstance(self.document, AnnotationDocument):
            raise AnnotationProtocolError("request requires a processor description and annotation document")
        AnnotationPipeline((self.processor.as_processor(lambda _document: ()),)).plan(self.document)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": REQUEST_FORMAT,
            "operation_id": self.operation_id,
            "step_id": self.step_id,
            "processor": self.processor.to_dict(),
            "input_digest": self.document.digest,
            "document": self.document.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Any) -> AnnotationRequest:
        data = _object(value, {"format", "operation_id", "step_id", "processor", "input_digest", "document"}, "request")
        if data["format"] != REQUEST_FORMAT:
            raise AnnotationProtocolError("unsupported annotation request protocol")
        result = cls(
            data["operation_id"],
            data["step_id"],
            ProcessorDescription.from_dict(data["processor"]),
            AnnotationDocument.from_dict(data["document"]),
        )
        if data["input_digest"] != result.document.digest:
            raise AnnotationProtocolError("request input digest does not match its document")
        return result

    @property
    def digest(self) -> str:
        return hashlib.sha256(encode_wire(self.to_dict())).hexdigest()


@dataclass(frozen=True, slots=True)
class AnnotationResponse:
    operation_id: str
    step_id: str
    processor: ProcessorDescription
    input_digest: str
    annotations: tuple[SpanAnnotation, ...]
    duration_ms: int = 0

    def __post_init__(self) -> None:
        identifier(self.operation_id, "operation ID")
        identifier(self.step_id, "step ID")
        sha256_text(self.input_digest, "response input digest")
        if not isinstance(self.processor, ProcessorDescription):
            raise AnnotationProtocolError("response requires a processor description")
        if type(self.duration_ms) is not int or not 0 <= self.duration_ms < 2**63:
            raise AnnotationProtocolError("duration_ms must be a non-negative bounded integer")
        if (
            not isinstance(self.annotations, (list, tuple))
            or len(self.annotations) > MAX_ANNOTATIONS
            or any(not isinstance(item, SpanAnnotation) for item in self.annotations)
        ):
            raise AnnotationProtocolError("response annotations must be a bounded annotation array")
        object.__setattr__(self, "annotations", tuple(self.annotations))

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": RESULT_FORMAT,
            "operation_id": self.operation_id,
            "step_id": self.step_id,
            "processor": self.processor.to_dict(),
            "input_digest": self.input_digest,
            "annotations": [item.to_dict() for item in self.annotations],
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, value: Any) -> AnnotationResponse:
        data = _object(
            value,
            {"format", "operation_id", "step_id", "processor", "input_digest", "annotations", "duration_ms"},
            "processor response",
        )
        if data["format"] != RESULT_FORMAT:
            raise AnnotationProtocolError("unsupported annotation result protocol")
        if not isinstance(data["annotations"], list) or len(data["annotations"]) > MAX_ANNOTATIONS:
            raise AnnotationProtocolError("response annotations must be a bounded array")
        annotations = []
        for item in data["annotations"]:
            raw = _object(item, {"id", "type", "start", "end", "features"}, "response annotation")
            annotations.append(SpanAnnotation(raw["id"], raw["type"], raw["start"], raw["end"], raw["features"]))
        return cls(
            data["operation_id"],
            data["step_id"],
            ProcessorDescription.from_dict(data["processor"]),
            data["input_digest"],
            tuple(annotations),
            data["duration_ms"],
        )

    def apply(self, request: AnnotationRequest) -> AnnotationPipelineResult:
        """Bind a response to its reserved input before validating its new layers."""
        if (
            self.operation_id != request.operation_id
            or self.step_id != request.step_id
            or self.processor != request.processor
            or self.input_digest != request.document.digest
        ):
            raise AnnotationProtocolError("response identity does not match the reserved request")
        processor = self.processor.as_processor(lambda _document: self.annotations)
        return AnnotationPipeline((processor,)).run(request.document)
