"""Immutable, typed text annotations with explicit Unicode offset semantics.

Offsets are Python Unicode code points in half-open intervals, never normalized
text offsets. This is a local document model, not a distributed annotation service.
"""

from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left, bisect_right
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .errors import InputError
from .strictjson import MAX_INTEGER_DIGITS

ANNOTATION_FORMAT = "corpusledger.annotations.v1"
MAX_TEXT_CODEPOINTS = 10_000_000
MAX_ANNOTATIONS = 1_000_000
_MAX_INTEGER_MAGNITUDE = 10**MAX_INTEGER_DIGITS
_KINDS = frozenset(("string", "integer", "number", "boolean", "object", "array", "reference", "references"))


def _name(value: Any, description: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise InputError(f"{description} must be a non-empty string of at most 256 code points")
    _unicode(value)


def _unicode(text: str) -> None:
    try:
        text.encode("utf-8")
    except UnicodeError as exc:
        raise InputError("annotation text and names must not contain unpaired surrogates") from exc


def _freeze(value: Any, depth: int = 0) -> Any:
    if depth > 32:
        raise InputError("annotation features exceed 32 levels of nesting")
    if type(value) is int:
        if abs(value) >= _MAX_INTEGER_MAGNITUDE:
            raise InputError(f"annotation integers exceed the {MAX_INTEGER_DIGITS}-digit limit")
        try:
            str(value)
        except ValueError as exc:
            raise InputError("annotation integer exceeds the active Python digit limit") from exc
        return value
    if value is None or type(value) is bool:
        return value
    if isinstance(value, str):
        _unicode(value)
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise InputError("annotation feature object keys must be strings")
        for key in value:
            _unicode(key)
        return MappingProxyType({key: _freeze(item, depth + 1) for key, item in sorted(value.items())})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item, depth + 1) for item in value)
    raise InputError("annotation features must be finite JSON values")


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _object(value: Any, keys: set[str], description: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise InputError(f"{description} must have exactly these fields: {', '.join(sorted(keys))}")
    return value


def _interval(start: int, end: int, length: int | None = None) -> None:
    if type(start) is not int or type(end) is not int or start < 0 or end < start:
        raise InputError("annotation offsets must be integers satisfying 0 <= start <= end")
    if length is not None and end > length:
        raise InputError("annotation end exceeds document length")


@dataclass(frozen=True, slots=True)
class AnnotationField:
    """Non-coercing scalar/container type, or an intra-document annotation reference."""

    kind: str = "string"
    required: bool = True
    nullable: bool = False
    target_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or self.kind not in _KINDS:
            raise InputError("unsupported annotation field kind")
        if type(self.required) is not bool or type(self.nullable) is not bool:
            raise InputError("annotation field required/nullable flags must be booleans")
        if self.target_type is not None:
            _name(self.target_type, "reference target type")
            if self.kind not in ("reference", "references"):
                raise InputError("target_type is only valid for reference fields")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "required": self.required,
            "nullable": self.nullable,
            "target_type": self.target_type,
        }


@dataclass(frozen=True, slots=True)
class AnnotationType:
    """A named closed feature schema; undeclared features are rejected."""

    name: str
    fields: Mapping[str, AnnotationField] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _name(self.name, "annotation type name")
        if not isinstance(self.fields, Mapping):
            raise InputError("annotation type fields must be a mapping")
        for name, spec in self.fields.items():
            _name(name, "annotation field name")
            if not isinstance(spec, AnnotationField):
                raise InputError("annotation type fields must contain AnnotationField values")
        object.__setattr__(self, "fields", MappingProxyType(dict(sorted(self.fields.items()))))

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "fields": {name: spec.to_dict() for name, spec in self.fields.items()}}


@dataclass(frozen=True, slots=True)
class SpanAnnotation:
    """One stable ID, type and half-open span; empty spans represent anchors."""

    annotation_id: str
    type_name: str
    start: int
    end: int
    features: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _name(self.annotation_id, "annotation ID")
        _name(self.type_name, "annotation type name")
        _interval(self.start, self.end)
        if not isinstance(self.features, Mapping):
            raise InputError("annotation features must be a mapping")
        object.__setattr__(self, "features", _freeze(self.features))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.annotation_id,
            "type": self.type_name,
            "start": self.start,
            "end": self.end,
            "features": _thaw(self.features),
        }


def _validate_features(annotation: SpanAnnotation, schema: AnnotationType, by_id: Mapping[str, SpanAnnotation]) -> None:
    unknown = set(annotation.features) - set(schema.fields)
    if unknown:
        raise InputError(f"annotation {annotation.annotation_id!r} has undeclared features: {sorted(unknown)}")
    for name, spec in schema.fields.items():
        if name not in annotation.features:
            if spec.required:
                raise InputError(f"annotation {annotation.annotation_id!r} is missing required feature {name!r}")
            continue
        value = annotation.features[name]
        if value is None and spec.nullable:
            continue
        valid = {
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
            "object": isinstance(value, Mapping),
            "array": isinstance(value, tuple),
            "reference": isinstance(value, str),
            "references": isinstance(value, tuple) and all(isinstance(item, str) for item in value),
        }[spec.kind]
        if not valid:
            raise InputError(f"feature {name!r} on annotation {annotation.annotation_id!r} must have type {spec.kind}")
        if spec.kind in ("reference", "references"):
            references = (value,) if spec.kind == "reference" else value
            for target in references:
                if target not in by_id:
                    raise InputError(f"annotation {annotation.annotation_id!r} has dangling reference {target!r}")
                if spec.target_type is not None and by_id[target].type_name != spec.target_type:
                    raise InputError(f"reference {target!r} does not target annotation type {spec.target_type!r}")


@dataclass(frozen=True, slots=True)
class AnnotationDocument:
    """Validated immutable snapshot. Text changes require constructing a new document."""

    document_id: str
    text: str
    annotation_types: tuple[AnnotationType, ...] = ()
    annotations: tuple[SpanAnnotation, ...] = ()
    _by_id: Mapping[str, SpanAnnotation] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _name(self.document_id, "document ID")
        if not isinstance(self.text, str) or len(self.text) > MAX_TEXT_CODEPOINTS:
            raise InputError(f"document text must be a string of at most {MAX_TEXT_CODEPOINTS} code points")
        _unicode(self.text)
        if not isinstance(self.annotation_types, (list, tuple)) or not isinstance(self.annotations, (list, tuple)):
            raise InputError("document types and annotations must be tuples or lists")
        if len(self.annotations) > MAX_ANNOTATIONS:
            raise InputError(f"document exceeds {MAX_ANNOTATIONS} annotations")
        types = tuple(self.annotation_types)
        annotations = tuple(self.annotations)
        schemas: dict[str, AnnotationType] = {}
        for schema in types:
            if not isinstance(schema, AnnotationType) or schema.name in schemas:
                raise InputError("document types must be AnnotationType values with unique names")
            schemas[schema.name] = schema
        for schema in types:
            for spec in schema.fields.values():
                if spec.target_type is not None and spec.target_type not in schemas:
                    raise InputError(f"unknown reference target type {spec.target_type!r}")
        by_id: dict[str, SpanAnnotation] = {}
        for annotation in annotations:
            if not isinstance(annotation, SpanAnnotation) or annotation.annotation_id in by_id:
                raise InputError("document annotations must be SpanAnnotation values with unique IDs")
            if annotation.type_name not in schemas:
                raise InputError(f"unknown annotation type {annotation.type_name!r}")
            _interval(annotation.start, annotation.end, len(self.text))
            by_id[annotation.annotation_id] = annotation
        for annotation in annotations:
            _validate_features(annotation, schemas[annotation.type_name], by_id)
        object.__setattr__(self, "annotation_types", tuple(sorted(types, key=lambda item: item.name)))
        object.__setattr__(self, "annotations", tuple(sorted(annotations, key=_span_key)))
        object.__setattr__(self, "_by_id", MappingProxyType(by_id))

    @property
    def text_sha256(self) -> str:
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()

    @property
    def digest(self) -> str:
        rendered = json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        return hashlib.sha256(rendered.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": ANNOTATION_FORMAT,
            "id": self.document_id,
            "text": self.text,
            "text_sha256": self.text_sha256,
            "offset_unit": "unicode_codepoint",
            "types": [schema.to_dict() for schema in self.annotation_types],
            "annotations": [annotation.to_dict() for annotation in self.annotations],
        }

    @classmethod
    def from_dict(cls, value: Any) -> AnnotationDocument:
        data = _object(
            value, {"format", "id", "text", "text_sha256", "offset_unit", "types", "annotations"}, "document"
        )
        if data["format"] != ANNOTATION_FORMAT or data["offset_unit"] != "unicode_codepoint":
            raise InputError("unsupported annotation format or offset unit")
        if not isinstance(data["types"], list) or not isinstance(data["annotations"], list):
            raise InputError("document types and annotations must be arrays")
        if len(data["annotations"]) > MAX_ANNOTATIONS:
            raise InputError(f"document exceeds {MAX_ANNOTATIONS} annotations")
        types = []
        for item in data["types"]:
            schema = _object(item, {"name", "fields"}, "annotation type")
            if not isinstance(schema["fields"], Mapping):
                raise InputError("annotation type fields must be an object")
            specs = {}
            for name, raw in schema["fields"].items():
                spec = _object(raw, {"kind", "required", "nullable", "target_type"}, "annotation field")
                specs[name] = AnnotationField(spec["kind"], spec["required"], spec["nullable"], spec["target_type"])
            types.append(AnnotationType(schema["name"], specs))
        annotations = []
        for item in data["annotations"]:
            raw = _object(item, {"id", "type", "start", "end", "features"}, "annotation")
            annotations.append(SpanAnnotation(raw["id"], raw["type"], raw["start"], raw["end"], raw["features"]))
        result = cls(data["id"], data["text"], tuple(types), tuple(annotations))
        if data["text_sha256"] != result.text_sha256:
            raise InputError("annotation document text digest does not match")
        return result

    def get(self, annotation_id: str) -> SpanAnnotation:
        """Look up a stable annotation ID; raises KeyError for an unknown ID."""
        return self._by_id[annotation_id]

    def span_text(self, annotation_id: str) -> str:
        annotation = self.get(annotation_id)
        return self.text[annotation.start : annotation.end]

    def index(self, type_name: str | None = None) -> AnnotationIndex:
        if type_name is not None and type_name not in {schema.name for schema in self.annotation_types}:
            raise InputError(f"unknown annotation type {type_name!r}")
        selected = tuple(item for item in self.annotations if type_name is None or item.type_name == type_name)
        return AnnotationIndex(selected, len(self.text))


def _span_key(annotation: SpanAnnotation) -> tuple[int, int, str]:
    return annotation.start, annotation.end, annotation.annotation_id


@dataclass(frozen=True, slots=True)
class AnnotationIndex:
    """Sorted span index. Start-position pruning is O(log n); candidate filtering O(n) worst case."""

    annotations: tuple[SpanAnnotation, ...]
    text_length: int
    _starts: tuple[int, ...] = field(init=False, repr=False)
    _prefix_ends: tuple[int, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _interval(0, self.text_length)
        if not isinstance(self.annotations, (list, tuple)):
            raise InputError("index annotations must be a tuple or list")
        selected = tuple(self.annotations)
        if any(not isinstance(item, SpanAnnotation) for item in selected):
            raise InputError("index entries must be SpanAnnotation values")
        selected = tuple(sorted(selected, key=_span_key))
        prefix = []
        maximum = 0
        for item in selected:
            _interval(item.start, item.end, self.text_length)
            maximum = max(maximum, item.end)
            prefix.append(maximum)
        object.__setattr__(self, "annotations", selected)
        object.__setattr__(self, "_starts", tuple(item.start for item in selected))
        object.__setattr__(self, "_prefix_ends", tuple(prefix))

    def query(self, start: int, end: int, *, relation: str = "overlapping") -> tuple[SpanAnnotation, ...]:
        """Select exact/inside/covering/overlapping spans in stable (start,end,ID) order.

        Empty anchors never overlap. A nonempty span covers a point at its start
        but not its end; an anchor covers itself. Inside includes anchors at both
        bounds, because an empty span is a valid interval subset.
        """
        _interval(start, end, self.text_length)
        if relation == "exact":
            low, high = bisect_left(self._starts, start), bisect_right(self._starts, start)
            return tuple(item for item in self.annotations[low:high] if item.end == end)
        if relation == "inside":
            low, high = bisect_left(self._starts, start), bisect_right(self._starts, end)
            return tuple(item for item in self.annotations[low:high] if item.end <= end)
        if relation == "covering":
            high = bisect_right(self._starts, start)
            low = bisect_left(self._prefix_ends, end, 0, high)
            return tuple(
                item
                for item in self.annotations[low:high]
                if item.end >= end and (start != end or item.end > end or item.start == item.end == start)
            )
        if relation == "overlapping":
            if start == end:
                return ()
            low = bisect_right(self._prefix_ends, start)
            high = bisect_left(self._starts, end)
            return tuple(item for item in self.annotations[low:high] if item.end > start and item.start < item.end)
        raise InputError("relation must be exact, inside, covering or overlapping")


def codepoint_to_utf16(text: str, offset: int) -> int:
    """Convert a Unicode code-point boundary to a Java/JavaScript UTF-16 offset."""
    if not isinstance(text, str):
        raise InputError("text must be a string")
    _unicode(text)
    _interval(0, offset, len(text))
    return sum(2 if ord(character) > 0xFFFF else 1 for character in text[:offset])


def utf16_to_codepoint(text: str, offset: int) -> int:
    """Convert UTF-16 boundaries, rejecting offsets that split surrogate pairs."""
    if not isinstance(text, str):
        raise InputError("text must be a string")
    _unicode(text)
    _interval(0, offset)
    units = 0
    for index, character in enumerate(text):
        if units == offset:
            return index
        units += 2 if ord(character) > 0xFFFF else 1
        if units > offset:
            raise InputError("UTF-16 offset splits a surrogate pair")
    if units != offset:
        raise InputError("UTF-16 offset exceeds text length")
    return len(text)
