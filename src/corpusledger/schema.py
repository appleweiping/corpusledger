"""Conservative, path-based schema inference for heterogeneous corpora."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .paths import join_pointer


def value_type(value: Any) -> str:
    """Return a non-coercing JSON-oriented type label."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


@dataclass
class FieldSummary:
    """Observed shape of one JSON Pointer field path."""

    present: int = 0
    nulls: int = 0
    types: set[str] = field(default_factory=set)
    item_types: set[str] = field(default_factory=set)
    object_keys: set[str] = field(default_factory=set)
    min_items: int | None = None
    max_items: int | None = None

    def observe(self, value: Any) -> None:
        """Add one observation without guessing semantic types."""
        self.present += 1
        kind = value_type(value)
        self.types.add(kind)
        if value is None:
            self.nulls += 1
        elif isinstance(value, list):
            size = len(value)
            self.min_items = size if self.min_items is None else min(self.min_items, size)
            self.max_items = size if self.max_items is None else max(self.max_items, size)
            self.item_types.update(value_type(item) for item in value)
        elif isinstance(value, dict):
            self.object_keys.update(str(key) for key in value)

    def to_dict(self, total_records: int) -> dict[str, Any]:
        """Serialize with sorted collections and explicit optionality."""
        result: dict[str, Any] = {
            "item_types": sorted(self.item_types),
            "nullable": self.nulls > 0,
            "object_keys": sorted(self.object_keys),
            "optional": self.present < total_records,
            "present": self.present,
            "types": sorted(self.types),
        }
        if self.min_items is not None:
            result["min_items"] = self.min_items
            result["max_items"] = self.max_items
        return result


def _walk(value: Any, path: str, fields: dict[str, FieldSummary]) -> None:
    if path:
        fields.setdefault(path, FieldSummary()).observe(value)
    if isinstance(value, dict):
        for key in sorted(value):
            child = join_pointer(path, str(key))
            _walk(value[key], child, fields)


@dataclass
class SchemaAccumulator:
    """Incrementally collect the observed schema without retaining records."""

    record_count: int = 0
    fields: dict[str, FieldSummary] = field(default_factory=dict)

    def observe(self, record: dict[str, Any]) -> None:
        """Add one normalized JSON object."""

        self.record_count += 1
        _walk(record, "", self.fields)

    def to_dict(self) -> dict[str, Any]:
        """Return the same format produced by :func:`infer_schema`."""

        return {
            "record_count": self.record_count,
            "fields": {path: self.fields[path].to_dict(self.record_count) for path in sorted(self.fields)},
        }


def infer_schema(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Infer field presence and observed types across records."""
    accumulator = SchemaAccumulator()
    for record in records:
        accumulator.observe(record)
    return accumulator.to_dict()


def schema_drift(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Classify added, removed, and changed field summaries."""
    old_fields = before.get("fields", {})
    new_fields = after.get("fields", {})
    old_names, new_names = set(old_fields), set(new_fields)
    changed = {
        name: {"before": old_fields[name], "after": new_fields[name]}
        for name in sorted(old_names & new_names)
        if old_fields[name] != new_fields[name]
    }
    return {
        "added_fields": sorted(new_names - old_names),
        "removed_fields": sorted(old_names - new_names),
        "changed_fields": changed,
    }


def to_json_schema(
    inferred: Mapping[str, Any],
    *,
    title: str | None = None,
    schema_id: str | None = None,
) -> dict[str, Any]:
    """Translate an inferred schema to draft-2020-12 JSON Schema.

    Inferred schemas record observations rather than application semantics.
    The exporter consequently preserves mixed JSON types, marks observed
    non-optional fields as required, and leaves object additional properties
    open.  It is intended for downstream validation and editor tooling without
    changing the authenticated manifest format.
    """

    if not isinstance(inferred, Mapping):
        raise TypeError("inferred schema must be an object")
    fields = inferred.get("fields")
    record_count = inferred.get("record_count")
    if isinstance(record_count, bool) or not isinstance(record_count, int) or record_count < 0:
        raise ValueError("schema record_count must be a non-negative integer")
    if not isinstance(fields, Mapping):
        raise ValueError("schema fields must be an object")

    root: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
    }
    if title is not None:
        if not isinstance(title, str) or not title.strip():
            raise ValueError("schema title must be a non-empty string")
        root["title"] = title
    if schema_id is not None:
        if not isinstance(schema_id, str) or not schema_id.strip():
            raise ValueError("schema id must be a non-empty string")
        root["$id"] = schema_id

    root_properties: dict[str, Any] = {}
    root_required: set[str] = set()
    for raw_path, summary in sorted(fields.items()):
        if not isinstance(raw_path, str) or not raw_path.startswith("/"):
            raise ValueError("schema field paths must be JSON Pointers")
        if not isinstance(summary, Mapping):
            raise ValueError(f"schema field {raw_path!r} must be an object")
        parts = tuple(_pointer_part(part) for part in raw_path.split("/")[1:])
        if not parts or any(not part for part in parts):
            raise ValueError(f"schema field path {raw_path!r} is invalid")
        _insert_field(root_properties, root_required, parts, summary)
    if root_properties:
        root["properties"] = root_properties
    if root_required:
        root["required"] = sorted(root_required)
    return root


def _pointer_part(value: str) -> str:
    return value.replace("~1", "/").replace("~0", "~")


def _insert_field(
    properties: dict[str, Any],
    required: set[str],
    parts: tuple[str, ...],
    summary: Mapping[str, Any],
) -> None:
    name = parts[0]
    node = properties.setdefault(name, {"type": "object"})
    if len(parts) == 1:
        node.clear()
        node.update(_summary_schema(summary))
        if summary.get("optional") is False:
            required.add(name)
        return
    node.setdefault("type", "object")
    child_properties = node.setdefault("properties", {})
    child_required = set(node.get("required", []))
    if not isinstance(child_properties, dict):
        raise ValueError(f"schema path collision at {name!r}")
    _insert_field(child_properties, child_required, parts[1:], summary)
    if child_required:
        node["required"] = sorted(child_required)
    node.setdefault("additionalProperties", True)


def _summary_schema(summary: Mapping[str, Any]) -> dict[str, Any]:
    types = summary.get("types")
    if not isinstance(types, list) or not types or not all(isinstance(item, str) for item in types):
        raise ValueError("schema field types must be a non-empty string array")
    supported = {"null", "boolean", "integer", "number", "string", "array", "object"}
    if any(item not in supported for item in types):
        raise ValueError("schema field contains an unsupported JSON type")
    result: dict[str, Any] = {"type": types[0] if len(types) == 1 else sorted(types)}
    item_types = summary.get("item_types", [])
    if "array" in types and item_types:
        if not isinstance(item_types, list) or not all(isinstance(item, str) for item in item_types):
            raise ValueError("schema array item_types must be a string array")
        result["items"] = {"type": item_types[0] if len(item_types) == 1 else sorted(item_types)}
    object_keys = summary.get("object_keys", [])
    if "object" in types and object_keys:
        if not isinstance(object_keys, list) or not all(isinstance(item, str) for item in object_keys):
            raise ValueError("schema object_keys must be a string array")
        result["properties"] = {key: {} for key in sorted(object_keys)}
        result["additionalProperties"] = True
    for key in ("min_items", "max_items"):
        if key in summary:
            value = summary[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"schema {key} must be a non-negative integer")
            result["minItems" if key == "min_items" else "maxItems"] = value
    return result


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    """One deterministic JSON Schema subset validation finding."""

    record: int
    path: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {"record": self.record, "path": self.path, "message": self.message}


def validate_json_schema(
    values: Iterable[Any], schema: Mapping[str, Any] | bool, *, max_errors: int = 100
) -> tuple[SchemaValidationIssue, ...]:
    """Validate records against a dependency-free, useful draft-schema subset.

    Supported keywords are ``type``, ``enum``, ``const``, ``required``,
    ``properties``, ``additionalProperties: false``, ``items``, array size,
    string length/pattern, numeric bounds, ``anyOf`` and ``allOf``. Unknown
    keywords are ignored, which keeps this validator conservative and makes it
    suitable for checking schemas produced by :func:`to_json_schema`.
    """

    if isinstance(schema, bool):
        if schema:
            return ()
        return tuple(SchemaValidationIssue(index, "", "schema is false") for index, _ in enumerate(values, start=1))
    if not isinstance(schema, Mapping):
        raise TypeError("JSON Schema must be an object or boolean")
    if isinstance(max_errors, bool) or not isinstance(max_errors, int) or max_errors < 1:
        raise ValueError("max_errors must be a positive integer")
    issues: list[SchemaValidationIssue] = []
    for index, value in enumerate(values, start=1):
        for path, message in _validate_value(value, schema, ""):
            issues.append(SchemaValidationIssue(index, path, message))
            if len(issues) >= max_errors:
                return tuple(issues)
    return tuple(issues)


def _validate_value(value: Any, schema: Mapping[str, Any], path: str) -> tuple[tuple[str, str], ...]:
    findings: list[tuple[str, str]] = []
    expected = schema.get("type")
    if expected is not None and not _matches_type(value, expected):
        return ((path, f"expected type {expected!r}, got {value_type(value)!r}"),)
    if "const" in schema and value != schema["const"]:
        findings.append((path, "value does not match const"))
    enum = schema.get("enum")
    if enum is not None and (not isinstance(enum, list) or value not in enum):
        findings.append((path, "value is not in enum"))
    any_of = schema.get("anyOf")
    if (
        isinstance(any_of, list)
        and any_of
        and not any(isinstance(option, Mapping) and not _validate_value(value, option, path) for option in any_of)
    ):
        findings.append((path, "value does not match anyOf"))
    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        for option in all_of:
            if isinstance(option, Mapping):
                findings.extend(_validate_value(value, option, path))
    if isinstance(value, dict):
        required = schema.get("required", [])
        if isinstance(required, list):
            for name in required:
                if isinstance(name, str) and name not in value:
                    findings.append((_child_path(path, name), "required property is missing"))
        properties = schema.get("properties", {})
        if not isinstance(properties, Mapping):
            properties = {}
        additional = schema.get("additionalProperties", True)
        for name, item in value.items():
            child = _child_path(path, str(name))
            if name in properties and isinstance(properties[name], Mapping):
                findings.extend(_validate_value(item, properties[name], child))
            elif additional is False:
                findings.append((child, "additional property is not allowed"))
            elif isinstance(additional, Mapping):
                findings.extend(_validate_value(item, additional, child))
    elif isinstance(value, list):
        minimum = schema.get("minItems")
        maximum = schema.get("maxItems")
        if isinstance(minimum, int) and len(value) < minimum:
            findings.append((path, f"array has fewer than {minimum} items"))
        if isinstance(maximum, int) and len(value) > maximum:
            findings.append((path, f"array has more than {maximum} items"))
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for index, item in enumerate(value):
                findings.extend(_validate_value(item, item_schema, _child_path(path, str(index))))
    elif isinstance(value, str):
        minimum = schema.get("minLength")
        maximum = schema.get("maxLength")
        if isinstance(minimum, int) and len(value) < minimum:
            findings.append((path, f"string is shorter than {minimum} characters"))
        if isinstance(maximum, int) and len(value) > maximum:
            findings.append((path, f"string is longer than {maximum} characters"))
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, value) is None:
            findings.append((path, "string does not match pattern"))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if isinstance(minimum, (int, float)) and value < minimum:
            findings.append((path, f"number is below minimum {minimum}"))
        if isinstance(maximum, (int, float)) and value > maximum:
            findings.append((path, f"number is above maximum {maximum}"))
    return tuple(findings)


def _matches_type(value: Any, expected: Any) -> bool:
    expected_types = [expected] if isinstance(expected, str) else expected
    if not isinstance(expected_types, list) or not all(isinstance(item, str) for item in expected_types):
        return False
    return any(
        (kind == "null" and value is None)
        or (kind == "boolean" and isinstance(value, bool))
        or (kind == "integer" and isinstance(value, int) and not isinstance(value, bool))
        or (kind == "number" and isinstance(value, (int, float)) and not isinstance(value, bool))
        or (kind == "string" and isinstance(value, str))
        or (kind == "array" and isinstance(value, list))
        or (kind == "object" and isinstance(value, dict))
        for kind in expected_types
    )


def _child_path(path: str, part: str) -> str:
    escaped = part.replace("~", "~0").replace("/", "~1")
    return f"{path}/{escaped}"
