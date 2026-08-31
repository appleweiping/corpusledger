"""Conservative, path-based schema inference for heterogeneous corpora."""

from __future__ import annotations

from collections.abc import Iterable
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


def infer_schema(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Infer field presence and observed types across records."""
    materialized = list(records)
    fields: dict[str, FieldSummary] = {}
    for record in materialized:
        _walk(record, "", fields)
    return {
        "record_count": len(materialized),
        "fields": {path: fields[path].to_dict(len(materialized)) for path in sorted(fields)},
    }


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
