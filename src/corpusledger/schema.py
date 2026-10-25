"""Which fields a dataset has, how often, and with which JSON types."""

from __future__ import annotations

from typing import Any

from .paths import join_pointer


def value_type(value: Any) -> str:
    """JSON type name of a value (``integer`` and ``number`` are kept apart)."""
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


class SchemaAccumulator:
    """Count, per JSON Pointer path, how many records have it and with which types."""

    def __init__(self) -> None:
        self.records = 0
        self.fields: dict[str, dict[str, int]] = {}

    def observe(self, record: dict[str, Any]) -> None:
        self.records += 1
        self._walk(record, "")

    def _walk(self, value: Any, path: str) -> None:
        if path:
            types = self.fields.setdefault(path, {})
            kind = value_type(value)
            types[kind] = types.get(kind, 0) + 1
        if isinstance(value, dict):
            for key in value:
                self._walk(value[key], join_pointer(path, str(key)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": self.records,
            "fields": {
                path: {"present": sum(types.values()), "types": dict(sorted(types.items()))}
                for path, types in sorted(self.fields.items())
            },
        }


def schema_drift(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Fields that appeared, disappeared, or changed their set of JSON types."""
    old = before.get("fields", {})
    new = after.get("fields", {})
    retyped = {
        path: {"before": sorted(old[path]["types"]), "after": sorted(new[path]["types"])}
        for path in sorted(old.keys() & new.keys())
        if set(old[path]["types"]) != set(new[path]["types"])
    }
    return {
        "added": sorted(new.keys() - old.keys()),
        "removed": sorted(old.keys() - new.keys()),
        "retyped": retyped,
    }
