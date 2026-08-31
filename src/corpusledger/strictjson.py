"""Strict JSON decoder hooks shared by corpus and manifest readers."""

from __future__ import annotations

from typing import Any, NoReturn


class StrictJsonError(ValueError):
    """Raised for ambiguous extensions accepted by Python's JSON decoder."""


def object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Construct an object while rejecting repeated member names."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StrictJsonError(f"duplicate object key {key!r}")
        result[key] = value
    return result


def reject_constant(value: str) -> NoReturn:
    """Reject NaN and infinity extensions accepted by ``json`` by default."""
    raise StrictJsonError(f"non-finite number {value!r} is not valid JSON")
