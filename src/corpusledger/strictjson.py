"""Strict JSON decoder hooks shared by corpus and manifest readers."""

from __future__ import annotations

import math
from typing import Any, NoReturn

MAX_INTEGER_DIGITS = 4_300


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


def finite_float(value: str) -> float:
    """Decode a JSON number only when it fits Python's finite binary64 range."""

    result = float(value)
    if not math.isfinite(result):
        raise StrictJsonError("JSON number is outside the finite binary64 range")
    return result


def bounded_int(value: str) -> int:
    """Decode integers under a stable cross-version resource limit."""

    digits = value[1:] if value.startswith("-") else value
    if len(digits) > MAX_INTEGER_DIGITS:
        raise StrictJsonError(f"JSON integer exceeds the {MAX_INTEGER_DIGITS}-digit limit")
    try:
        return int(value)
    except ValueError as exc:
        raise StrictJsonError("JSON integer cannot be decoded under the active Python limit") from exc
