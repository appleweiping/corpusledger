"""Stable JSON normalization used by every fingerprint operation."""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass
from typing import Any, Literal

from .errors import CanonicalizationError
from .strictjson import MAX_INTEGER_DIGITS

CANONICAL_VERSION = "1"
ListStrategy = Literal["preserve", "sort"]
_INTEGER_LIMIT = 10**MAX_INTEGER_DIGITS


@dataclass(frozen=True)
class CanonicalPolicy:
    """Controls normalization without relying on process locale.

    Mapping keys are always sorted. List order is preserved by default because it
    commonly carries linguistic meaning. ``sort`` is available for set-like data.
    """

    unicode_form: Literal["NFC", "NFKC", "none"] = "NFC"
    list_strategy: ListStrategy = "preserve"
    sort_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Reject invalid runtime policy values that static typing cannot prevent."""
        if self.unicode_form not in {"NFC", "NFKC", "none"}:
            raise ValueError(f"unsupported Unicode normalization form: {self.unicode_form}")
        if self.list_strategy not in {"preserve", "sort"}:
            raise ValueError(f"unsupported list strategy: {self.list_strategy}")
        if not isinstance(self.sort_paths, tuple) or not all(
            isinstance(path, str) and path.strip() for path in self.sort_paths
        ):
            raise ValueError("sort_paths must be a tuple of non-empty dotted paths")
        if len(set(self.sort_paths)) != len(self.sort_paths):
            raise ValueError("sort_paths must not contain duplicates")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible policy description."""
        result: dict[str, Any] = {
            "list_strategy": self.list_strategy,
            "unicode_form": self.unicode_form,
        }
        if self.sort_paths:
            result["sort_paths"] = list(self.sort_paths)
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> CanonicalPolicy:
        """Load serialized policy metadata, normalizing JSON arrays to tuples."""

        payload = dict(value)
        if "sort_paths" in payload:
            paths = payload["sort_paths"]
            if not isinstance(paths, list):
                raise ValueError("sort_paths must be a JSON array")
            payload["sort_paths"] = tuple(paths)
        return cls(**payload)


def _normalize_string(value: str, policy: CanonicalPolicy, *, path: str) -> str:
    scalars: list[str] = []
    index = 0
    while index < len(value):
        code_point = ord(value[index])
        if 0xD800 <= code_point <= 0xDBFF:
            if index + 1 >= len(value):
                raise CanonicalizationError(f"{path}: unpaired UTF-16 surrogate is not a valid Unicode scalar value")
            low = ord(value[index + 1])
            if not 0xDC00 <= low <= 0xDFFF:
                raise CanonicalizationError(f"{path}: unpaired UTF-16 surrogate is not a valid Unicode scalar value")
            scalars.append(chr(0x10000 + ((code_point - 0xD800) << 10) + (low - 0xDC00)))
            index += 2
            continue
        if 0xDC00 <= code_point <= 0xDFFF:
            raise CanonicalizationError(f"{path}: unpaired UTF-16 surrogate is not a valid Unicode scalar value")
        scalars.append(value[index])
        index += 1
    value = "".join(scalars)
    if policy.unicode_form == "none":
        return value
    return unicodedata.normalize(policy.unicode_form, value)


def canonicalize(value: Any, policy: CanonicalPolicy | None = None, *, path: str = "$") -> Any:
    """Convert a supported Python value into a deterministic JSON value.

    Only strict JSON types are supported. Non-string mapping keys, non-finite
    numbers, tuples, dates, decimals, and arbitrary Python objects are rejected.
    """
    policy = policy or CanonicalPolicy()
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) >= _INTEGER_LIMIT:
            raise CanonicalizationError(f"{path}: integer exceeds the {MAX_INTEGER_DIGITS}-digit limit")
        return value
    if isinstance(value, str):
        return _normalize_string(value, policy, path=path)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError(f"{path}: NaN and infinity are not valid canonical JSON")
        return 0.0 if value == 0 else value
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError(f"{path}: mapping key {key!r} is not a string")
            normalized_key = _normalize_string(key, policy, path=f"{path} (object key)")
            if normalized_key in normalized:
                raise CanonicalizationError(f"{path}: keys collide after Unicode normalization: {key!r}")
            normalized[normalized_key] = canonicalize(item, policy, path=f"{path}.{normalized_key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, list):
        normalized_items = [canonicalize(item, policy, path=f"{path}[{i}]") for i, item in enumerate(value)]
        if policy.list_strategy == "sort" or _path_is_sorted(path, policy.sort_paths):
            normalized_items.sort(key=lambda item: canonical_json(item, policy))
        return normalized_items
    raise CanonicalizationError(f"{path}: unsupported value type {type(value).__name__}")


def _path_is_sorted(path: str, selectors: tuple[str, ...]) -> bool:
    """Match user-facing dotted paths against canonicalizer's diagnostic path."""

    return any(path == "$" + ("." + selector if not selector.startswith("$") else selector) for selector in selectors)


def canonical_json(value: Any, policy: CanonicalPolicy | None = None) -> str:
    """Serialize ``value`` as whitespace-free, stable UTF-8 JSON text."""
    import json

    policy = policy or CanonicalPolicy()
    normalized = canonicalize(value, policy)
    return json.dumps(normalized, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_bytes(value: Any, policy: CanonicalPolicy | None = None) -> bytes:
    """Return the canonical UTF-8 representation of ``value``."""
    return canonical_json(value, policy).encode("utf-8")
