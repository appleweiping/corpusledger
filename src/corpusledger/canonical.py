"""Stable JSON normalization used by every fingerprint operation."""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass
from typing import Any, Literal

from .errors import CanonicalizationError

CANONICAL_VERSION = "1"
ListStrategy = Literal["preserve", "sort"]


@dataclass(frozen=True)
class CanonicalPolicy:
    """Controls normalization without relying on process locale.

    Mapping keys are always sorted. List order is preserved by default because it
    commonly carries linguistic meaning. ``sort`` is available for set-like data.
    """

    unicode_form: Literal["NFC", "NFKC", "none"] = "NFC"
    list_strategy: ListStrategy = "preserve"

    def __post_init__(self) -> None:
        """Reject invalid runtime policy values that static typing cannot prevent."""
        if self.unicode_form not in {"NFC", "NFKC", "none"}:
            raise ValueError(f"unsupported Unicode normalization form: {self.unicode_form}")
        if self.list_strategy not in {"preserve", "sort"}:
            raise ValueError(f"unsupported list strategy: {self.list_strategy}")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible policy description."""
        return {
            "list_strategy": self.list_strategy,
            "unicode_form": self.unicode_form,
        }


def _normalize_string(value: str, policy: CanonicalPolicy) -> str:
    if policy.unicode_form == "none":
        return value
    return unicodedata.normalize(policy.unicode_form, value)


def canonicalize(value: Any, policy: CanonicalPolicy | None = None, *, path: str = "$") -> Any:
    """Convert a supported Python value into a deterministic JSON value.

    Only strict JSON types are supported. Non-string mapping keys, non-finite
    numbers, tuples, dates, decimals, and arbitrary Python objects are rejected.
    """
    policy = policy or CanonicalPolicy()
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return _normalize_string(value, policy)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError(f"{path}: NaN and infinity are not valid canonical JSON")
        return 0.0 if value == 0 else value
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError(f"{path}: mapping key {key!r} is not a string")
            normalized_key = _normalize_string(key, policy)
            if normalized_key in normalized:
                raise CanonicalizationError(f"{path}: keys collide after Unicode normalization: {key!r}")
            normalized[normalized_key] = canonicalize(item, policy, path=f"{path}.{normalized_key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, list):
        normalized_items = [canonicalize(item, policy, path=f"{path}[{i}]") for i, item in enumerate(value)]
        if policy.list_strategy == "sort":
            normalized_items.sort(key=lambda item: canonical_json(item, policy))
        return normalized_items
    raise CanonicalizationError(f"{path}: unsupported value type {type(value).__name__}")


def canonical_json(value: Any, policy: CanonicalPolicy | None = None) -> str:
    """Serialize ``value`` as whitespace-free, stable UTF-8 JSON text."""
    import json

    policy = policy or CanonicalPolicy()
    normalized = canonicalize(value, policy)
    return json.dumps(normalized, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_bytes(value: Any, policy: CanonicalPolicy | None = None) -> bytes:
    """Return the canonical UTF-8 representation of ``value``."""
    return canonical_json(value, policy).encode("utf-8")
