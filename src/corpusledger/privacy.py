"""Privacy-risk hints over already-loaded record content.

This module never opens credential stores or scans outside the corpus. Findings
identify locations and hashes, not the full potentially sensitive values.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .paths import join_pointer

DEFAULT_SENSITIVE_NAMES = frozenset({"password", "passwd", "secret", "api_key", "token", "access_token", "ssn"})
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_+./=-]+$")
PRIVACY_VERSION = "1"


@dataclass(frozen=True)
class PrivacyConfig:
    """Configuration for field-name and high-entropy token detection."""

    sensitive_names: frozenset[str] = DEFAULT_SENSITIVE_NAMES
    min_token_length: int = 24
    entropy_threshold: float = 3.7

    def __post_init__(self) -> None:
        """Validate thresholds and normalize callers to an immutable name set."""
        if not isinstance(self.sensitive_names, (set, frozenset, list, tuple)):
            raise ValueError("sensitive_names must be a collection of strings")
        if any(not isinstance(name, str) or not name for name in self.sensitive_names):
            raise ValueError("sensitive_names must contain only non-empty strings")
        object.__setattr__(
            self,
            "sensitive_names",
            frozenset(name.casefold() for name in self.sensitive_names),
        )
        if isinstance(self.min_token_length, bool) or not isinstance(self.min_token_length, int):
            raise ValueError("min_token_length must be an integer")
        if self.min_token_length < 1:
            raise ValueError("min_token_length must be at least 1")
        if isinstance(self.entropy_threshold, bool) or not isinstance(self.entropy_threshold, (int, float)):
            raise ValueError("entropy_threshold must be numeric")
        if not math.isfinite(self.entropy_threshold) or self.entropy_threshold < 0:
            raise ValueError("entropy_threshold must be finite and non-negative")
        object.__setattr__(self, "entropy_threshold", float(self.entropy_threshold))

    def to_dict(self) -> dict[str, Any]:
        """Return the complete, deterministic scanner configuration."""
        return {
            "entropy_threshold": self.entropy_threshold,
            "min_token_length": self.min_token_length,
            "sensitive_names": sorted(self.sensitive_names),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PrivacyConfig:
        """Build a validated configuration from manifest metadata."""
        required = {"entropy_threshold", "min_token_length", "sensitive_names"}
        if set(value) != required:
            raise ValueError("privacy config must contain exactly entropy_threshold, min_token_length, sensitive_names")
        names = value["sensitive_names"]
        if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise ValueError("privacy sensitive_names must be a list of strings")
        length = value["min_token_length"]
        threshold = value["entropy_threshold"]
        if isinstance(length, bool) or not isinstance(length, int):
            raise ValueError("privacy min_token_length must be an integer")
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise ValueError("privacy entropy_threshold must be numeric")
        return cls(frozenset(names), length, float(threshold))


def shannon_entropy(text: str) -> float:
    """Compute Shannon entropy in bits per character."""
    if not text:
        return 0.0
    counts = Counter(text)
    return -sum((count / len(text)) * math.log2(count / len(text)) for count in counts.values())


def _evidence(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _scan(value: Any, path: str, record_id: str, config: PrivacyConfig, findings: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            child = join_pointer(path, str(key))
            if key.casefold() in config.sensitive_names:
                findings.append({"kind": "sensitive_field_name", "path": child, "record_id": record_id})
            _scan(item, child, record_id, config, findings)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _scan(item, join_pointer(path, str(index)), record_id, config, findings)
    elif isinstance(value, str) and len(value) >= config.min_token_length and TOKEN_PATTERN.fullmatch(value):
        entropy = shannon_entropy(value)
        if entropy >= config.entropy_threshold:
            findings.append(
                {
                    "evidence_hash": _evidence(value),
                    "entropy": round(entropy, 2),
                    "kind": "high_entropy_token",
                    "length": len(value),
                    "path": path,
                    "record_id": record_id,
                }
            )


def scan_records(
    records: Iterable[tuple[str, dict[str, Any]]], config: PrivacyConfig | None = None
) -> list[dict[str, Any]]:
    """Return deterministic, redacted risk findings for records."""
    config = config or PrivacyConfig()
    findings: list[dict[str, Any]] = []
    for record_id, data in records:
        _scan(data, "", record_id, config, findings)
    return sorted(findings, key=lambda item: (str(item["record_id"]), str(item["path"]), str(item["kind"])))
