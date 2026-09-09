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
from pathlib import Path
from typing import Any

from .paths import join_pointer
from .readers import iter_corpus

DEFAULT_SENSITIVE_NAMES = frozenset({"password", "passwd", "secret", "api_key", "token", "access_token", "ssn"})
PRIVACY_PACKS = {
    "default": DEFAULT_SENSITIVE_NAMES,
    "credentials": DEFAULT_SENSITIVE_NAMES
    | frozenset({"authorization", "credential", "client_secret", "private_key", "refresh_token", "jwt"}),
    "pii": DEFAULT_SENSITIVE_NAMES
    | frozenset(
        {
            "address",
            "date_of_birth",
            "dob",
            "email",
            "first_name",
            "full_name",
            "last_name",
            "phone",
            "telephone",
            "zip",
            "zip_code",
        }
    ),
}
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
    def from_pack(
        cls,
        pack: str = "default",
        *,
        min_token_length: int = 24,
        entropy_threshold: float = 3.7,
    ) -> PrivacyConfig:
        """Build a named, deterministic privacy rule pack."""

        if not isinstance(pack, str) or pack not in PRIVACY_PACKS:
            choices = ", ".join(sorted(PRIVACY_PACKS))
            raise ValueError(f"unknown privacy pack {pack!r}; choose one of: {choices}")
        return cls(PRIVACY_PACKS[pack], min_token_length, entropy_threshold)

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


def privacy_packs() -> tuple[str, ...]:
    """Return stable names of built-in privacy rule packs."""

    return tuple(sorted(PRIVACY_PACKS))


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


def scan_record(
    record_id: str,
    data: dict[str, Any],
    config: PrivacyConfig | None = None,
) -> tuple[dict[str, Any], ...]:
    """Scan one record, allowing callers to discard its content immediately."""

    return tuple(scan_records(((record_id, data),), config))


@dataclass(frozen=True)
class PrivacyReport:
    """Scanner configuration, complete record count, and redacted findings."""

    input_path: str
    id_field: str
    config: PrivacyConfig
    records_checked: int
    findings: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the shared CLI and service report contract."""
        return {
            "schema_version": 1,
            "privacy_version": PRIVACY_VERSION,
            "operation": "privacy",
            "input": self.input_path,
            "id_field": self.id_field,
            "config": self.config.to_dict(),
            "records_checked": self.records_checked,
            "finding_count": len(self.findings),
            "findings": [dict(finding) for finding in self.findings],
        }


def scan_corpus(path: str | Path, *, id_field: str = "id", config: PrivacyConfig | None = None) -> PrivacyReport:
    """Scan a selected corpus using strict readers and stable report ordering.

    JSONL bodies are read one record at a time. Duplicate-ID bookkeeping and all
    findings remain in memory; JSON arrays are materialized by the JSON reader.
    IDs and field paths are retained as locations, so callers must themselves
    use non-sensitive identifiers and keys when sharing reports.
    """
    if not isinstance(id_field, str) or not id_field.strip():
        raise ValueError("id_field must be a non-empty string")
    if config is not None and not isinstance(config, PrivacyConfig):
        raise TypeError("config must be a PrivacyConfig")
    active = config if config is not None else PrivacyConfig()
    source = Path(path).resolve()
    count = 0

    def records() -> Iterable[tuple[str, dict[str, Any]]]:
        nonlocal count
        for record in iter_corpus(source, id_field=id_field):
            count += 1
            yield record.record_id, record.data

    findings = scan_records(records(), active)
    return PrivacyReport(source.as_posix(), id_field, active, count, tuple(findings))
