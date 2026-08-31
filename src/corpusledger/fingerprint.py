"""Record, file, and corpus hashing primitives."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes
from .readers import Record, logical_file_hash_payload

HashAlgorithm = Literal["sha256", "blake2b"]


@dataclass(frozen=True)
class HashMetadata:
    """Metadata required to reproduce a fingerprint."""

    algorithm: HashAlgorithm
    canonical_version: str
    policy: dict[str, object]


def digest(data: bytes, algorithm: HashAlgorithm = "sha256") -> str:
    """Hash bytes with a supported, explicitly named algorithm."""
    if algorithm == "sha256":
        return hashlib.sha256(data).hexdigest()
    if algorithm == "blake2b":
        return hashlib.blake2b(data, digest_size=32).hexdigest()
    raise ValueError(f"unsupported hash algorithm: {algorithm}")


def hash_value(value: object, policy: CanonicalPolicy, algorithm: HashAlgorithm = "sha256") -> str:
    """Hash an arbitrary canonicalizable value."""
    return digest(canonical_bytes(value, policy), algorithm)


def record_fingerprint(record: Record, policy: CanonicalPolicy, algorithm: HashAlgorithm = "sha256") -> str:
    """Hash record content; source filename and position are excluded."""
    return hash_value(record.data, policy, algorithm)


def file_fingerprints(
    records: Iterable[Record],
    policy: CanonicalPolicy,
    algorithm: HashAlgorithm = "sha256",
) -> dict[str, str]:
    """Hash logical records grouped by source path."""
    grouped: dict[str, list[Record]] = {}
    for record in records:
        grouped.setdefault(record.source, []).append(record)
    return {
        source: hash_value(logical_file_hash_payload(grouped[source], policy), policy, algorithm)
        for source in sorted(grouped)
    }


def corpus_fingerprint(
    record_hashes: dict[str, str],
    policy: CanonicalPolicy,
    algorithm: HashAlgorithm = "sha256",
) -> str:
    """Hash ID-to-record hashes independently of physical ordering."""
    return hash_value([{"id": key, "hash": record_hashes[key]} for key in sorted(record_hashes)], policy, algorithm)


def order_fingerprint(ids: Iterable[str], policy: CanonicalPolicy, algorithm: HashAlgorithm = "sha256") -> str:
    """Hash record order separately from content."""
    return hash_value(list(ids), policy, algorithm)


def metadata(policy: CanonicalPolicy, algorithm: HashAlgorithm) -> HashMetadata:
    """Return self-describing fingerprint metadata."""
    return HashMetadata(algorithm, CANONICAL_VERSION, policy.to_dict())
