"""Record, file, and corpus hashing primitives."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal, Protocol

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes
from .readers import Record, logical_file_hash_payload

HashAlgorithm = Literal["sha256", "blake2b"]


class _Hash(Protocol):
    def update(self, value: bytes) -> None: ...

    def hexdigest(self) -> str: ...


def _new_hash(algorithm: HashAlgorithm) -> _Hash:
    if algorithm == "sha256":
        return hashlib.sha256()
    if algorithm == "blake2b":
        return hashlib.blake2b(digest_size=32)
    raise ValueError(f"unsupported hash algorithm: {algorithm}")


class CanonicalSequenceHasher:
    """Incrementally hash a canonical JSON array without retaining its values.

    Under ``list_strategy='sort'``, compatibility with format version 1 requires
    sorting the canonical sequence members. That opt-in policy therefore retains
    encoded members; the default ``preserve`` policy has constant working memory.
    """

    def __init__(self, policy: CanonicalPolicy, algorithm: HashAlgorithm = "sha256") -> None:
        self._policy = policy
        self._hasher = _new_hash(algorithm)
        self._first = True
        self._finished = False
        self._sorted_items: list[bytes] | None = [] if policy.list_strategy == "sort" else None
        if self._sorted_items is None:
            self._hasher.update(b"[")

    @property
    def streaming(self) -> bool:
        """Whether members are discarded immediately after hashing."""

        return self._sorted_items is None

    def add(self, value: object) -> None:
        """Append one array member."""

        if self._finished:
            raise RuntimeError("cannot add to a finished sequence fingerprint")
        encoded = canonical_bytes(value, self._policy)
        if self._sorted_items is not None:
            self._sorted_items.append(encoded)
            return
        if not self._first:
            self._hasher.update(b",")
        self._hasher.update(encoded)
        self._first = False

    def finish(self) -> str:
        """Finalize the array and return its hexadecimal digest once."""

        if self._finished:
            raise RuntimeError("sequence fingerprint is already finished")
        if self._sorted_items is not None:
            self._hasher.update(b"[")
            for index, encoded in enumerate(sorted(self._sorted_items)):
                if index:
                    self._hasher.update(b",")
                self._hasher.update(encoded)
            self._sorted_items.clear()
        self._hasher.update(b"]")
        self._finished = True
        return self._hasher.hexdigest()


@dataclass(frozen=True)
class HashMetadata:
    """Metadata required to reproduce a fingerprint."""

    algorithm: HashAlgorithm
    canonical_version: str
    policy: dict[str, object]


def digest(data: bytes, algorithm: HashAlgorithm = "sha256") -> str:
    """Hash bytes with a supported, explicitly named algorithm."""
    hasher = _new_hash(algorithm)
    hasher.update(data)
    return hasher.hexdigest()


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
