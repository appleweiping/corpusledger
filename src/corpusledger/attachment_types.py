"""Immutable attachment metadata and explicit bounded storage policies."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .annotations import _object
from .errors import InputError

MAX_BLOB_BYTES = 4 * 1024 * 1024
MAX_ATTACHMENT_NAMES = 64
MAX_LOGICAL_BYTES = 8 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_MEDIA = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*\Z")


class AttachmentError(InputError):
    """An attachment contract, payload or resource bound is invalid."""


class AttachmentQuotaError(AttachmentError):
    """A configured logical, physical, receipt or encoded-size quota was exceeded."""


def integer(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise AttachmentError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def sha256(value: Any) -> str:
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise AttachmentError("attachment digest must be lowercase SHA-256")
    return value


def logical_name(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or value != value.strip()
        or value in (".", "..")
        or any(char in value for char in ("/", "\\", ":"))
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise AttachmentError("attachment name must be a bounded logical name, not a path")
    try:
        if len(value.encode("utf-8")) > 256:
            raise AttachmentError("attachment name exceeds 256 UTF-8 bytes")
    except UnicodeError as error:
        raise AttachmentError("attachment name contains invalid Unicode") from error
    return value


def command_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 256
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise AttachmentError("attachment command ID must be bounded nonempty text")
    try:
        if len(value.encode("utf-8")) > 256:
            raise AttachmentError("attachment command ID exceeds 256 UTF-8 bytes")
    except UnicodeError as error:
        raise AttachmentError("attachment command ID contains invalid Unicode") from error
    return value


@dataclass(frozen=True, slots=True)
class AttachmentManifest:
    name: str
    sha256: str
    size: int
    media_type: str

    def __post_init__(self) -> None:
        logical_name(self.name)
        sha256(self.sha256)
        integer(self.size, "attachment size", 0, MAX_BLOB_BYTES)
        if not isinstance(self.media_type, str) or len(self.media_type) > 127 or not _MEDIA.fullmatch(self.media_type):
            raise AttachmentError("media_type must be a lowercase type/subtype without parameters")

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "sha256": self.sha256, "size": self.size, "media_type": self.media_type}

    @classmethod
    def from_dict(cls, value: Any) -> AttachmentManifest:
        return cls(**dict(_object(value, {"name", "sha256", "size", "media_type"}, "attachment manifest")))


@dataclass(frozen=True, slots=True)
class AttachmentLimits:
    max_blob_bytes: int = MAX_BLOB_BYTES
    max_names: int = MAX_ATTACHMENT_NAMES
    max_event_bytes: int = MAX_LOGICAL_BYTES
    max_store_bytes: int = 64 * 1024 * 1024
    max_store_blobs: int = 1024
    max_receipts: int = 100_000

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("max_blob_bytes", 0, MAX_BLOB_BYTES),
            ("max_names", 0, MAX_ATTACHMENT_NAMES),
            ("max_event_bytes", 0, MAX_LOGICAL_BYTES),
            ("max_store_bytes", 0, 1024 * 1024 * 1024),
            ("max_store_blobs", 0, 100_000),
            ("max_receipts", 0, 1_000_000),
        ):
            integer(getattr(self, name), name, minimum, maximum)

    def to_dict(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


def manifests(value: Any, limits: AttachmentLimits | None = None) -> tuple[AttachmentManifest, ...]:
    policy = limits if limits is not None else AttachmentLimits()
    if not isinstance(policy, AttachmentLimits):
        raise AttachmentError("limits must be AttachmentLimits")
    if not isinstance(value, (list, tuple)):
        raise AttachmentError("attachment manifest must be an array")
    if len(value) > policy.max_names:
        raise AttachmentQuotaError("attachment manifest exceeds its name limit")
    result = tuple(value)
    if any(not isinstance(item, AttachmentManifest) for item in result):
        raise AttachmentError("attachment entries must be AttachmentManifest values")
    if len({item.name for item in result}) != len(result):
        raise AttachmentError("attachment names must be unique")
    if (
        any(item.size > policy.max_blob_bytes for item in result)
        or sum(item.size for item in result) > policy.max_event_bytes
    ):
        raise AttachmentQuotaError("attachment manifest exceeds its logical byte limit")
    sizes: dict[str, int] = {}
    for item in result:
        if item.sha256 in sizes and sizes[item.sha256] != item.size:
            raise AttachmentError("same attachment digest has conflicting sizes")
        sizes[item.sha256] = item.size
    return tuple(sorted(result, key=lambda item: item.name))


def bounded_json(value: Any, maximum: int, *, adapt: Callable[[Any], Any] | None = None) -> bytes:
    """Canonical JSON with bounded incremental traversal, including native adapters.

    Adapters may expose tuples of native document/annotation values: these are
    converted one item at a time, not expanded into an unbounded complete tree.
    """
    result = bytearray()
    nodes = 0

    def emit(fragment: str) -> None:
        if len(fragment) > maximum - len(result):
            raise AttachmentQuotaError("attachment JSON exceeds its complete byte limit")
        encoded = fragment.encode("utf-8")
        if len(encoded) > maximum - len(result):
            raise AttachmentQuotaError("attachment JSON exceeds its complete byte limit")
        result.extend(encoded)

    def visit(item: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if depth > 40 or nodes > maximum:
            raise AttachmentError("attachment JSON exceeds its structural limit")
        if adapt is not None:
            item = adapt(item)
        if isinstance(item, Mapping):
            if len(item) > (maximum - len(result)) // 4 or any(not isinstance(key, str) for key in item):
                raise AttachmentError("attachment JSON has invalid or excessive object fields")
            emit("{")
            for index, key in enumerate(sorted(item)):
                if index:
                    emit(",")
                visit(key, depth + 1)
                emit(":")
                visit(item[key], depth + 1)
            emit("}")
        elif isinstance(item, (list, tuple)):
            if len(item) > maximum - len(result):
                raise AttachmentError("attachment JSON has excessive array items")
            emit("[")
            for index, child in enumerate(item):
                if index:
                    emit(",")
                visit(child, depth + 1)
            emit("]")
        else:
            if isinstance(item, str):
                if len(item) > maximum - len(result):
                    raise AttachmentQuotaError("attachment JSON exceeds its complete byte limit")
                # JSON escaping and UTF-8 encoding remain chunked, even for a
                # single huge string in a caller-supplied metadata object.
                emit('"')
                try:
                    for offset in range(0, len(item), 4096):
                        emit(json.dumps(item[offset : offset + 4096], ensure_ascii=False)[1:-1])
                except (ValueError, UnicodeError) as error:
                    raise AttachmentError("attachment JSON contains invalid text") from error
                emit('"')
                return
            elif item is not None and type(item) not in (int, float, bool):
                raise AttachmentError("attachment JSON contains unsupported values")
            elif type(item) is int and abs(item) >= 10**1000:
                raise AttachmentError("attachment JSON integer is too large")
            elif type(item) is float and not math.isfinite(item):
                raise AttachmentError("attachment JSON contains nonfinite values")
            try:
                emit(json.dumps(item, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
            except (ValueError, UnicodeError) as error:
                raise AttachmentError("attachment JSON contains invalid text or numbers") from error

    visit(value, 0)
    return bytes(result)


def json_digest(value: Any) -> str:
    return hashlib.sha256(bounded_json(value, MAX_SNAPSHOT_BYTES)).hexdigest()
