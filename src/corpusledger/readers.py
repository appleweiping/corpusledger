"""Streaming readers for JSON, JSON Lines, and extensible file sources."""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable

from .canonical import CanonicalPolicy, canonical_json, canonicalize
from .errors import CanonicalizationError, DuplicateIdError, InputError
from .strictjson import (
    StrictJsonError,
    bounded_int,
    finite_float,
    object_without_duplicates,
    reject_constant,
)

READER_ENTRY_POINT_GROUP = "corpusledger.readers"
_BUILTIN_EXTENSIONS = frozenset({".json", ".jsonl"})
_READER_EXTENSION = re.compile(r"\.[a-z0-9][a-z0-9_+-]*\Z")


@dataclass(frozen=True, slots=True)
class Record:
    """A record plus its stable identity and source location."""

    record_id: str
    data: dict[str, Any]
    source: str
    position: int


@runtime_checkable
class ReaderAdapter(Protocol):
    """Contract implemented by an explicitly selected file reader plugin.

    Reader entry points use the ``corpusledger.readers`` group. The entry point
    must resolve to an instance (or a zero-argument class) implementing this
    protocol. Third-party entry points are never imported automatically.
    """

    name: str
    version: str
    extensions: frozenset[str]

    def iter_records(
        self,
        path: Path,
        *,
        id_field: str,
        policy: CanonicalPolicy,
    ) -> Iterator[Record]: ...


def _id_text(raw: Any, *, source: str, position: int, policy: CanonicalPolicy) -> str:
    if raw is None or isinstance(raw, (dict, list)):
        raise InputError(f"{source} record {position}: ID must be a scalar, not {type(raw).__name__}")
    try:
        normalized = canonicalize(str(raw), policy)
    except CanonicalizationError as exc:
        raise InputError(f"{source} record {position}: invalid ID: {exc}") from exc
    if not isinstance(normalized, str):
        raise InputError(f"{source} record {position}: normalized ID is not text")
    if not normalized:
        raise InputError(f"{source} record {position}: ID must not be empty")
    return normalized


def _records_from_values(
    values: Iterable[Any], source: str, id_field: str, policy: CanonicalPolicy
) -> Iterator[Record]:
    for position, value in enumerate(values, start=1):
        if not isinstance(value, dict):
            raise InputError(f"{source} record {position}: expected an object, got {type(value).__name__}")
        if id_field not in value:
            raise InputError(f"{source} record {position}: missing ID field {id_field!r}")
        yield Record(
            _id_text(value[id_field], source=source, position=position, policy=policy),
            value,
            source,
            position,
        )


def iter_json(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> Iterator[Record]:
    """Iterate a JSON array or object.

    The standard-library JSON decoder materializes the document. Use JSONL for
    constant-record working memory.
    """

    try:
        with path.open("r", encoding="utf-8") as stream:
            value = json.load(
                stream,
                object_pairs_hook=object_without_duplicates,
                parse_constant=reject_constant,
                parse_float=finite_float,
                parse_int=bounded_int,
            )
    except StrictJsonError as exc:
        raise InputError(f"cannot read JSON {path}: {exc}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InputError(f"cannot read JSON {path}: {exc}") from exc
    values = value if isinstance(value, list) else [value]
    yield from _records_from_values(values, path.as_posix(), id_field, policy or CanonicalPolicy())


def read_json(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> list[Record]:
    """Compatibility wrapper returning all records from one JSON document."""

    return list(iter_json(path, id_field, policy))


def iter_jsonl(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> Iterator[Record]:
    """Yield strict non-empty JSON Lines records while retaining line numbers."""

    active_policy = policy or CanonicalPolicy()
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(
                        line,
                        object_pairs_hook=object_without_duplicates,
                        parse_constant=reject_constant,
                        parse_float=finite_float,
                        parse_int=bounded_int,
                    )
                except json.JSONDecodeError as exc:
                    raise InputError(f"{path.as_posix()} line {line_number}: malformed JSON: {exc.msg}") from exc
                except StrictJsonError as exc:
                    raise InputError(f"{path.as_posix()} line {line_number}: {exc}") from exc
                if not isinstance(value, dict):
                    raise InputError(f"{path.as_posix()} line {line_number}: expected an object")
                if id_field not in value:
                    raise InputError(f"{path.as_posix()} line {line_number}: missing ID field {id_field!r}")
                yield Record(
                    _id_text(
                        value[id_field],
                        source=path.as_posix(),
                        position=line_number,
                        policy=active_policy,
                    ),
                    value,
                    path.as_posix(),
                    line_number,
                )
    except (OSError, UnicodeError) as exc:
        raise InputError(f"cannot read JSONL {path}: {exc}") from exc


def read_jsonl(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> list[Record]:
    """Compatibility wrapper returning all JSON Lines records."""

    return list(iter_jsonl(path, id_field, policy))


def _normalize_extensions(extensions: object, *, reader_name: str) -> frozenset[str]:
    if not isinstance(extensions, frozenset) or not extensions:
        raise InputError(f"reader {reader_name!r} extensions must be a non-empty frozenset")
    normalized: set[str] = set()
    for extension in extensions:
        if not isinstance(extension, str) or _READER_EXTENSION.fullmatch(extension) is None:
            raise InputError(
                f"reader {reader_name!r} extensions must be lowercase ASCII file suffixes such as '.parquet'"
            )
        normalized.add(extension)
    return frozenset(normalized)


def _reader_identity_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise InputError(f"reader adapter {field} must be a non-empty string")
    if value != value.strip() or any(unicodedata.category(character) in {"Cc", "Cf", "Cs"} for character in value):
        raise InputError(f"reader adapter {field} must not contain surrounding whitespace or control characters")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise InputError(f"reader adapter {field} must contain only Unicode scalar values")
    return value


def validate_reader_identity(name: object, version: object) -> tuple[str, str]:
    """Validate stable, serializable reader identity metadata."""

    return (
        _reader_identity_text(name, field="name"),
        _reader_identity_text(version, field="version"),
    )


def validate_reader_adapter(value: object) -> ReaderAdapter:
    """Validate and return a reader object without invoking its parser."""

    name = getattr(value, "name", None)
    version = getattr(value, "version", None)
    method = getattr(value, "iter_records", None)
    validated_name, _ = validate_reader_identity(name, version)
    _normalize_extensions(getattr(value, "extensions", None), reader_name=validated_name)
    if not callable(method):
        raise InputError(f"reader {name!r} must provide iter_records()")
    return cast(ReaderAdapter, value)


def load_reader_adapter(name: str) -> ReaderAdapter:
    """Load one explicitly named ``corpusledger.readers`` entry point."""

    if not isinstance(name, str) or not name:
        raise InputError("reader entry point name must not be empty")
    _reader_identity_text(name, field="entry point name")
    try:
        matches = tuple(entry_points().select(group=READER_ENTRY_POINT_GROUP, name=name))
    except Exception as exc:
        raise InputError(f"cannot discover reader entry point {name!r}: {type(exc).__name__}") from exc
    if not matches:
        raise InputError(f"reader entry point {name!r} is not installed")
    if len(matches) != 1:
        raise InputError(f"reader entry point {name!r} is ambiguous")
    try:
        loaded = matches[0].load()
        candidate = loaded() if isinstance(loaded, type) else loaded
    except Exception as exc:
        raise InputError(f"cannot load reader entry point {name!r}: {type(exc).__name__}") from exc
    adapter = validate_reader_adapter(candidate)
    if adapter.name != name:
        raise InputError(f"reader entry point {name!r} returned adapter named {adapter.name!r}")
    return adapter


def discover_inputs(
    path: Path,
    exclude_paths: Iterable[str | Path] = (),
    *,
    reader: ReaderAdapter | None = None,
) -> list[Path]:
    """Return deterministic supported input paths beneath ``path``."""

    excluded = {Path(item).resolve() for item in exclude_paths}
    extensions = set(_BUILTIN_EXTENSIONS)
    if reader is not None:
        extensions.update(_normalize_extensions(reader.extensions, reader_name=reader.name))
    if path.is_file():
        if path.suffix.lower() not in extensions:
            raise InputError(f"unsupported input extension: {path.suffix or '<none>'}")
        return [] if path.resolve() in excluded else [path]
    if not path.is_dir():
        raise InputError(f"input does not exist: {path}")
    paths = sorted(
        (
            item
            for item in path.rglob("*")
            if item.is_file() and item.resolve() not in excluded and item.suffix.lower() in extensions
        ),
        key=lambda item: item.relative_to(path).as_posix(),
    )
    if not paths:
        rendered = ", ".join(sorted(extensions))
        raise InputError(f"directory contains no supported files ({rendered}): {path}")
    return paths


def _iter_path(
    path: Path,
    id_field: str,
    policy: CanonicalPolicy,
    reader: ReaderAdapter | None,
) -> Iterator[Record]:
    if reader is not None and path.suffix.lower() in reader.extensions:
        try:
            yield from reader.iter_records(path, id_field=id_field, policy=policy)
        except InputError:
            raise
        except Exception as exc:
            raise InputError(f"reader {reader.name!r} failed for {path}: {type(exc).__name__}") from exc
        return
    if path.suffix.lower() == ".jsonl":
        yield from iter_jsonl(path, id_field, policy)
    else:
        yield from iter_json(path, id_field, policy)


def _validated_adapter_record(
    record: object,
    *,
    path: Path,
    id_field: str,
    policy: CanonicalPolicy,
) -> Record:
    if not isinstance(record, Record):
        raise InputError(f"reader for {path} yielded {type(record).__name__}, expected Record")
    if not isinstance(record.data, dict) or id_field not in record.data:
        raise InputError(f"reader for {path} yielded a record without ID field {id_field!r}")
    if isinstance(record.position, bool) or not isinstance(record.position, int) or record.position < 1:
        raise InputError(f"reader for {path} yielded an invalid record position")
    expected = _id_text(record.data[id_field], source=path.as_posix(), position=record.position, policy=policy)
    if record.record_id != expected:
        raise InputError(f"reader for {path} yielded record ID inconsistent with {id_field!r}")
    if record.source != path.as_posix():
        raise InputError(f"reader for {path} yielded an inconsistent source path")
    return record


def iter_corpus(
    path: str | Path,
    id_field: str = "id",
    *,
    policy: CanonicalPolicy | None = None,
    exclude_paths: Iterable[str | Path] = (),
    reader: ReaderAdapter | None = None,
) -> Iterator[Record]:
    """Yield a corpus while rejecting duplicate normalized IDs globally.

    JSONL input is parsed one record at a time. The duplicate-ID index is the
    unavoidable in-memory lower bound for global uniqueness checking.
    """

    root = Path(path).resolve()
    active_policy = policy or CanonicalPolicy()
    active_reader = validate_reader_adapter(reader) if reader is not None else None
    seen: dict[str, tuple[str, int]] = {}
    for item in discover_inputs(root, exclude_paths, reader=active_reader):
        for candidate in _iter_path(item, id_field, active_policy, active_reader):
            record = _validated_adapter_record(
                candidate,
                path=item,
                id_field=id_field,
                policy=active_policy,
            )
            previous = seen.get(record.record_id)
            if previous:
                raise DuplicateIdError(
                    f"duplicate ID {record.record_id!r}: "
                    f"{previous[0]}:{previous[1]} and {record.source}:{record.position}"
                )
            seen[record.record_id] = (record.source, record.position)
            yield record


def read_corpus(
    path: str | Path,
    id_field: str = "id",
    *,
    policy: CanonicalPolicy | None = None,
    exclude_paths: Iterable[str | Path] = (),
    reader: ReaderAdapter | None = None,
) -> list[Record]:
    """Compatibility wrapper materializing :func:`iter_corpus`."""

    return list(
        iter_corpus(
            path,
            id_field,
            policy=policy,
            exclude_paths=exclude_paths,
            reader=reader,
        )
    )


def logical_file_hash_payload(records: Iterable[Record], policy: CanonicalPolicy) -> list[dict[str, str]]:
    """Build a format-independent payload for one logical source file."""

    return [{"id": record.record_id, "record": canonical_json(record.data, policy)} for record in records]
