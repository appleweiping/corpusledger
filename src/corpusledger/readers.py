"""Readers for JSON, JSON Lines, and corpus directories."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import CanonicalPolicy, canonical_json, canonicalize
from .errors import DuplicateIdError, InputError
from .strictjson import StrictJsonError, object_without_duplicates, reject_constant


@dataclass(frozen=True)
class Record:
    """A record plus its stable identity and source location."""

    record_id: str
    data: dict[str, Any]
    source: str
    position: int


def _id_text(raw: Any, *, source: str, position: int, policy: CanonicalPolicy) -> str:
    if raw is None or isinstance(raw, (dict, list)):
        raise InputError(f"{source} record {position}: ID must be a scalar, not {type(raw).__name__}")
    normalized = canonicalize(str(raw), policy)
    assert isinstance(normalized, str)
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


def read_json(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> list[Record]:
    """Read a JSON array, or one JSON object, as records."""
    try:
        with path.open("r", encoding="utf-8") as stream:
            value = json.load(
                stream,
                object_pairs_hook=object_without_duplicates,
                parse_constant=reject_constant,
            )
    except StrictJsonError as exc:
        raise InputError(f"cannot read JSON {path}: {exc}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InputError(f"cannot read JSON {path}: {exc}") from exc
    values = value if isinstance(value, list) else [value]
    return list(_records_from_values(values, path.as_posix(), id_field, policy or CanonicalPolicy()))


def read_jsonl(path: Path, id_field: str = "id", policy: CanonicalPolicy | None = None) -> list[Record]:
    """Read non-empty JSON Lines; line numbers are retained as positions."""
    records: list[Record] = []
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
                    )
                except json.JSONDecodeError as exc:
                    raise InputError(f"{path.as_posix()} line {line_number}: malformed JSON: {exc.msg}") from exc
                except StrictJsonError as exc:
                    raise InputError(f"{path.as_posix()} line {line_number}: {exc}") from exc
                if not isinstance(value, dict):
                    raise InputError(f"{path.as_posix()} line {line_number}: expected an object")
                if id_field not in value:
                    raise InputError(f"{path.as_posix()} line {line_number}: missing ID field {id_field!r}")
                records.append(
                    Record(
                        _id_text(
                            value[id_field],
                            source=path.as_posix(),
                            position=line_number,
                            policy=policy or CanonicalPolicy(),
                        ),
                        value,
                        path.as_posix(),
                        line_number,
                    )
                )
    except (OSError, UnicodeError) as exc:
        raise InputError(f"cannot read JSONL {path}: {exc}") from exc
    return records


def discover_inputs(path: Path, exclude_paths: Iterable[str | Path] = ()) -> list[Path]:
    """Return deterministic JSON/JSONL paths beneath ``path``."""
    excluded = {Path(item).resolve() for item in exclude_paths}
    if path.is_file():
        if path.suffix.lower() not in {".json", ".jsonl"}:
            raise InputError(f"unsupported input extension: {path.suffix or '<none>'}")
        return [] if path.resolve() in excluded else [path]
    if not path.is_dir():
        raise InputError(f"input does not exist: {path}")
    paths = sorted(
        (
            item
            for item in path.rglob("*")
            if item.is_file() and item.resolve() not in excluded and item.suffix.lower() in {".json", ".jsonl"}
        ),
        key=lambda item: item.relative_to(path).as_posix(),
    )
    if not paths:
        raise InputError(f"directory contains no .json or .jsonl files: {path}")
    return paths


def read_corpus(
    path: str | Path,
    id_field: str = "id",
    *,
    policy: CanonicalPolicy | None = None,
    exclude_paths: Iterable[str | Path] = (),
) -> list[Record]:
    """Read a file or directory and reject duplicate IDs globally."""
    root = Path(path).resolve()
    records: list[Record] = []
    seen: dict[str, Record] = {}
    policy = policy or CanonicalPolicy()
    for item in discover_inputs(root, exclude_paths):
        current = (
            read_jsonl(item, id_field, policy) if item.suffix.lower() == ".jsonl" else read_json(item, id_field, policy)
        )
        for record in current:
            previous = seen.get(record.record_id)
            if previous:
                raise DuplicateIdError(
                    f"duplicate ID {record.record_id!r}: "
                    f"{previous.source}:{previous.position} and {record.source}:{record.position}"
                )
            seen[record.record_id] = record
            records.append(record)
    return records


def logical_file_hash_payload(records: Iterable[Record], policy: CanonicalPolicy) -> list[dict[str, str]]:
    """Build a format-independent payload for one logical source file."""
    return [{"id": record.record_id, "record": canonical_json(record.data, policy)} for record in records]
