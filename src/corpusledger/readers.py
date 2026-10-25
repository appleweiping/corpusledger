"""Read records from JSON Lines, JSON, CSV and Parquet files (or a directory of them)."""

from __future__ import annotations

import base64
import csv
import datetime as dt
import decimal
import json
import math
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import InputError
from .strictjson import (
    StrictJsonError,
    bounded_int,
    finite_float,
    object_without_duplicates,
    reject_constant,
)

SUFFIXES = (".jsonl", ".ndjson", ".json", ".csv", ".parquet")


@dataclass(frozen=True, slots=True)
class Row:
    """One record and where it came from (1-based line or row number)."""

    data: dict[str, Any]
    source: str
    position: int


def _strict_loads(text: str) -> Any:
    return json.loads(
        text,
        object_pairs_hook=object_without_duplicates,
        parse_constant=reject_constant,
        parse_float=finite_float,
        parse_int=bounded_int,
    )


def _jsonl(path: Path, label: str) -> Iterator[Row]:
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = _strict_loads(line)
            except (json.JSONDecodeError, StrictJsonError) as exc:
                raise InputError(f"{label} line {number}: invalid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise InputError(f"{label} line {number}: expected a JSON object")
            yield Row(value, label, number)


def _json(path: Path, label: str) -> Iterator[Row]:
    try:
        value = _strict_loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, StrictJsonError) as exc:
        raise InputError(f"{label}: invalid JSON: {exc}") from exc
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list):
        raise InputError(f"{label}: expected a JSON array of objects")
    for number, item in enumerate(value, 1):
        if not isinstance(item, dict):
            raise InputError(f"{label} item {number}: expected an object")
        yield Row(item, label, number)


def _csv(path: Path, label: str) -> Iterator[Row]:
    with path.open(encoding="utf-8", newline="") as stream:
        for number, row in enumerate(csv.DictReader(stream), 1):
            yield Row(dict(row), label, number)


def to_json_value(value: Any) -> Any:
    """Convert values that Parquet can hold but JSON cannot (dates, bytes, decimals)."""
    if isinstance(value, dict):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def _parquet(path: Path, label: str) -> Iterator[Row]:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise InputError(
            "reading Parquet needs pyarrow: pip install 'corpusledger[parquet]'"
        ) from None
    number = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
        for item in batch.to_pylist():
            number += 1
            yield Row(to_json_value(item), label, number)


_READERS = {
    ".jsonl": _jsonl,
    ".ndjson": _jsonl,
    ".json": _json,
    ".csv": _csv,
    ".parquet": _parquet,
}


def input_files(path: Path) -> list[Path]:
    """The file itself, or every supported file under a directory in sorted order."""
    if path.is_file():
        if path.suffix.lower() not in _READERS:
            raise InputError(f"{path}: unsupported file type (use {', '.join(SUFFIXES)})")
        return [path]
    if path.is_dir():
        files = sorted(
            (
                item
                for item in path.rglob("*")
                if item.is_file() and item.suffix.lower() in _READERS
            ),
            key=lambda item: item.relative_to(path).as_posix(),
        )
        if not files:
            raise InputError(f"{path}: no {', '.join(SUFFIXES)} files found")
        return files
    raise InputError(f"{path}: no such file or directory")


def iter_rows(path: str | Path) -> Iterator[Row]:
    """Yield every record of a file or directory, one at a time."""
    root = Path(path)
    files = input_files(root)
    for file in files:
        label = file.relative_to(root).as_posix() if root.is_dir() else file.name
        try:
            yield from _READERS[file.suffix.lower()](file, label)
        except UnicodeDecodeError as exc:
            raise InputError(f"{label}: not valid UTF-8 ({exc.reason})") from exc
        except OSError as exc:
            raise InputError(f"{label}: {exc}") from exc
