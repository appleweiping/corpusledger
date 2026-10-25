"""Turn a dataset into a list of record IDs and content hashes (a snapshot).

A snapshot keeps, for every record, an ID, a short label for humans, a hash of
the whole record and a hash of every field. Two snapshots (or a snapshot and
the current data) are enough to say which records were added, removed or
edited and in which fields, without keeping the old data around.

How records are matched between versions:

- ``Match("id", ("id",))``: by the value of an ID field;
- ``Match("key", ("instruction", "input"))``: by a hash of the listed fields,
  for datasets that have no ID column;
- ``Match("content")``: by a hash of the whole record. Edits then show up as
  one removal plus one addition.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from .canonical import CanonicalPolicy, canonicalize
from .errors import CanonicalizationError, InputError, SnapshotError
from .paths import join_pointer
from .readers import Row, iter_rows
from .schema import SchemaAccumulator

FORMAT = "corpusledger/2"
_POLICY = CanonicalPolicy()
_LABEL_WIDTH = 72
_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def short_hash(value: Any) -> str:
    """First 16 hex digits (64 bits) of the SHA-256 of an already canonical JSON value."""
    return hashlib.sha256(_ENCODER.encode(value).encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Match:
    """How records are paired between two versions of a dataset."""

    by: Literal["id", "key", "content"]
    fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.by not in ("id", "key", "content"):
            raise ValueError(f"unknown match mode {self.by!r}")
        if self.by == "id" and len(self.fields) != 1:
            raise ValueError("id matching needs exactly one field")
        if self.by == "key" and not self.fields:
            raise ValueError("key matching needs at least one field")
        if self.by == "content" and self.fields:
            raise ValueError("content matching takes no fields")

    def describe(self) -> str:
        if self.by == "id":
            return f"id field {self.fields[0]!r}"
        if self.by == "key":
            return "key " + " + ".join(self.fields)
        return "whole-record content"

    def to_dict(self) -> dict[str, Any]:
        return {"by": self.by, "fields": list(self.fields)}


@dataclass(frozen=True)
class Entry:
    """What a snapshot remembers about one record."""

    id: str
    label: str
    hash: str
    fields: dict[str, str]
    rest: str | None = None  # key matching: hash of the record without its key fields

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"id": self.id, "label": self.label, "hash": self.hash}
        if self.rest is not None:
            result["rest"] = self.rest
        result["fields"] = self.fields
        return result


@dataclass
class Snapshot:
    """IDs and hashes of every record in one version of a dataset."""

    source: str
    match: Match
    ignore: tuple[str, ...]
    entries: list[Entry]
    schema: dict[str, Any]
    duplicates: int = 0
    rows: dict[str, dict[str, Any]] | None = field(default=None, repr=False, compare=False)

    @property
    def dataset_hash(self) -> str:
        """SHA-256 over sorted (id, hash) pairs: same content gives the same value in any order."""
        pairs = sorted((entry.id, entry.hash) for entry in self.entries)
        return hashlib.sha256(json.dumps(pairs, separators=(",", ":")).encode()).hexdigest()

    @property
    def order_hash(self) -> str:
        ids = [entry.id for entry in self.entries]
        return hashlib.sha256(json.dumps(ids, ensure_ascii=False).encode("utf-8")).hexdigest()

    def save(self, path: str | Path) -> None:
        """Write the snapshot as JSON with one record per line, so it diffs well in git."""
        header = {
            "format": FORMAT,
            "source": self.source,
            "match": self.match.to_dict(),
            "ignore": list(self.ignore),
            "records": len(self.entries),
            "duplicates": self.duplicates,
            "dataset_hash": self.dataset_hash,
            "schema": self.schema,
        }
        head = json.dumps(header, ensure_ascii=False, separators=(",", ":"))[:-1]
        lines = (
            json.dumps(entry.to_dict(), ensure_ascii=False, separators=(",", ":"))
            for entry in self.entries
        )
        body = ",\n".join(lines)
        try:
            Path(path).write_text(f'{head},"entries":[\n{body}\n]}}\n', encoding="utf-8")
        except OSError as exc:
            raise SnapshotError(f"cannot write {path}: {exc}") from exc

    @classmethod
    def load(cls, path: str | Path) -> Snapshot:
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"cannot read snapshot {path}: {exc}") from exc
        if not isinstance(raw, dict) or raw.get("format") != FORMAT:
            raise SnapshotError(f"{path} is not a {FORMAT} snapshot")
        try:
            match = Match(raw["match"]["by"], tuple(raw["match"]["fields"]))
            entries = [
                Entry(
                    str(item["id"]),
                    str(item["label"]),
                    str(item["hash"]),
                    {str(key): str(value) for key, value in item["fields"].items()},
                    item.get("rest"),
                )
                for item in raw["entries"]
            ]
            snapshot = cls(
                source=str(raw["source"]),
                match=match,
                ignore=tuple(raw["ignore"]),
                entries=entries,
                schema=raw["schema"],
                duplicates=int(raw.get("duplicates", 0)),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise SnapshotError(f"malformed snapshot {path}: {exc}") from exc
        if raw.get("dataset_hash") != snapshot.dataset_hash:
            raise SnapshotError(
                f"{path}: dataset_hash does not match its entries (edited by hand?)"
            )
        return snapshot


def is_snapshot(path: str | Path) -> bool:
    """True if ``path`` is a file written by :meth:`Snapshot.save`."""
    target = Path(path)
    if not target.is_file() or target.suffix.lower() != ".json":
        return False
    with target.open("rb") as stream:
        start = stream.read(64)
    return start.startswith(b'{"format":"corpusledger/')


def flatten(value: Any, path: str = "") -> dict[str, Any]:
    """Map JSON Pointer paths to leaf values. Lists are leaves; empty objects too."""
    output: dict[str, Any] = {}
    if isinstance(value, dict) and (value or not path):
        for key in sorted(value):
            output.update(flatten(value[key], join_pointer(path, str(key))))
    else:
        output[path] = value
    return output


def _drop(record: dict[str, Any], name: str) -> None:
    if not name.startswith("/"):
        record.pop(name, None)
        return
    parts = [part.replace("~1", "/").replace("~0", "~") for part in name[1:].split("/")]
    target: Any = record
    for part in parts[:-1]:
        target = target.get(part) if isinstance(target, dict) else None
        if target is None:
            return
    if isinstance(target, dict):
        target.pop(parts[-1], None)


def _label(text: str) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= _LABEL_WIDTH else flat[: _LABEL_WIDTH - 3] + "..."


def _text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def detect_match(rows: Iterable[Row]) -> Match:
    """``id`` matching if the first record has an ``id`` field, else whole-record content."""
    for row in rows:
        return Match("id", ("id",)) if "id" in row.data else Match("content")
    return Match("content")


def _entries(
    rows: Iterable[Row], match: Match, ignore: Sequence[str]
) -> Iterator[tuple[Entry, dict[str, Any]]]:
    for row in rows:
        try:
            record = canonicalize(row.data, _POLICY)
        except CanonicalizationError as exc:
            raise InputError(f"{row.source} record {row.position}: {exc}") from exc
        for name in ignore:
            _drop(record, name)
        rest = None
        if match.by == "id":
            name = match.fields[0]
            if name not in record or record[name] is None or record[name] == "":
                raise InputError(
                    f"{row.source} record {row.position} has no {name!r} field; "
                    "use --key FIELD,... to match records by other fields"
                )
            value = record[name]
            if isinstance(value, (dict, list)):
                raise InputError(f"{row.source} record {row.position}: ID must be a scalar")
            record_id = label = str(value)
        elif match.by == "key":
            missing = [name for name in match.fields if name not in record]
            if missing:
                names = ", ".join(map(repr, missing))
                raise InputError(f"{row.source} record {row.position} has no {names} field")
            values = [record[name] for name in match.fields]
            record_id = "k:" + short_hash(values)
            label = _label(" | ".join(_text(value) for value in values if value not in ("", None)))
            rest = short_hash({k: v for k, v in record.items() if k not in match.fields})
        else:
            record_id = "c:" + short_hash(record)
            first_text = next((v for v in record.values() if isinstance(v, str) and v), "")
            label = _label(first_text or short_hash(record))
        fields = {path: short_hash(value) for path, value in flatten(record).items()}
        entry = Entry(record_id, label, short_hash(record), fields, rest)
        yield entry, record


def build_snapshot(
    path: str | Path,
    match: Match | None = None,
    *,
    ignore: Sequence[str] = (),
    keep_rows: bool = False,
) -> Snapshot:
    """Read a dataset and hash every record.

    Records whose ID repeats get ``#2``, ``#3`` ... appended in file order, and
    are counted in :attr:`Snapshot.duplicates`. With ``keep_rows`` the
    normalized records are kept in memory so a diff can show example values.
    """
    rows_iter = iter_rows(path)
    first = next(rows_iter, None)
    if match is None:
        match = detect_match([first] if first is not None else [])
    all_rows = itertools.chain([first] if first is not None else [], rows_iter)
    schema = SchemaAccumulator()
    entries: list[Entry] = []
    rows: dict[str, dict[str, Any]] | None = {} if keep_rows else None
    seen: dict[str, int] = {}
    duplicates = 0
    for entry, record in _entries(all_rows, match, ignore):
        count = seen.get(entry.id, 0) + 1
        seen[entry.id] = count
        if count > 1:
            duplicates += 1
            entry = Entry(f"{entry.id}#{count}", entry.label, entry.hash, entry.fields, entry.rest)
        entries.append(entry)
        schema.observe(record)
        if rows is not None:
            rows[entry.id] = record
    return Snapshot(
        source=Path(path).name,
        match=match,
        ignore=tuple(ignore),
        entries=entries,
        schema=schema.to_dict(),
        duplicates=duplicates,
        rows=rows,
    )
