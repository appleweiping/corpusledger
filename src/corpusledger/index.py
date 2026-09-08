"""Queryable, authenticated indexes over manifest record metadata."""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import tempfile
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from .canonical import CanonicalPolicy, canonical_json
from .manifest import Manifest, RecordEntry

INDEX_FORMAT = "corpusledger-index/1"
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _manifest_digest(manifest: Manifest) -> str:
    payload = (canonical_json(manifest.to_dict(), CanonicalPolicy(unicode_form="none")) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_digest(value: str, name: str) -> None:
    if not isinstance(value, str) or _HEX_DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be a 64-character lowercase hexadecimal digest")


@dataclass(frozen=True, slots=True)
class IndexRecord:
    """One searchable manifest record and its indexed field paths."""

    record_id: str
    record_hash: str
    source: str
    position: int
    field_paths: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "record_id": self.record_id,
            "record_hash": self.record_hash,
            "source": self.source,
            "position": self.position,
            "field_paths": list(self.field_paths),
        }


@dataclass(frozen=True)
class DuplicateFieldGroup:
    """Records sharing one authenticated field digest."""

    field_path: str
    field_hash: str
    record_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "field_path": self.field_path,
            "field_hash": self.field_hash,
            "record_ids": list(self.record_ids),
        }


class ManifestIndex:
    """Read-only query interface for a digest-bound manifest index.

    The index stores metadata and field paths, never source text. It can be
    rebuilt from a :class:`Manifest` and independently checked against that
    manifest before it is used in a release or review workflow.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._connection = sqlite3.connect(str(self.path))
        self._closed = False
        try:
            self._validate_schema()
        except sqlite3.DatabaseError as error:
            self.close()
            raise ValueError(f"cannot load manifest index: {error}") from error
        except BaseException:
            self.close()
            raise

    @classmethod
    def build(cls, manifest: Manifest, path: str | Path) -> ManifestIndex:
        """Atomically build an index from one manifest and return it open."""

        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a Manifest")
        return cls.build_stream(
            manifest.records,
            path,
            manifest_digest=_manifest_digest(manifest),
            corpus_hash=manifest.corpus_hash,
            record_count=len(manifest.records),
        )

    @classmethod
    def build_stream(
        cls,
        entries: Iterable[RecordEntry],
        path: str | Path,
        *,
        manifest_digest: str,
        corpus_hash: str,
        record_count: int,
    ) -> ManifestIndex:
        """Build an index from a one-pass record-entry stream.

        This is the bounded-memory counterpart to :meth:`build`: callers that
        already validate a large manifest in a database or JSONL pipeline can
        stream :class:`~corpusledger.manifest.RecordEntry` values without
        materializing a complete :class:`Manifest`. The supplied digest and
        corpus hash must come from that authenticated manifest; ``verify`` can
        still be used later with a fully loaded manifest.
        """

        _validate_digest(manifest_digest, "manifest_digest")
        _validate_digest(corpus_hash, "corpus_hash")
        if isinstance(record_count, bool) or not isinstance(record_count, int) or record_count < 0:
            raise ValueError("record_count must be a non-negative integer")
        destination = Path(path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
        os.close(descriptor)
        try:
            connection = sqlite3.connect(temporary)
            try:
                _create_schema(connection)
                count = 0
                with connection:
                    for entry in entries:
                        if not isinstance(entry, RecordEntry):
                            raise TypeError("entries must contain RecordEntry values")
                        connection.execute(
                            "INSERT INTO records(record_id,record_hash,source,position) VALUES(?,?,?,?)",
                            (entry.record_id, entry.hash, entry.source, entry.position),
                        )
                        connection.executemany(
                            "INSERT INTO fields(record_id,path,field_hash) VALUES(?,?,?)",
                            (
                                (entry.record_id, path_value, field_hash)
                                for path_value, field_hash in sorted(entry.field_hashes.items())
                            ),
                        )
                        count += 1
                    if count != record_count:
                        raise ValueError(f"record stream yielded {count} entries; expected {record_count}")
                    connection.executemany(
                        "INSERT INTO metadata(key,value) VALUES(?,?)",
                        (
                            ("format", INDEX_FORMAT),
                            ("manifest_digest", manifest_digest),
                            ("corpus_hash", corpus_hash),
                            ("record_count", str(record_count)),
                        ),
                    )
            except sqlite3.IntegrityError as error:
                raise ValueError(f"cannot build manifest index: {error}") from error
            finally:
                connection.close()
            os.replace(temporary, destination)
        except BaseException:
            with suppress(OSError):
                os.unlink(temporary)
            raise
        return cls(destination)

    def __enter__(self) -> ManifestIndex:
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("manifest index is closed")

    def _validate_schema(self) -> None:
        tables = {row[0] for row in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != {"metadata", "records", "fields"}:
            raise ValueError("not a supported CorpusLedger manifest index")
        metadata = dict(self._connection.execute("SELECT key,value FROM metadata"))
        if metadata.get("format") != INDEX_FORMAT:
            raise ValueError("manifest index has an unsupported format")
        for key in ("manifest_digest", "corpus_hash", "record_count"):
            if key not in metadata or not metadata[key]:
                raise ValueError(f"manifest index is missing {key}")
        try:
            count = int(metadata["record_count"])
        except ValueError as error:
            raise ValueError("manifest index record_count is invalid") from error
        if count < 0:
            raise ValueError("manifest index record_count must not be negative")
        actual = self._connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        if int(actual) != count:
            raise ValueError("manifest index record count does not match metadata")

    @property
    def manifest_digest(self) -> str:
        self._ensure_open()
        return self._metadata("manifest_digest")

    @property
    def corpus_hash(self) -> str:
        self._ensure_open()
        return self._metadata("corpus_hash")

    @property
    def record_count(self) -> int:
        self._ensure_open()
        return int(self._metadata("record_count"))

    def _metadata(self, key: str) -> str:
        row = self._connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if row is None:
            raise ValueError(f"manifest index is missing {key}")
        return str(row[0])

    def get(self, record_id: str) -> IndexRecord:
        self._ensure_open()
        if not isinstance(record_id, str) or not record_id:
            raise ValueError("record_id must be a non-empty string")
        row = self._connection.execute(
            "SELECT record_id,record_hash,source,position FROM records WHERE record_id=?", (record_id,)
        ).fetchone()
        if row is None:
            raise KeyError(record_id)
        return self._record(row)

    def query(
        self,
        *,
        id_prefix: str | None = None,
        source: str | None = None,
        field_path: str | None = None,
        field_hash: str | None = None,
        limit: int = 100,
    ) -> tuple[IndexRecord, ...]:
        """Return deterministic bounded rows matching optional metadata filters."""

        self._ensure_open()
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise ValueError("limit must be an integer between 1 and 10000")
        for name, value in (
            ("id_prefix", id_prefix),
            ("source", source),
            ("field_path", field_path),
            ("field_hash", field_hash),
        ):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"{name} must be a non-empty string when provided")
        clauses: list[str] = []
        parameters: list[str] = []
        if id_prefix is not None:
            clauses.append("r.record_id LIKE ? ESCAPE '\\'")
            parameters.append(id_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        if source is not None:
            clauses.append("r.source = ?")
            parameters.append(source)
        join = ""
        if field_path is not None:
            join = " JOIN fields f ON f.record_id = r.record_id"
            clauses.append("f.path = ?")
            parameters.append(field_path)
        elif field_hash is not None:
            join = " JOIN fields f ON f.record_id = r.record_id"
        if field_hash is not None:
            clauses.append("f.field_hash = ?")
            parameters.append(field_hash)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._connection.execute(
            f"SELECT DISTINCT r.record_id,r.record_hash,r.source,r.position FROM records r{join}{where} "  # nosec B608 - fragments are fixed clauses; values stay parameterized.
            "ORDER BY r.record_id LIMIT ?",
            (*parameters, limit),
        )
        return tuple(self._record(row) for row in rows)

    def duplicate_fields(self, *, field_path: str | None = None, limit: int = 100) -> tuple[DuplicateFieldGroup, ...]:
        """Return bounded groups of equal field digests, excluding uniques."""

        self._ensure_open()
        if field_path is not None and (not isinstance(field_path, str) or not field_path):
            raise ValueError("field_path must be a non-empty string when provided")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10_000:
            raise ValueError("limit must be an integer between 1 and 10000")
        if field_path is None:
            rows = self._connection.execute(
                "SELECT path,field_hash,GROUP_CONCAT(record_id) FROM fields "
                "GROUP BY path,field_hash HAVING COUNT(*) > 1 "
                "ORDER BY path,field_hash LIMIT ?",
                (limit,),
            )
        else:
            rows = self._connection.execute(
                "SELECT path,field_hash,GROUP_CONCAT(record_id) FROM fields WHERE path = ? "
                "GROUP BY path,field_hash HAVING COUNT(*) > 1 "
                "ORDER BY path,field_hash LIMIT ?",
                (field_path, limit),
            )
        return tuple(
            DuplicateFieldGroup(
                str(path), str(field_hash), tuple(sorted(str(record) for record in str(ids).split(",")))
            )
            for path, field_hash, ids in rows
        )

    def _record(self, row: tuple[object, ...]) -> IndexRecord:
        fields = self._connection.execute("SELECT path FROM fields WHERE record_id=? ORDER BY path", (row[0],))
        return IndexRecord(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            int(str(row[3])),
            tuple(str(item[0]) for item in fields),
        )

    def verify(self, manifest: Manifest) -> None:
        """Raise ``ValueError`` unless every indexed entry matches a manifest."""

        self._ensure_open()
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a Manifest")
        if self.manifest_digest != _manifest_digest(manifest):
            raise ValueError("manifest index digest does not match manifest")
        if self.corpus_hash != manifest.corpus_hash or self.record_count != len(manifest.records):
            raise ValueError("manifest index identity does not match manifest")
        for entry in manifest.records:
            indexed = self.get(entry.record_id)
            if (
                indexed.record_hash != entry.hash
                or indexed.source != entry.source
                or indexed.position != entry.position
            ):
                raise ValueError(f"manifest index record mismatch: {entry.record_id!r}")
            if indexed.field_paths != tuple(sorted(entry.field_hashes)):
                raise ValueError(f"manifest index fields mismatch: {entry.record_id!r}")

    def stats(self) -> dict[str, int | str]:
        """Return a compact inventory suitable for audit reports."""

        self._ensure_open()
        field_count = int(self._connection.execute("SELECT COUNT(*) FROM fields").fetchone()[0])
        return {
            "format": INDEX_FORMAT,
            "manifest_digest": self.manifest_digest,
            "corpus_hash": self.corpus_hash,
            "records": self.record_count,
            "field_paths": field_count,
        }

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA user_version = 1;
        CREATE TABLE metadata(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        CREATE TABLE records(
            record_id TEXT PRIMARY KEY NOT NULL,
            record_hash TEXT NOT NULL,
            source TEXT NOT NULL,
            position INTEGER NOT NULL CHECK(position > 0)
        );
        CREATE TABLE fields(
            record_id TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
            path TEXT NOT NULL,
            field_hash TEXT NOT NULL,
            PRIMARY KEY(record_id, path)
        );
        CREATE INDEX records_source_idx ON records(source);
        CREATE INDEX fields_path_idx ON fields(path);
        """
    )
