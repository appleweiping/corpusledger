"""Transactional catalog of named corpus snapshots and lineage."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from .diff import CorpusDiff, compare
from .manifest import Manifest


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    """Catalog identity and immutable manifest digest."""

    name: str
    version: int
    corpus_hash: str
    parent: str | None
    tags: tuple[str, ...]


class SnapshotCatalog:
    """A local catalog; manifest contents are copied into the transaction."""

    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(str(path))
        self._closed = False
        try:
            with self._connection:
                if not self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                    self._connection.executescript(
                        "CREATE TABLE snapshots(name TEXT NOT NULL, version INTEGER NOT NULL, "
                        "corpus_hash TEXT NOT NULL, parent TEXT, tags TEXT NOT NULL, manifest TEXT NOT NULL, "
                        "PRIMARY KEY(name,version)); PRAGMA user_version=1;"
                    )
                if self._connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                    raise ValueError("not a supported CorpusLedger snapshot catalog")
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> SnapshotCatalog:
        self._ensure_open()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("snapshot catalog is closed")

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True

    def register(
        self, name: str, manifest: Manifest, *, parent: str | None = None, tags: tuple[str, ...] = ()
    ) -> SnapshotRef:
        """Append a snapshot version; parent must already exist if provided."""
        self._ensure_open()
        if not isinstance(name, str) or not name.strip() or any(character.isspace() for character in name):
            raise ValueError("snapshot name must be a non-empty token")
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a Manifest")
        if len(set(tags)) != len(tags) or any(not isinstance(tag, str) or not tag.strip() for tag in tags):
            raise ValueError("tags must contain unique non-empty strings")
        if (
            parent is not None
            and self._connection.execute("SELECT 1 FROM snapshots WHERE corpus_hash=?", (parent,)).fetchone() is None
        ):
            raise KeyError(f"unknown parent corpus hash: {parent}")
        latest = self._connection.execute(
            "SELECT version FROM snapshots WHERE name=? ORDER BY version DESC LIMIT 1", (name,)
        ).fetchone()
        version = int(latest[0]) + 1 if latest else 1
        body = json.dumps(
            manifest.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        with self._connection:
            self._connection.execute(
                "INSERT INTO snapshots(name,version,corpus_hash,parent,tags,manifest) VALUES(?,?,?,?,?,?)",
                (name, version, manifest.corpus_hash, parent, json.dumps(sorted(tags)), body),
            )
        return SnapshotRef(name, version, manifest.corpus_hash, parent, tuple(sorted(tags)))

    def list(self, name: str | None = None) -> tuple[SnapshotRef, ...]:
        """List snapshots in stable name/version order."""
        self._ensure_open()
        if name is None:
            rows = self._connection.execute(
                "SELECT name,version,corpus_hash,parent,tags FROM snapshots ORDER BY name,version"
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT name,version,corpus_hash,parent,tags FROM snapshots WHERE name=? ORDER BY version", (name,)
            ).fetchall()
        return tuple(SnapshotRef(row[0], row[1], row[2], row[3], tuple(json.loads(row[4]))) for row in rows)

    def manifest(self, name: str, version: int | None = None) -> Manifest:
        """Load a detached manifest, defaulting to the latest named version."""
        self._ensure_open()
        if version is None:
            row = self._connection.execute(
                "SELECT manifest FROM snapshots WHERE name=? ORDER BY version DESC LIMIT 1", (name,)
            ).fetchone()
        else:
            row = self._connection.execute(
                "SELECT manifest FROM snapshots WHERE name=? AND version=?", (name, version)
            ).fetchone()
        if row is None:
            raise KeyError((name, version))
        with tempfile.TemporaryDirectory(prefix="corpusledger-manifest-") as directory:
            temporary = Path(directory) / "manifest.json"
            temporary.write_text(row[0] + "\n", encoding="utf-8")
            return Manifest.load(temporary)

    def diff(self, name: str, before: int, after: int) -> CorpusDiff:
        """Diff two versions from one named snapshot series."""
        return compare(self.manifest(name, before), self.manifest(name, after))

    def lineage(self, corpus_hash: str) -> tuple[SnapshotRef, ...]:
        """Follow parent hashes from a snapshot to its root."""
        result: list[SnapshotRef] = []
        current = corpus_hash
        seen: set[str] = set()
        while current is not None:
            if current in seen:
                raise ValueError("snapshot catalog contains a parent cycle")
            seen.add(current)
            row = self._connection.execute(
                "SELECT name,version,corpus_hash,parent,tags FROM snapshots WHERE corpus_hash=?", (current,)
            ).fetchone()
            if row is None:
                raise KeyError(current)
            result.append(SnapshotRef(row[0], row[1], row[2], row[3], tuple(json.loads(row[4]))))
            current = row[3]
        return tuple(result)
