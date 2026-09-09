"""Transactional, content-addressed multi-document annotation event history.

This local store uses optimistic revisions. Hashes detect accidental corruption;
they are not signatures or protection from an attacker rewriting the database.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any

from .annotation_pipeline import AnnotationPipeline
from .annotations import AnnotationDocument, _freeze, _name, _object, _thaw
from .errors import InputError
from .strictjson import bounded_int, finite_float, object_without_duplicates, reject_constant

EVENT_FORMAT = "corpusledger.annotation-event.v1"
STORE_FORMAT = "corpusledger.annotation-store.v1"
MAX_EVENT_DOCUMENTS = 10_000
MAX_EVENT_BYTES = 128 * 1024 * 1024
_APPLICATION_ID = 0x434C4153


class AnnotationStoreError(InputError):
    """A store, event payload, or durable history is invalid or unavailable."""


class AnnotationConflictError(AnnotationStoreError):
    """The current revision differs from the caller's explicit expectation."""

    def __init__(self, event_id: str, expected_revision: int, actual_revision: int) -> None:
        self.actual_revision = actual_revision
        self.expected_revision = expected_revision
        super().__init__(f"event {event_id!r} revision conflict: expected {expected_revision}, found {actual_revision}")


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise AnnotationStoreError("event payload is not supported finite JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _loads(raw: Any) -> Any:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_EVENT_BYTES:
        raise AnnotationStoreError("stored payload is invalid or exceeds the event byte limit")
    try:
        return json.loads(
            raw,
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
            parse_int=bounded_int,
        )
    except (ValueError, RecursionError) as exc:
        raise AnnotationStoreError("stored payload contains invalid or ambiguous JSON") from exc


def _revision(value: int, *, zero: bool = False) -> None:
    if type(value) is not int or value < (0 if zero else 1) or value >= 2**63 - 1:
        raise AnnotationStoreError(
            "revision must be a bounded non-negative integer" if zero else "revision must be a positive bounded integer"
        )


def _limit(value: int) -> None:
    if type(value) is not int or not 1 <= value <= 1000:
        raise AnnotationStoreError("limit must be an integer between 1 and 1000")


@dataclass(frozen=True, slots=True)
class AnnotationEvent:
    """Named, immutable collection of related documents and finite JSON metadata."""

    event_id: str
    documents: tuple[AnnotationDocument, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _name(self.event_id, "event ID")
        if not isinstance(self.documents, (tuple, list)) or len(self.documents) > MAX_EVENT_DOCUMENTS:
            raise AnnotationStoreError(
                f"event documents must be a tuple/list of at most {MAX_EVENT_DOCUMENTS} documents"
            )
        if any(not isinstance(document, AnnotationDocument) for document in self.documents):
            raise AnnotationStoreError("event documents must contain AnnotationDocument values")
        ids = [document.document_id for document in self.documents]
        if len(ids) != len(set(ids)):
            raise AnnotationStoreError("event document IDs must be unique")
        if not isinstance(self.metadata, Mapping):
            raise AnnotationStoreError("event metadata must be an object")
        object.__setattr__(self, "documents", tuple(sorted(self.documents, key=lambda item: item.document_id)))
        object.__setattr__(self, "metadata", _freeze(self.metadata))

    @property
    def digest(self) -> str:
        return _digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": EVENT_FORMAT,
            "id": self.event_id,
            "documents": [document.to_dict() for document in self.documents],
            "metadata": _thaw(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Any) -> AnnotationEvent:
        data = _object(value, {"format", "id", "documents", "metadata"}, "annotation event")
        if data["format"] != EVENT_FORMAT or not isinstance(data["documents"], list):
            raise AnnotationStoreError("unsupported event format or documents field")
        if len(data["documents"]) > MAX_EVENT_DOCUMENTS:
            raise AnnotationStoreError(f"event exceeds {MAX_EVENT_DOCUMENTS} documents")
        return cls(
            data["id"], tuple(AnnotationDocument.from_dict(item) for item in data["documents"]), data["metadata"]
        )

    def get_document(self, document_id: str) -> AnnotationDocument:
        for document in self.documents:
            if document.document_id == document_id:
                return document
        raise KeyError(document_id)


@dataclass(frozen=True, slots=True)
class AnnotationRevisionInfo:
    """Small immutable revision descriptor, excluding raw text/feature values."""

    event_id: str
    revision: int
    digest: str
    parent_digest: str | None
    documents: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "revision": self.revision,
            "digest": self.digest,
            "parent_digest": self.parent_digest,
            "documents": list(self.documents),
        }


@dataclass(frozen=True, slots=True)
class AnnotationRevision(AnnotationRevisionInfo):
    """Materialized event revision and its caller-declared provenance."""

    event: AnnotationEvent
    provenance: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": "corpusledger.annotation-revision.v1",
            **AnnotationRevisionInfo.to_dict(self),
            "event": self.event.to_dict(),
            "provenance": _thaw(self.provenance),
        }


@dataclass(frozen=True, slots=True)
class AnnotationStoreVerification:
    events: int
    revisions: int
    documents: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": "corpusledger.annotation-store-verification.v1",
            "events": self.events,
            "revisions": self.revisions,
            "documents": self.documents,
            "valid": True,
        }


class AnnotationStore:
    """SQLite event history with content-addressed documents and atomic CAS writes.

    ``create=False`` requires an existing store; it does not mean read-only.
    Connections are thread-affine. Independent connections/processes can write
    safely using explicit revisions; callbacks are never run under a write lock.
    """

    def __init__(self, path: str | Path, *, create: bool = True, timeout: float = 5.0) -> None:
        if (
            type(create) is not bool
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise AnnotationStoreError("create must be boolean and timeout must be finite/non-negative")
        self._closed = False
        try:
            self.path = Path(path).resolve()
            uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
            self._connection = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None)
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            raise AnnotationStoreError("cannot open annotation store") from exc
        try:
            with self._transaction():
                needs_initialization = not self._tables() and create
            if needs_initialization:
                with self._transaction(write=True):
                    # Recheck after acquiring the writer lock: another connection
                    # may have initialized this empty store in the meantime.
                    if not self._tables():
                        self._connection.execute("CREATE TABLE documents(digest TEXT PRIMARY KEY, body TEXT NOT NULL)")
                        self._connection.execute(
                            "CREATE TABLE revisions(event_id TEXT NOT NULL, revision INTEGER NOT NULL, "
                            "digest TEXT NOT NULL UNIQUE, parent_digest TEXT, body TEXT NOT NULL, "
                            "PRIMARY KEY(event_id,revision))"
                        )
                        self._connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                        self._connection.execute("PRAGMA user_version=1")
            with self._transaction():
                if self._tables() != {"documents", "revisions"}:
                    raise AnnotationStoreError("database is not a supported annotation store")
                if (
                    self._connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
                    or self._connection.execute("PRAGMA user_version").fetchone()[0] != 1
                ):
                    raise AnnotationStoreError("unsupported annotation store identity/version")
                for table, columns in (
                    ("documents", ("digest", "body")),
                    ("revisions", ("event_id", "revision", "digest", "parent_digest", "body")),
                ):
                    # Identifiers are fixed literals, never derived from event input.
                    actual = tuple(row[1] for row in self._connection.execute(f"PRAGMA table_info({table})"))
                    if actual != columns:
                        raise AnnotationStoreError("annotation store schema does not match its format")
        except BaseException:
            self.close()
            raise

    def _tables(self) -> set[str]:
        return {row[0] for row in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def __enter__(self) -> AnnotationStore:
        self._ensure_open()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise AnnotationStoreError("annotation store is closed")

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Iterator[None]:
        self._ensure_open()
        try:
            self._connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield
            self._connection.execute("COMMIT")
        except BaseException as exc:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            if isinstance(exc, sqlite3.Error):
                raise AnnotationStoreError("annotation store transaction failed") from exc
            raise

    def _descriptor(self, row: tuple[Any, ...]) -> dict[str, Any]:
        event_id, revision, digest, parent, raw = row
        data = _object(
            _loads(raw),
            {"format", "event_id", "revision", "parent_digest", "documents", "metadata", "provenance"},
            "stored revision",
        )
        if data["format"] != STORE_FORMAT or (data["event_id"], data["revision"], data["parent_digest"]) != (
            event_id,
            revision,
            parent,
        ):
            raise AnnotationStoreError("stored revision identity does not match its index")
        _name(event_id, "event ID")
        _revision(revision)
        _revision(data["revision"])
        if _digest(data) != digest:
            raise AnnotationStoreError("stored revision digest does not match")
        if not isinstance(data["documents"], list) or len(data["documents"]) > MAX_EVENT_DOCUMENTS:
            raise AnnotationStoreError("stored revision document index is invalid")
        ids = []
        for reference in data["documents"]:
            entry = _object(reference, {"id", "digest"}, "document reference")
            _name(entry["id"], "document ID")
            if (
                not isinstance(entry["digest"], str)
                or len(entry["digest"]) != 64
                or any(c not in "0123456789abcdef" for c in entry["digest"])
            ):
                raise AnnotationStoreError("stored document digest is invalid")
            ids.append(entry["id"])
        if ids != sorted(set(ids)):
            raise AnnotationStoreError("stored document IDs must be unique and sorted")
        if not isinstance(data["metadata"], Mapping) or not isinstance(data["provenance"], Mapping):
            raise AnnotationStoreError("stored metadata/provenance must be objects")
        _freeze(data["metadata"])
        _freeze(data["provenance"])
        return dict(data)

    def _chain(
        self, event_id: str, through: int | None = None
    ) -> Iterator[tuple[AnnotationRevisionInfo, dict[str, Any]]]:
        cursor = self._connection.execute(
            "SELECT event_id,revision,digest,parent_digest,body FROM revisions "
            "WHERE event_id=? AND (? IS NULL OR revision<=?) ORDER BY revision",
            (event_id, through, through),
        )
        prior: str | None = None
        expected = 1
        for row in cursor:
            data = self._descriptor(row)
            if row[1] != expected or row[3] != prior:
                raise AnnotationStoreError("annotation revision history has a gap or broken parent link")
            info = AnnotationRevisionInfo(
                row[0], row[1], row[2], row[3], tuple(item["id"] for item in data["documents"])
            )
            yield info, data
            prior = row[2]
            expected += 1

    def _document(self, digest: str) -> AnnotationDocument:
        row = self._connection.execute("SELECT body FROM documents WHERE digest=?", (digest,)).fetchone()
        if row is None:
            raise AnnotationStoreError("stored revision references a missing document")
        document = AnnotationDocument.from_dict(_loads(row[0]))
        if document.digest != digest:
            raise AnnotationStoreError("stored document content digest does not match")
        return document

    def _get(self, event_id: str, revision: int | None) -> AnnotationRevision:
        selected = None
        for info, data in self._chain(event_id, revision):
            selected = info, data
        if selected is None or (revision is not None and selected[0].revision != revision):
            raise KeyError((event_id, revision))
        info, data = selected
        documents = tuple(self._document(item["digest"]) for item in data["documents"])
        if tuple(item.document_id for item in documents) != info.documents:
            raise AnnotationStoreError("document IDs do not match stored references")
        event = AnnotationEvent(event_id, documents, data["metadata"])
        return AnnotationRevision(
            info.event_id,
            info.revision,
            info.digest,
            info.parent_digest,
            info.documents,
            event,
            _freeze(data["provenance"]),
        )

    def get(self, event_id: str, revision: int | None = None) -> AnnotationRevision:
        """Validate descriptor ancestry and materialize one consistent revision."""
        _name(event_id, "event ID")
        if revision is not None:
            _revision(revision)
        with self._transaction():
            return self._get(event_id, revision)

    def put(
        self, event: AnnotationEvent, *, expected_revision: int = 0, provenance: Mapping[str, Any] | None = None
    ) -> AnnotationRevision:
        """Append all documents atomically. Expected zero means create-only."""
        if not isinstance(event, AnnotationEvent):
            raise AnnotationStoreError("event must be an AnnotationEvent")
        _revision(expected_revision, zero=True)
        if provenance is not None and not isinstance(provenance, Mapping):
            raise AnnotationStoreError("provenance must be an object")
        evidence = _thaw(_freeze(provenance if provenance is not None else {}))
        bodies = []
        document_bytes = 0
        for document in event.documents:
            body = _json(document.to_dict())
            encoded = body.encode("utf-8")
            document_bytes += len(encoded)
            if document_bytes > MAX_EVENT_BYTES:
                raise AnnotationStoreError("event documents exceed byte limit")
            bodies.append((hashlib.sha256(encoded).hexdigest(), body))
        with self._transaction(write=True):
            previous = None
            for info, _ in self._chain(event.event_id):
                previous = info
            actual = previous.revision if previous else 0
            if actual != expected_revision:
                raise AnnotationConflictError(event.event_id, expected_revision, actual)
            parent = previous.digest if previous else None
            payload = {
                "format": STORE_FORMAT,
                "event_id": event.event_id,
                "revision": actual + 1,
                "parent_digest": parent,
                "metadata": _thaw(event.metadata),
                "provenance": evidence,
                "documents": [
                    {"id": document.document_id, "digest": digest}
                    for document, (digest, _) in zip(event.documents, bodies, strict=True)
                ],
            }
            rendered = _json(payload)
            if len(rendered.encode("utf-8")) + document_bytes > MAX_EVENT_BYTES:
                raise AnnotationStoreError("complete event exceeds byte limit")
            digest = _digest(payload)
            for document_digest, body in bodies:
                existing = self._connection.execute(
                    "SELECT body FROM documents WHERE digest=?", (document_digest,)
                ).fetchone()
                if existing is not None and existing[0] != body:
                    raise AnnotationStoreError("existing document content conflicts with its digest")
                self._connection.execute(
                    "INSERT OR IGNORE INTO documents(digest,body) VALUES(?,?)", (document_digest, body)
                )
            self._connection.execute(
                "INSERT INTO revisions(event_id,revision,digest,parent_digest,body) VALUES(?,?,?,?,?)",
                (event.event_id, actual + 1, digest, parent, rendered),
            )
            return AnnotationRevision(
                event.event_id,
                actual + 1,
                digest,
                parent,
                tuple(document.document_id for document in event.documents),
                event,
                _freeze(evidence),
            )

    def list(self, *, after_event_id: str | None = None, limit: int = 100) -> tuple[AnnotationRevisionInfo, ...]:
        """List latest descriptors in binary event-ID order with an exclusive cursor."""
        _limit(limit)
        if after_event_id is not None:
            _name(after_event_id, "event cursor")
        with self._transaction():
            ids = [
                row[0]
                for row in self._connection.execute(
                    "SELECT DISTINCT event_id FROM revisions WHERE (? IS NULL OR event_id>?) ORDER BY event_id LIMIT ?",
                    (after_event_id, after_event_id, limit),
                )
            ]
            results = []
            for event_id in ids:
                latest = None
                for info, _ in self._chain(event_id):
                    latest = info
                if latest is not None:
                    results.append(latest)
            return tuple(results)

    def history(
        self, event_id: str, *, after_revision: int = 0, limit: int = 100
    ) -> tuple[AnnotationRevisionInfo, ...]:
        """Verify descriptor history and return a bounded exclusive-cursor page."""
        _name(event_id, "event ID")
        _revision(after_revision, zero=True)
        _limit(limit)
        with self._transaction():
            result: list[AnnotationRevisionInfo] = []
            found = False
            for info, _ in self._chain(event_id):
                found = True
                if info.revision > after_revision and len(result) < limit:
                    result.append(info)
            if not found:
                raise KeyError(event_id)
            return tuple(result)

    def verify(self, event_id: str | None = None) -> AnnotationStoreVerification:
        """Verify every selected descriptor, ancestry link and referenced document."""
        if event_id is not None:
            _name(event_id, "event ID")
        with self._transaction():
            if self._connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise AnnotationStoreError("SQLite integrity check failed")
            ids = [
                row[0]
                for row in self._connection.execute(
                    "SELECT DISTINCT event_id FROM revisions WHERE (? IS NULL OR event_id=?) ORDER BY event_id",
                    (event_id, event_id),
                )
            ]
            if event_id is not None and not ids:
                raise KeyError(event_id)
            checked: dict[str, str] = {}
            revisions = 0
            for identifier in ids:
                for _, data in self._chain(identifier):
                    revisions += 1
                    for reference in data["documents"]:
                        digest = reference["digest"]
                        if digest not in checked:
                            checked[digest] = self._document(digest).document_id
                        if checked[digest] != reference["id"]:
                            raise AnnotationStoreError("document IDs do not match stored references")
            return AnnotationStoreVerification(len(ids), revisions, len(checked))

    def process(
        self, event_id: str, pipelines: Mapping[str, AnnotationPipeline], *, expected_revision: int
    ) -> AnnotationRevision:
        """Process selected documents, then atomically publish a CAS revision.

        Preflight all DAGs before callbacks. A concurrent update rejects commit;
        callback external side effects cannot be undone or made exactly-once.
        """
        _revision(expected_revision)
        if not isinstance(pipelines, Mapping) or not pipelines:
            raise AnnotationStoreError("pipelines must be a non-empty document-ID mapping")
        planned = {}
        for document_id, pipeline in dict(pipelines).items():
            _name(document_id, "pipeline document ID")
            if not isinstance(pipeline, AnnotationPipeline):
                raise AnnotationStoreError("pipeline values must be AnnotationPipeline instances")
            planned[document_id] = AnnotationPipeline(pipeline.processors)
        source = self.get(event_id)
        if source.revision != expected_revision:
            raise AnnotationConflictError(event_id, expected_revision, source.revision)
        for document_id, pipeline in planned.items():
            pipeline.plan(source.event.get_document(document_id))
        documents = []
        provenance = {}
        for document in source.event.documents:
            if document.document_id in planned:
                result = planned[document.document_id].run(document)
                documents.append(result.document)
                provenance[document.document_id] = result.to_dict()
            else:
                documents.append(document)
        event = AnnotationEvent(event_id, tuple(documents), source.event.metadata)
        return self.put(event, expected_revision=expected_revision, provenance={"annotation_pipelines": provenance})
