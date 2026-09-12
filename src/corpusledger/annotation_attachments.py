"""Atomic, content-addressed attachment commands bound to event revisions."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from .annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationRevision,
    AnnotationStore,
    AnnotationStoreError,
    _loads,
)
from .annotations import _name, _object
from .attachment_types import (
    AttachmentError,
    AttachmentLimits,
    AttachmentManifest,
    AttachmentQuotaError,
    bounded_json,
    integer,
    json_digest,
    logical_name,
    manifests,
    sha256,
)
from .attachment_types import (
    command_id as validate_command_id,
)
from .errors import InputError

RECEIPT_FORMAT = "corpusledger.annotation-attachment-receipt.v1"
REQUEST_FORMAT = "corpusledger.annotation-attachment-command.v1"
MAX_RECEIPT_BYTES = 64 * 1024


class AttachmentConflictError(AnnotationStoreError):
    """A revision digest, logical name, or command binding conflicts."""


class AttachmentCorruptionError(AnnotationStoreError):
    """Persisted attachment content or a command receipt cannot be verified."""


def attachment_request(
    action: str,
    event_id: str,
    *,
    expected_revision: int,
    expected_digest: str | None,
    manifest: AttachmentManifest | None = None,
    name: str | None = None,
    snapshot_digest: str | None = None,
    event_digest: str | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _name(event_id, "event ID")
    integer(expected_revision, "expected revision", 0, 2**63 - 2)
    if expected_revision == 0:
        if expected_digest is not None or action != "import":
            raise AttachmentError("only create-only snapshot import accepts revision zero and digest None")
    else:
        sha256(expected_digest)
    base = {
        "format": REQUEST_FORMAT,
        "action": action,
        "event_id": event_id,
        "expected_revision": expected_revision,
        "expected_digest": expected_digest,
    }
    if action == "attach":
        if not isinstance(manifest, AttachmentManifest) or any(
            item is not None for item in (name, snapshot_digest, event_digest, source)
        ):
            raise AttachmentError("attach requires only an attachment manifest")
        base["manifest"] = manifest.to_dict()
    elif action == "detach":
        if any(item is not None for item in (manifest, snapshot_digest, event_digest, source)):
            raise AttachmentError("detach requires only a logical name")
        base["name"] = logical_name(name)
    elif action == "import":
        if manifest is not None or name is not None:
            raise AttachmentError("snapshot import cannot contain attach/detach fields")
        base["snapshot_digest"], base["event_digest"] = sha256(snapshot_digest), sha256(event_digest)
        origin = _object(source, {"event_id", "revision", "digest"}, "snapshot source")
        _name(origin["event_id"], "snapshot source event ID")
        if origin["event_id"] != event_id:
            raise AttachmentError("snapshot source and target IDs must match; renaming is not supported")
        integer(origin["revision"], "snapshot source revision", 1, 2**63 - 2)
        sha256(origin["digest"])
        base["source"] = dict(origin)
    else:
        raise AttachmentError("unsupported attachment command")
    return base


def _validate_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AttachmentError("attachment request must be an object")
    base = {"format", "action", "event_id", "expected_revision", "expected_digest"}
    extra = {"attach": {"manifest"}, "detach": {"name"}, "import": {"snapshot_digest", "event_digest", "source"}}
    action = value.get("action")
    if not isinstance(action, str) or action not in extra:
        raise AttachmentError("unsupported attachment command")
    data = _object(value, base | extra[action], "attachment request")
    if data["format"] != REQUEST_FORMAT:
        raise AttachmentError("unsupported attachment request format")
    fields = {key: data[key] for key in extra[action]}
    if action == "attach":
        fields["manifest"] = AttachmentManifest.from_dict(fields["manifest"])
    return attachment_request(
        action,
        data["event_id"],
        expected_revision=data["expected_revision"],
        expected_digest=data["expected_digest"],
        **fields,
    )


class AnnotationAttachments:
    """Thread-affine commands; independent store connections serialize CAS writes."""

    def __init__(self, store: AnnotationStore, limits: AttachmentLimits | None = None) -> None:
        if not isinstance(store, AnnotationStore) or not store.attachments_enabled:
            raise AttachmentError("explicitly enable store attachments first")
        self.store = store
        self.limits = limits if limits is not None else AttachmentLimits()
        if not isinstance(self.limits, AttachmentLimits):
            raise AttachmentError("limits must be AttachmentLimits")

    def _receipt(self, identifier: str, request: dict[str, Any] | None = None) -> AnnotationRevision | None:
        try:
            return self._read_receipt(identifier, request)
        except AttachmentConflictError:
            raise
        except (InputError, KeyError) as error:
            raise AttachmentCorruptionError("stored attachment receipt cannot be verified") from error

    def _read_receipt(self, identifier: str, request: dict[str, Any] | None = None) -> AnnotationRevision | None:
        connection = self.store._connection
        header = connection.execute(
            "SELECT typeof(body),length(CAST(body AS BLOB)),"
            "typeof(request_digest),length(CAST(request_digest AS BLOB)) "
            "FROM annotation_attachment_receipts WHERE command_id=?",
            (identifier,),
        ).fetchone()
        if header is None:
            return None
        if header[0] != "text" or type(header[1]) is not int or not 1 <= header[1] <= MAX_RECEIPT_BYTES:
            raise AnnotationStoreError("attachment receipt has invalid type/size")
        if header[2:] != ("text", 64):
            raise AttachmentCorruptionError("attachment receipt digest has invalid type/size")
        # Both payload columns have been admitted in this same read/write
        # transaction before either can be materialized in Python.
        body, indexed_digest = connection.execute(
            "SELECT body,request_digest FROM annotation_attachment_receipts WHERE command_id=?", (identifier,)
        ).fetchone()
        data = _object(
            _loads(body),
            {"format", "command_id", "request_digest", "request", "result", "sha256"},
            "attachment receipt",
        )
        if data["format"] != RECEIPT_FORMAT or data["command_id"] != identifier:
            raise AnnotationStoreError("attachment receipt identity does not match")
        sha256(data["sha256"])
        expected_hash = json_digest({key: item for key, item in data.items() if key != "sha256"})
        saved_request = _validate_request(data["request"])
        request_hash = json_digest(saved_request)
        if data["sha256"] != expected_hash or request_hash != data["request_digest"] or request_hash != indexed_digest:
            raise AnnotationStoreError("attachment receipt digest does not match")
        if request is not None and bounded_json(request, MAX_RECEIPT_BYTES) != bounded_json(
            saved_request, MAX_RECEIPT_BYTES
        ):
            raise AttachmentConflictError("attachment command ID is bound to a different request")
        result = _object(data["result"], {"event_id", "revision", "digest"}, "attachment receipt result")
        if (
            result["event_id"] != saved_request["event_id"]
            or type(result["revision"]) is not int
            or result["revision"] != saved_request["expected_revision"] + 1
        ):
            raise AnnotationStoreError("attachment receipt result identity does not match its request")
        sha256(result["digest"])
        revision = self.store._get(result["event_id"], result["revision"])
        if revision.digest != result["digest"] or revision.parent_digest != saved_request["expected_digest"]:
            raise AnnotationStoreError("attachment receipt pinned revision does not match")
        expected_provenance = {
            "annotation_attachment": {
                "command_id": identifier,
                "request_digest": request_hash,
                "request": saved_request,
            }
        }
        if json_digest(revision.provenance) != json_digest(expected_provenance):
            raise AnnotationStoreError("attachment receipt provenance does not match")
        expected_event = self._event(
            saved_request,
            self._source(saved_request),
            revision.event if saved_request["action"] == "import" else None,
            enforce_limits=False,
        )
        if expected_event.digest != revision.event.digest:
            raise AnnotationStoreError("attachment receipt result does not implement its bound command")
        return revision

    def receipt(self, command_id: str) -> AnnotationRevision:
        validate_command_id(command_id)
        with self.store._transaction():
            result = self._receipt(command_id)
            if result is None:
                raise KeyError(command_id)
            return result

    def _source(self, request: dict[str, Any]) -> AnnotationRevision | None:
        if not request["expected_revision"]:
            return None
        source = self.store._get(request["event_id"], request["expected_revision"])
        if source.digest != request["expected_digest"]:
            raise AttachmentConflictError("attachment source revision digest does not match")
        return source

    def _event(
        self,
        request: dict[str, Any],
        source: AnnotationRevision | None,
        imported: AnnotationEvent | None = None,
        *,
        enforce_limits: bool = True,
    ) -> AnnotationEvent:
        if request["action"] == "import":
            if imported is None or imported.digest != request["event_digest"]:
                raise AttachmentError("snapshot event does not match its command")
            if source is not None and source.event.version == 2 and imported.version == 1:
                raise AnnotationStoreError("an existing event v2 cannot be downgraded to v1")
            event = imported
        else:
            if source is None:
                raise AttachmentError("attachment command requires an existing source revision")
            selected = {item.name: item for item in source.event.attachments}
            if request["action"] == "attach":
                item = AttachmentManifest.from_dict(request["manifest"])
                if item.name in selected:
                    raise AttachmentConflictError("attachment name already exists; detach it explicitly first")
                selected[item.name] = item
            else:
                if request["name"] not in selected:
                    raise KeyError(request["name"])
                del selected[request["name"]]
            event = replace(source.event, attachments=tuple(selected.values()), version=2)
        manifests(event.attachments, self.limits if enforce_limits else None)
        return event

    def _prepared(self, identifier: str, request: dict[str, Any], event: AnnotationEvent) -> Any:
        provenance = {
            "annotation_attachment": {
                "command_id": identifier,
                "request_digest": json_digest(request),
                "request": request,
            }
        }
        return self.store._prepare_append(event, provenance)

    def _preview(
        self, identifier: str, request: dict[str, Any], imported: AnnotationEvent | None = None
    ) -> AnnotationRevision:
        validate_command_id(identifier)
        with self.store._transaction():
            existing = self._receipt(identifier, request)
            if existing is not None:
                return existing
            source = self._source(request)
        event = self._event(request, source, imported)
        prepared = self._prepared(identifier, request, event)
        revision = request["expected_revision"] + 1
        parent = request["expected_digest"]
        digest = hashlib.sha256(prepared.render(revision, parent).encode("utf-8")).hexdigest()
        return AnnotationRevision(
            event.event_id,
            revision,
            digest,
            parent,
            tuple(doc.document_id for doc in event.documents),
            event,
            prepared.provenance,
        )

    def _admit(self, blobs: Mapping[str, bytes]) -> None:
        connection = self.store._connection
        count = total = 0
        for digest, size, storage_type, actual_size, size_type in connection.execute(
            # SQL CASE admits each digest before projecting it: corrupt long
            # TEXT/BLOB values become NULL, never unbounded Python objects.
            # Likewise, SQLite affinity must not expose a corrupt text `size`.
            "SELECT CASE WHEN typeof(sha256)='text' AND length(CAST(sha256 AS BLOB))=64 "
            "THEN sha256 END AS sha256,"
            "CASE WHEN typeof(size)='integer' THEN size END AS size,"
            "typeof(body),length(body),typeof(size) FROM annotation_blobs"
        ):
            try:
                sha256(digest)
                integer(size, "stored attachment size", 0, 4 * 1024 * 1024)
            except AttachmentError as error:
                raise AttachmentCorruptionError("attachment storage has invalid indexed metadata") from error
            if storage_type != "blob" or size_type != "integer" or actual_size != size:
                raise AnnotationStoreError("attachment storage contains invalid BLOB type/size")
            count += 1
            total += size
            if count > self.limits.max_store_blobs or total > self.limits.max_store_bytes:
                raise AttachmentQuotaError("existing attachment storage exceeds configured physical quota")
        receipts = connection.execute("SELECT COUNT(*) FROM annotation_attachment_receipts").fetchone()[0]
        if receipts >= self.limits.max_receipts:
            raise AttachmentQuotaError("attachment receipt quota exhausted")
        for digest, content in blobs.items():
            previous = connection.execute("SELECT 1 FROM annotation_blobs WHERE sha256=?", (digest,)).fetchone()
            if previous is not None:
                check = AttachmentManifest("blob", digest, len(content), "application/octet-stream")
                if self.store._attachment_blob(check) != content:
                    raise AnnotationStoreError("existing attachment content conflicts with its digest")
                continue
            count += 1
            total += len(content)
            if count > self.limits.max_store_blobs or total > self.limits.max_store_bytes:
                raise AttachmentQuotaError("attachment command exceeds physical storage quota")
            connection.execute(
                "INSERT INTO annotation_blobs(sha256,size,body) VALUES(?,?,?)", (digest, len(content), content)
            )

    def _publish(
        self,
        identifier: str,
        request: dict[str, Any],
        blobs: Mapping[str, bytes],
        imported: AnnotationEvent | None = None,
    ) -> AnnotationRevision:
        preview = self._preview(identifier, request, imported)
        prepared = self._prepared(identifier, request, preview.event)
        with self.store._transaction(write=True):
            existing = self._receipt(identifier, request)
            if existing is not None:
                return existing
            latest = None
            for info, _ in self.store._chain(request["event_id"]):
                latest = info
            actual = latest.revision if latest else 0
            if actual != request["expected_revision"]:
                raise AnnotationConflictError(request["event_id"], request["expected_revision"], actual)
            if (latest.digest if latest else None) != request["expected_digest"]:
                raise AttachmentConflictError("attachment source digest changed before commit")
            self._admit(blobs)
            result = self.store._append_in_transaction(prepared, expected_revision=request["expected_revision"])
            body = {
                "format": RECEIPT_FORMAT,
                "command_id": identifier,
                "request_digest": json_digest(request),
                "request": request,
                "result": {"event_id": result.event_id, "revision": result.revision, "digest": result.digest},
            }
            body["sha256"] = json_digest(body)
            encoded = bounded_json(body, MAX_RECEIPT_BYTES).decode("utf-8")
            self.store._connection.execute(
                "INSERT INTO annotation_attachment_receipts(command_id,request_digest,body) VALUES(?,?,?)",
                (identifier, body["request_digest"], encoded),
            )
            return result

    def _attach_request(
        self, event_id: str, name: str, data: bytes, media_type: str, expected_revision: int, expected_digest: str
    ) -> dict[str, Any]:
        if type(data) is not bytes:
            raise AttachmentError("attachment data must be immutable bytes")
        if len(data) > self.limits.max_blob_bytes:
            raise AttachmentQuotaError("attachment data exceeds the blob limit")
        manifest = AttachmentManifest(name, hashlib.sha256(data).hexdigest(), len(data), media_type)
        return attachment_request(
            "attach", event_id, expected_revision=expected_revision, expected_digest=expected_digest, manifest=manifest
        )

    def attach(
        self,
        event_id: str,
        name: str,
        data: bytes,
        media_type: str,
        *,
        expected_revision: int,
        expected_digest: str,
        command_id: str,
    ) -> AnnotationRevision:
        request = self._attach_request(event_id, name, data, media_type, expected_revision, expected_digest)
        return self._publish(command_id, request, {request["manifest"]["sha256"]: data})

    def preview_attach(
        self,
        event_id: str,
        name: str,
        data: bytes,
        media_type: str,
        *,
        expected_revision: int,
        expected_digest: str,
        command_id: str,
    ) -> AnnotationRevision:
        request = self._attach_request(event_id, name, data, media_type, expected_revision, expected_digest)
        return self._preview(command_id, request)

    def detach(
        self, event_id: str, name: str, *, expected_revision: int, expected_digest: str, command_id: str
    ) -> AnnotationRevision:
        request = attachment_request(
            "detach", event_id, expected_revision=expected_revision, expected_digest=expected_digest, name=name
        )
        return self._publish(command_id, request, {})

    def preview_detach(
        self, event_id: str, name: str, *, expected_revision: int, expected_digest: str, command_id: str
    ) -> AnnotationRevision:
        request = attachment_request(
            "detach", event_id, expected_revision=expected_revision, expected_digest=expected_digest, name=name
        )
        return self._preview(command_id, request)

    def list(self, event_id: str, *, revision: int) -> tuple[AttachmentManifest, ...]:
        integer(revision, "pinned revision", 1, 2**63 - 2)
        result = self.store.get(event_id, revision)
        return manifests(result.event.attachments, self.limits)

    def read(self, event_id: str, name: str, *, revision: int) -> bytes:
        _name(event_id, "event ID")
        logical_name(name)
        integer(revision, "pinned revision", 1, 2**63 - 2)
        with self.store._transaction():
            event = self.store._get(event_id, revision).event
            for item in manifests(event.attachments, self.limits):
                if item.name == name:
                    return self.store._attachment_blob(item)
            raise KeyError(name)
