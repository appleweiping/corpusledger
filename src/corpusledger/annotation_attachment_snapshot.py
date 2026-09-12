"""One-version attachment snapshots, not authenticated history backups."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .annotation_attachments import AnnotationAttachments, attachment_request
from .annotation_store import AnnotationEvent, AnnotationRevision, AnnotationStore
from .annotations import (
    ANNOTATION_FORMAT,
    AnnotationDocument,
    AnnotationField,
    AnnotationType,
    SpanAnnotation,
    _object,
)
from .attachment_types import (
    MAX_BLOB_BYTES,
    MAX_SNAPSHOT_BYTES,
    AttachmentError,
    AttachmentLimits,
    AttachmentManifest,
    AttachmentQuotaError,
    bounded_json,
    integer,
    manifests,
    sha256,
)

SNAPSHOT_FORMAT = "corpusledger.annotation-attachment-snapshot.v1"


def _adapt(value: Any) -> Any:
    # Keep nested document/annotation tuples lazy: never call a whole event's
    # to_dict before its much smaller snapshot envelope has been admitted.
    if isinstance(value, AnnotationEvent):
        return {
            "format": f"corpusledger.annotation-event.v{value.version}",
            "id": value.event_id,
            "documents": value.documents,
            "metadata": value.metadata,
            **({"attachments": value.attachments} if value.version == 2 else {}),
        }
    if isinstance(value, AnnotationDocument):
        if len(value.text) > MAX_SNAPSHOT_BYTES:
            raise AttachmentQuotaError("snapshot document exceeds complete byte limit")
        digest = hashlib.sha256()
        size = 0
        for offset in range(0, len(value.text), 4096):
            chunk = value.text[offset : offset + 4096].encode("utf-8")
            size += len(chunk)
            if size > MAX_SNAPSHOT_BYTES:
                raise AttachmentQuotaError("snapshot document exceeds complete byte limit")
            digest.update(chunk)
        return {
            "format": ANNOTATION_FORMAT,
            "id": value.document_id,
            "text": value.text,
            "text_sha256": digest.hexdigest(),
            "offset_unit": "unicode_codepoint",
            "types": value.annotation_types,
            "annotations": value.annotations,
        }
    if isinstance(value, AnnotationType):
        return {"name": value.name, "fields": value.fields}
    if isinstance(value, AnnotationField):
        return value.to_dict()
    if isinstance(value, SpanAnnotation):
        return {
            "id": value.annotation_id,
            "type": value.type_name,
            "start": value.start,
            "end": value.end,
            "features": value.features,
        }
    if isinstance(value, AttachmentManifest):
        return value.to_dict()
    return value


@dataclass(frozen=True, slots=True)
class AttachmentBlob:
    sha256: str
    data: bytes

    def __post_init__(self) -> None:
        sha256(self.sha256)
        if type(self.data) is not bytes or len(self.data) > MAX_BLOB_BYTES:
            raise AttachmentError("snapshot BLOB must be immutable bytes within the hard size limit")
        if hashlib.sha256(self.data).hexdigest() != self.sha256:
            raise AttachmentError("snapshot BLOB SHA-256 does not match")

    @property
    def size(self) -> int:
        return len(self.data)

    def to_dict(self) -> dict[str, Any]:
        return {"sha256": self.sha256, "size": self.size, "base64": base64.b64encode(self.data).decode("ascii")}

    @classmethod
    def from_dict(cls, value: Any) -> AttachmentBlob:
        data = _object(value, {"sha256", "size", "base64"}, "snapshot BLOB")
        sha256(data["sha256"])
        size = integer(data["size"], "snapshot BLOB size", 0, MAX_BLOB_BYTES)
        encoded = data["base64"]
        if not isinstance(encoded, str) or len(encoded) != 4 * ((size + 2) // 3):
            raise AttachmentError("snapshot Base64 length does not match declared size")
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as error:
            raise AttachmentError("snapshot BLOB must use canonical Base64") from error
        if len(decoded) != size or base64.b64encode(decoded).decode("ascii") != encoded:
            raise AttachmentError("snapshot BLOB must use canonical Base64")
        return cls(data["sha256"], decoded)


@dataclass(frozen=True, slots=True)
class AnnotationAttachmentSnapshot:
    event: AnnotationEvent
    source_revision: int
    source_digest: str
    blobs: tuple[AttachmentBlob, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.event, AnnotationEvent):
            raise AttachmentError("snapshot event must be AnnotationEvent")
        integer(self.source_revision, "snapshot source revision", 1, 2**63 - 2)
        sha256(self.source_digest)
        if not isinstance(self.blobs, (tuple, list)) or len(self.blobs) > 64:
            raise AttachmentError("snapshot BLOB inventory must be bounded")
        if any(not isinstance(blob, AttachmentBlob) for blob in self.blobs):
            raise AttachmentError("snapshot BLOB entries must be AttachmentBlob")
        if len({blob.sha256 for blob in self.blobs}) != len(self.blobs):
            raise AttachmentError("snapshot BLOB inventory contains duplicates")
        object.__setattr__(self, "blobs", tuple(sorted(self.blobs, key=lambda item: item.sha256)))
        expected = {item.sha256: item.size for item in self.event.attachments}
        if {blob.sha256: blob.size for blob in self.blobs} != expected:
            raise AttachmentError("snapshot BLOB inventory has missing, extra, or mismatched entries")
        # Validate full envelope even for directly constructed typed snapshots.
        self.to_bytes()

    @property
    def source(self) -> dict[str, Any]:
        return {"event_id": self.event.event_id, "revision": self.source_revision, "digest": self.source_digest}

    def validate_limits(self, limits: AttachmentLimits | None = None) -> None:
        policy = limits if limits is not None else AttachmentLimits()
        manifests(self.event.attachments, policy)
        if len(self.blobs) > policy.max_store_blobs or sum(blob.size for blob in self.blobs) > policy.max_store_bytes:
            raise AttachmentQuotaError("snapshot exceeds configured unique BLOB limits")

    def _body(self) -> dict[str, Any]:
        # Base64 has a known expansion; pre-admit it before building the strings.
        if sum(4 * ((blob.size + 2) // 3) for blob in self.blobs) > MAX_SNAPSHOT_BYTES:
            raise AttachmentQuotaError("snapshot Base64 exceeds complete byte limit")
        return {
            "format": SNAPSHOT_FORMAT,
            "source": self.source,
            "event": self.event,
            "blobs": [blob.to_dict() for blob in self.blobs],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(bounded_json(self._body(), MAX_SNAPSHOT_BYTES, adapt=_adapt)).hexdigest()

    def to_bytes(self) -> bytes:
        body = self._body()
        digest = hashlib.sha256(bounded_json(body, MAX_SNAPSHOT_BYTES, adapt=_adapt)).hexdigest()
        return bounded_json({**body, "sha256": digest}, MAX_SNAPSHOT_BYTES, adapt=_adapt)

    def to_dict(self) -> dict[str, Any]:
        return dict(json.loads(self.to_bytes()))

    @classmethod
    def from_dict(cls, value: Any) -> AnnotationAttachmentSnapshot:
        bounded_json(value, MAX_SNAPSHOT_BYTES)
        data = _object(value, {"format", "source", "event", "blobs", "sha256"}, "attachment snapshot")
        if data["format"] != SNAPSHOT_FORMAT:
            raise AttachmentError("unsupported attachment snapshot format")
        sha256(data["sha256"])
        body = {key: item for key, item in data.items() if key != "sha256"}
        if hashlib.sha256(bounded_json(body, MAX_SNAPSHOT_BYTES)).hexdigest() != data["sha256"]:
            raise AttachmentError("snapshot envelope digest does not match")
        source = _object(data["source"], {"event_id", "revision", "digest"}, "snapshot source")
        event = AnnotationEvent.from_dict(data["event"])
        if source["event_id"] != event.event_id:
            raise AttachmentError("snapshot source and event IDs do not match")
        if not isinstance(data["blobs"], list) or len(data["blobs"]) > 64:
            raise AttachmentError("snapshot BLOB array exceeds its limit")
        result = cls(
            event, source["revision"], source["digest"], tuple(AttachmentBlob.from_dict(item) for item in data["blobs"])
        )
        if result.digest != data["sha256"]:
            raise AttachmentError("snapshot must use sorted canonical inventories")
        return result


def export_snapshot(
    store: AnnotationStore, event_id: str, revision: int, *, limits: AttachmentLimits | None = None
) -> AnnotationAttachmentSnapshot:
    manager = AnnotationAttachments(store, limits)
    integer(revision, "pinned revision", 1, 2**63 - 2)
    with store._transaction():
        selected = store._get(event_id, revision)
        manifest = manifests(selected.event.attachments, manager.limits)
        # Bound document serialization before loading/encoding all BLOB payloads.
        bounded_json(selected.event, MAX_SNAPSHOT_BYTES, adapt=_adapt)
        blobs = {item.sha256: AttachmentBlob(item.sha256, store._attachment_blob(item)) for item in manifest}
        result = AnnotationAttachmentSnapshot(selected.event, selected.revision, selected.digest, tuple(blobs.values()))
        result.validate_limits(manager.limits)
        return result


def _import_request(
    snapshot: AnnotationAttachmentSnapshot, expected_revision: int, expected_digest: str | None
) -> dict[str, Any]:
    if not isinstance(snapshot, AnnotationAttachmentSnapshot):
        raise AttachmentError("snapshot must be AnnotationAttachmentSnapshot")
    return attachment_request(
        "import",
        snapshot.event.event_id,
        expected_revision=expected_revision,
        expected_digest=expected_digest,
        snapshot_digest=snapshot.digest,
        event_digest=snapshot.event.digest,
        source=snapshot.source,
    )


def preview_import_snapshot(
    store: AnnotationStore,
    snapshot: AnnotationAttachmentSnapshot,
    *,
    command_id: str,
    expected_revision: int = 0,
    expected_digest: str | None = None,
    limits: AttachmentLimits | None = None,
) -> AnnotationRevision:
    manager = AnnotationAttachments(store, limits)
    request = _import_request(snapshot, expected_revision, expected_digest)
    snapshot.validate_limits(manager.limits)
    return manager._preview(command_id, request, snapshot.event)


def import_snapshot(
    store: AnnotationStore,
    snapshot: AnnotationAttachmentSnapshot,
    *,
    command_id: str,
    expected_revision: int = 0,
    expected_digest: str | None = None,
    limits: AttachmentLimits | None = None,
) -> AnnotationRevision:
    manager = AnnotationAttachments(store, limits)
    request = _import_request(snapshot, expected_revision, expected_digest)
    snapshot.validate_limits(manager.limits)
    return manager._publish(command_id, request, {blob.sha256: blob.data for blob in snapshot.blobs}, snapshot.event)
