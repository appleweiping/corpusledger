"""Strict typed client for the explicitly configured local annotation service."""

from __future__ import annotations

import hashlib
import http.client
import itertools
import socket
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ._annotation_journal import validate_transition
from .annotation_attachment_snapshot import AnnotationAttachmentSnapshot
from .annotation_attachments import attachment_request
from .annotation_execution import AnnotationOperation
from .annotation_protocol import MAX_WIRE_BYTES, decode_wire, encode_wire, identifier, sha256_text
from .annotation_remote import _response_length, _token, loopback_endpoint
from .annotation_service import (
    ATTACHMENT_DATA_FORMAT,
    ATTACHMENT_LIST_FORMAT,
    COMMAND_FORMAT,
    ERROR_STATUS,
    RESPONSE_FORMAT,
    _credential_name,
    _decode_attachment_data,
    _encode_attachment_data,
    _timeout,
)
from .annotation_store import (
    STORE_FORMAT,
    STORE_FORMAT_V2,
    AnnotationEvent,
    AnnotationRevision,
    AnnotationRevisionInfo,
    _limit,
    _revision,
)
from .annotations import _freeze, _name, _object, _thaw
from .attachment_types import AttachmentManifest, command_id, integer, json_digest, logical_name, manifests, sha256
from .errors import InputError


class AnnotationClientError(InputError):
    """Stable, content-free error code from a local service or transport."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"annotation service: {code}")


def _info(value: Any) -> AnnotationRevisionInfo:
    data = _object(value, {"event_id", "revision", "digest", "parent_digest", "documents"}, "revision info")
    _name(data["event_id"], "event ID")
    _revision(data["revision"])
    sha256_text(data["digest"], "revision digest")
    if data["revision"] == 1:
        if data["parent_digest"] is not None:
            raise InputError("initial revision cannot have a parent")
    else:
        sha256_text(data["parent_digest"], "parent digest")
    if not isinstance(data["documents"], list) or len(data["documents"]) > 10000:
        raise InputError("invalid revision document IDs")
    for name in data["documents"]:
        _name(name, "document ID")
    if data["documents"] != sorted(set(data["documents"])):
        raise InputError("document IDs must be unique and sorted")
    return AnnotationRevisionInfo(**{**data, "documents": tuple(data["documents"])})


def _snapshot(value: Any) -> AnnotationRevision:
    data = _object(
        value,
        {"format", "event_id", "revision", "digest", "parent_digest", "documents", "event", "provenance"},
        "revision",
    )
    if data["format"] != "corpusledger.annotation-revision.v1":
        raise InputError("unsupported revision format")
    info = _info({key: data[key] for key in ("event_id", "revision", "digest", "parent_digest", "documents")})
    event = AnnotationEvent.from_dict(data["event"])
    if event.event_id != info.event_id or tuple(document.document_id for document in event.documents) != info.documents:
        raise InputError("revision event identity mismatch")
    if not isinstance(data["provenance"], dict):
        raise InputError("revision provenance must be an object")
    provenance = _freeze(data["provenance"])
    descriptor = {
        "format": STORE_FORMAT if event.version == 1 else STORE_FORMAT_V2,
        "event_id": event.event_id,
        "metadata": _thaw(event.metadata),
        "documents": [{"id": doc.document_id, "digest": doc.digest} for doc in event.documents],
        "parent_digest": info.parent_digest,
        "provenance": _thaw(provenance),
        "revision": info.revision,
    }
    if event.version == 2:
        descriptor["attachments"] = [item.to_dict() for item in event.attachments]
    if hashlib.sha256(encode_wire(descriptor)).hexdigest() != info.digest:
        raise InputError("revision descriptor digest mismatch")
    return AnnotationRevision(
        info.event_id, info.revision, info.digest, info.parent_digest, info.documents, event, provenance
    )


@dataclass(frozen=True, slots=True)
class AnnotationClient:
    """No redirects, DNS hosts or environment proxies; credentials rotate by env.

    Timeout covers socket connect/send/receive, not local JSON validation. Errors
    are redacted. A transport failure cannot establish whether a write completed;
    inspect the event or durable operation ID before issuing a new operation.
    """

    endpoint: str
    token_env: str
    timeout: float = 30

    def __post_init__(self) -> None:
        origin, _, _ = loopback_endpoint(self.endpoint)
        _credential_name(self.token_env)
        object.__setattr__(self, "endpoint", origin)
        object.__setattr__(self, "timeout", _timeout(self.timeout))

    def _exchange(self, command: str, arguments: dict[str, Any]) -> Any:
        body = encode_wire({"format": COMMAND_FORMAT, "command": command, "arguments": arguments})
        _, host, port = loopback_endpoint(self.endpoint)
        token = _token(self.token_env)
        connection = http.client.HTTPConnection(host, port, timeout=self.timeout)
        active: socket.socket | None = None
        timer: threading.Timer | None = None
        started = time.monotonic()
        try:
            connection.connect()
            active = connection.sock
            remaining = self.timeout - (time.monotonic() - started)
            if active is None or remaining <= 0:
                raise AnnotationClientError("transport_failed")
            connected = active

            def abort() -> None:
                with suppress(OSError):
                    connected.shutdown(socket.SHUT_RDWR)
                connected.close()

            timer = threading.Timer(remaining, abort)
            timer.name = "corpusledger-event-client-deadline"
            timer.daemon = True
            timer.start()
            connection.request(
                "POST",
                "/v1/events",
                body,
                {
                    "Host": self.endpoint.removeprefix("http://"),
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            with connection.getresponse() as response:
                status = response.status
                count = _response_length(response, MAX_WIRE_BYTES)
                received = bytearray()
                while chunk := response.read1(min(65536, MAX_WIRE_BYTES + 1 - len(received))):
                    received.extend(chunk)
                    if len(received) > MAX_WIRE_BYTES:
                        raise AnnotationClientError("response_invalid")
                if len(received) != count or time.monotonic() - started > self.timeout:
                    raise AnnotationClientError("transport_failed")
            payload = decode_wire(bytes(received))
            if not isinstance(payload, dict) or type(payload.get("ok")) is not bool:
                raise AnnotationClientError("response_invalid")
            fields = {"format", "command", "ok", "result" if payload["ok"] else "error"}
            envelope = _object(payload, fields, "event response")
            if envelope["format"] != RESPONSE_FORMAT:
                raise AnnotationClientError("response_invalid")
            if envelope["ok"]:
                if status != 200 or envelope["command"] != command:
                    raise AnnotationClientError("response_invalid")
                return envelope["result"]
            error = _object(envelope["error"], {"code"}, "service error")
            code = error["code"]
            if (
                not isinstance(code, str)
                or ERROR_STATUS.get(code) != status
                or envelope["command"] not in (None, command)
            ):
                raise AnnotationClientError("response_invalid")
            raise AnnotationClientError(code)
        except (OSError, http.client.HTTPException):
            raise AnnotationClientError("transport_failed") from None
        finally:
            if timer is not None:
                timer.cancel()
                timer.join(timeout=1)
            connection.close()
            if active is not None:
                active.close()

    def _request(self, command: str, arguments: dict[str, Any]) -> Any:
        try:
            self._validate_arguments(arguments, command=command)
            return self._exchange(command, arguments)
        except AnnotationClientError:
            raise
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("request_or_response_invalid") from None

    @staticmethod
    def _validate_arguments(arguments: dict[str, Any], *, command: str = "") -> None:
        for key in ("event_id", "after_event_id"):
            if key in arguments and (key == "event_id" or arguments[key] is not None):
                _name(arguments[key], "event ID")
        for key in ("operation_id", "after_operation_id"):
            if key in arguments and (key == "operation_id" or arguments[key] is not None):
                identifier(arguments[key], "operation ID")
        for key in ("revision", "expected_revision", "after_revision", "after_version"):
            if key in arguments and (key != "revision" or arguments[key] is not None):
                _revision(
                    arguments[key],
                    zero=key.startswith("after_")
                    or (command == "attachment_snapshot_import" and key == "expected_revision"),
                )
        if "limit" in arguments:
            _limit(arguments["limit"])
        if "expected_digest" in arguments and not (
            command == "attachment_snapshot_import"
            and arguments["expected_revision"] == 0
            and arguments["expected_digest"] is None
        ):
            sha256_text(arguments["expected_digest"], "source revision digest")
        if "retry_uncertain" in arguments and type(arguments["retry_uncertain"]) is not bool:
            raise InputError("retry_uncertain must be a boolean")
        if "pipelines" in arguments:
            selection = arguments["pipelines"]
            if not isinstance(selection, dict) or not 1 <= len(selection) <= 128:
                raise InputError("invalid pipeline selection")
            for document_id, pipeline_id in selection.items():
                _name(document_id, "document ID")
                identifier(pipeline_id, "pipeline ID")

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
        try:
            encoded = _encode_attachment_data(data)
            item = AttachmentManifest(name, hashlib.sha256(data).hexdigest(), len(data), media_type)
            request = attachment_request(
                "attach", event_id, expected_revision=expected_revision, expected_digest=expected_digest, manifest=item
            )
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("request_invalid") from None
        result = self._attachment_mutation(
            "attachment_attach",
            {
                "event_id": event_id,
                "name": name,
                "data": encoded,
                "media_type": media_type,
                "expected_revision": expected_revision,
                "expected_digest": expected_digest,
                "command_id": command_id,
            },
            request,
        )
        if item not in result.event.attachments or result.event.version != 2:
            raise AnnotationClientError("response_invalid")
        return result

    def detach(
        self,
        event_id: str,
        name: str,
        *,
        expected_revision: int,
        expected_digest: str,
        command_id: str,
    ) -> AnnotationRevision:
        try:
            request = attachment_request(
                "detach", event_id, expected_revision=expected_revision, expected_digest=expected_digest, name=name
            )
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("request_invalid") from None
        result = self._attachment_mutation(
            "attachment_detach",
            {
                "event_id": event_id,
                "name": name,
                "expected_revision": expected_revision,
                "expected_digest": expected_digest,
                "command_id": command_id,
            },
            request,
        )
        if any(item.name == name for item in result.event.attachments) or result.event.version != 2:
            raise AnnotationClientError("response_invalid")
        return result

    def _attachment_mutation(
        self, command: str, arguments: dict[str, Any], request: dict[str, Any]
    ) -> AnnotationRevision:
        try:
            command_id(arguments["command_id"])
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("request_invalid") from None
        result = self._revision_result(command, arguments)
        provenance = {
            "annotation_attachment": {
                "command_id": arguments["command_id"],
                "request_digest": json_digest(request),
                "request": request,
            }
        }
        try:
            if (
                result.event_id != request["event_id"]
                or result.revision != request["expected_revision"] + 1
                or result.parent_digest != request["expected_digest"]
                or json_digest(result.provenance) != json_digest(provenance)
            ):
                raise InputError("attachment result command binding mismatch")
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("response_invalid") from None
        return result

    def _attachment_read(
        self, command: str, event_id: str, revision: int, expected_digest: str, *, name: str | None = None
    ) -> Any:
        try:
            integer(revision, "pinned revision", 1, 2**63 - 2)
            sha256(expected_digest)
            if command == "attachment_get":
                logical_name(name)
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("request_invalid") from None
        arguments = {"event_id": event_id, "revision": revision, "expected_digest": expected_digest}
        if command == "attachment_get":
            arguments["name"] = name
        return self._request(command, arguments)

    @staticmethod
    def _attachment_pin(
        value: Any, fields: set[str], expected_format: str, event_id: str, revision: int, digest: str
    ) -> dict[str, Any]:
        data = _object(value, {"format", "event_id", "revision", "digest"} | fields, "attachment response")
        if (
            data["format"] != expected_format
            or data["event_id"] != event_id
            or type(data["revision"]) is not int
            or data["revision"] != revision
            or data["digest"] != digest
        ):
            raise InputError("attachment response pin mismatch")
        return dict(data)

    def read_attachment(self, event_id: str, name: str, *, revision: int, expected_digest: str) -> bytes:
        value = self._attachment_read("attachment_get", event_id, revision, expected_digest, name=name)
        try:
            result = self._attachment_pin(
                value, {"attachment", "data"}, ATTACHMENT_DATA_FORMAT, event_id, revision, expected_digest
            )
            item = AttachmentManifest.from_dict(result["attachment"])
            data = _decode_attachment_data(result["data"], maximum=item.size)
            if item.name != name or len(data) != item.size or hashlib.sha256(data).hexdigest() != item.sha256:
                raise InputError("attachment manifest does not match requested data")
            return data
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def list_attachments(self, event_id: str, *, revision: int, expected_digest: str) -> tuple[AttachmentManifest, ...]:
        value = self._attachment_read("attachment_list", event_id, revision, expected_digest)
        try:
            result = self._attachment_pin(
                value, {"attachments"}, ATTACHMENT_LIST_FORMAT, event_id, revision, expected_digest
            )
            raw = result["attachments"]
            if not isinstance(raw, list) or len(raw) > 64:
                raise InputError("invalid attachment inventory")
            items = manifests(tuple(AttachmentManifest.from_dict(item) for item in raw))
            if [item.to_dict() for item in items] != raw:
                raise InputError("attachment inventory must be canonical")
            return items
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def export_attachment_snapshot(
        self, event_id: str, *, revision: int, expected_digest: str
    ) -> AnnotationAttachmentSnapshot:
        value = self._attachment_read("attachment_snapshot_export", event_id, revision, expected_digest)
        try:
            snapshot = AnnotationAttachmentSnapshot.from_dict(value)
            if snapshot.source != {"event_id": event_id, "revision": revision, "digest": expected_digest}:
                raise InputError("snapshot source pin mismatch")
            return snapshot
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def import_attachment_snapshot(
        self,
        snapshot: AnnotationAttachmentSnapshot,
        *,
        command_id: str,
        expected_snapshot_digest: str,
        expected_revision: int = 0,
        expected_digest: str | None = None,
    ) -> AnnotationRevision:
        try:
            if not isinstance(snapshot, AnnotationAttachmentSnapshot):
                raise InputError("snapshot must be a typed attachment snapshot")
            sha256(expected_snapshot_digest)
            if snapshot.digest != expected_snapshot_digest:
                raise InputError("snapshot does not match external pin")
            request = attachment_request(
                "import",
                snapshot.event.event_id,
                expected_revision=expected_revision,
                expected_digest=expected_digest,
                snapshot_digest=snapshot.digest,
                event_digest=snapshot.event.digest,
                source=snapshot.source,
            )
            serialized = snapshot.to_dict()
        except (InputError, ValueError, TypeError):
            raise AnnotationClientError("request_invalid") from None
        result = self._attachment_mutation(
            "attachment_snapshot_import",
            {
                "snapshot": serialized,
                "command_id": command_id,
                "expected_snapshot_digest": expected_snapshot_digest,
                "expected_revision": expected_revision,
                "expected_digest": expected_digest,
            },
            request,
        )
        if result.event.digest != snapshot.event.digest:
            raise AnnotationClientError("response_invalid")
        return result

    def create(self, event: AnnotationEvent) -> AnnotationRevision:
        if not isinstance(event, AnnotationEvent):
            raise AnnotationClientError("request_invalid")
        result = self._revision_result("create", {"event": event.to_dict()})
        if result.event.digest != event.digest or result.revision != 1:
            raise AnnotationClientError("response_invalid")
        return result

    def get(self, event_id: str, revision: int | None = None) -> AnnotationRevision:
        result = self._revision_result("get", {"event_id": event_id, "revision": revision})
        if result.event_id != event_id or (revision is not None and result.revision != revision):
            raise AnnotationClientError("response_invalid")
        return result

    def _revision_result(self, command: str, arguments: dict[str, Any]) -> AnnotationRevision:
        value = self._request(command, arguments)
        try:
            return _snapshot(value)
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def list(self, *, after_event_id: str | None = None, limit: int = 100) -> tuple[AnnotationRevisionInfo, ...]:
        result = self._infos("list", {"after_event_id": after_event_id, "limit": limit})
        ids = [item.event_id for item in result]
        if ids != sorted(set(ids)) or (after_event_id is not None and any(name <= after_event_id for name in ids)):
            raise AnnotationClientError("response_invalid")
        return result

    def history(
        self, event_id: str, *, after_revision: int = 0, limit: int = 100
    ) -> tuple[AnnotationRevisionInfo, ...]:
        result = self._infos("history", {"event_id": event_id, "after_revision": after_revision, "limit": limit})
        if any(
            item.event_id != event_id or item.revision != after_revision + index + 1
            for index, item in enumerate(result)
        ):
            raise AnnotationClientError("response_invalid")
        if any(current.parent_digest != previous.digest for previous, current in itertools.pairwise(result)):
            raise AnnotationClientError("response_invalid")
        return result

    def _infos(self, command: str, arguments: dict[str, Any]) -> tuple[AnnotationRevisionInfo, ...]:
        value = self._request(command, arguments)
        try:
            _limit(arguments["limit"])
            if not isinstance(value, list) or len(value) > arguments["limit"]:
                raise InputError("invalid revision page")
            return tuple(_info(item) for item in value)
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def begin(
        self,
        operation_id: str,
        event_id: str,
        pipelines: Mapping[str, str],
        *,
        expected_revision: int,
        expected_digest: str,
    ) -> AnnotationOperation:
        if not isinstance(pipelines, Mapping):
            raise AnnotationClientError("request_invalid")
        arguments = {
            "operation_id": operation_id,
            "event_id": event_id,
            "pipelines": dict(pipelines),
            "expected_revision": expected_revision,
            "expected_digest": expected_digest,
        }
        result = self._operation("begin", arguments)
        binding = result.to_dict()["request"]
        if any(binding[key] != arguments[key] for key in ("event_id", "expected_revision", "expected_digest")):
            raise AnnotationClientError("response_invalid")
        self._selection_matches(result, pipelines)
        return result

    def resume(
        self, operation_id: str, pipelines: Mapping[str, str], *, retry_uncertain: bool = False
    ) -> AnnotationOperation:
        if not isinstance(pipelines, Mapping):
            raise AnnotationClientError("request_invalid")
        result = self._operation(
            "resume", {"operation_id": operation_id, "pipelines": dict(pipelines), "retry_uncertain": retry_uncertain}
        )
        self._selection_matches(result, pipelines)
        return result

    @staticmethod
    def _selection_matches(operation: AnnotationOperation, pipelines: Mapping[str, str]) -> None:
        steps = operation.to_dict()["request"]["steps"]
        if {step["document_id"] for step in steps} != set(pipelines) or any(
            pipelines[step["document_id"]] != step["pipeline_id"] for step in steps
        ):
            raise AnnotationClientError("response_invalid")

    def status(self, operation_id: str) -> AnnotationOperation:
        return self._operation("status", {"operation_id": operation_id})

    def _operation(self, command: str, arguments: dict[str, Any]) -> AnnotationOperation:
        value = self._request(command, arguments)
        try:
            result = AnnotationOperation(value)
            if result.operation_id != arguments["operation_id"]:
                raise InputError("operation identity mismatch")
            return result
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None

    def operations(self, *, after_operation_id: str | None = None, limit: int = 100) -> tuple[AnnotationOperation, ...]:
        results = self._operation_page("operations", {"after_operation_id": after_operation_id, "limit": limit})
        ids = [item.operation_id for item in results]
        if ids != sorted(set(ids)) or (
            after_operation_id is not None and any(name <= after_operation_id for name in ids)
        ):
            raise AnnotationClientError("response_invalid")
        return results

    def operation_history(
        self, operation_id: str, *, after_version: int = 0, limit: int = 100
    ) -> tuple[AnnotationOperation, ...]:
        results = self._operation_page(
            "operation_history", {"operation_id": operation_id, "after_version": after_version, "limit": limit}
        )
        if any(
            item.operation_id != operation_id or item.version != after_version + index + 1
            for index, item in enumerate(results)
        ):
            raise AnnotationClientError("response_invalid")
        try:
            if results and after_version == 0:
                validate_transition(None, results[0].to_dict())
            for previous, current in itertools.pairwise(results):
                validate_transition(previous.to_dict(), current.to_dict())
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None
        return results

    def _operation_page(self, command: str, arguments: dict[str, Any]) -> tuple[AnnotationOperation, ...]:
        value = self._request(command, arguments)
        try:
            _limit(arguments["limit"])
            if not isinstance(value, list) or len(value) > arguments["limit"]:
                raise InputError("invalid operation page")
            return tuple(AnnotationOperation(item) for item in value)
        except (InputError, ValueError, TypeError, KeyError):
            raise AnnotationClientError("response_invalid") from None
