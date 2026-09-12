"""Explicitly configured, authenticated loopback annotation event service.

This is a local integration boundary, not an internet-facing application server.
Socket deadlines do not cancel an already accepted durable execution.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hmac
import ipaddress
import re
import socket
import socketserver
import sqlite3
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ._annotation_journal import (
    AnnotationExecutionConflict,
    AnnotationExecutionUncertain,
)
from .annotation_attachment_snapshot import (
    AnnotationAttachmentSnapshot,
    export_snapshot,
    import_snapshot,
    preview_import_snapshot,
)
from .annotation_attachments import AnnotationAttachments, AttachmentConflictError
from .annotation_execution import AnnotationExecutor, RemoteAnnotationPipeline
from .annotation_pipeline import AnnotationPipelineError
from .annotation_protocol import MAX_WIRE_BYTES, ProcessorDescription, decode_wire, encode_wire, identifier
from .annotation_remote import RemoteAnnotationError, RemoteAnnotationProcessor, _token
from .annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationRevision,
    AnnotationStore,
    AnnotationStoreError,
)
from .annotations import _name, _object
from .attachment_types import MAX_BLOB_BYTES, AttachmentLimits, AttachmentQuotaError, integer, sha256
from .errors import InputError

COMMAND_FORMAT = "corpusledger.event-command.v1"
RESPONSE_FORMAT = "corpusledger.event-response.v1"
CONFIG_FORMAT = "corpusledger.annotation-service-config.v1"
ERROR_STATUS = {
    "request_invalid": 400,
    "unauthorized": 401,
    "not_found": 404,
    "conflict": 409,
    "uncertain": 409,
    "execution_disabled": 409,
    "attachments_disabled": 409,
    "too_large": 413,
    "unavailable": 503,
    "internal_error": 500,
}
_ARGUMENTS = {
    "create": {"event"},
    "get": {"event_id", "revision"},
    "list": {"after_event_id", "limit"},
    "history": {"event_id", "after_revision", "limit"},
    "begin": {"operation_id", "event_id", "pipelines", "expected_revision", "expected_digest"},
    "resume": {"operation_id", "pipelines", "retry_uncertain"},
    "status": {"operation_id"},
    "operations": {"after_operation_id", "limit"},
    "operation_history": {"operation_id", "after_version", "limit"},
    "attachment_attach": {
        "event_id",
        "name",
        "data",
        "media_type",
        "expected_revision",
        "expected_digest",
        "command_id",
    },
    "attachment_detach": {"event_id", "name", "expected_revision", "expected_digest", "command_id"},
    "attachment_get": {"event_id", "name", "revision", "expected_digest"},
    "attachment_list": {"event_id", "revision", "expected_digest"},
    "attachment_snapshot_export": {"event_id", "revision", "expected_digest"},
    "attachment_snapshot_import": {
        "snapshot",
        "command_id",
        "expected_revision",
        "expected_digest",
        "expected_snapshot_digest",
    },
}
ATTACHMENT_DATA_FORMAT = "corpusledger.attachment-data.v1"
ATTACHMENT_LIST_FORMAT = "corpusledger.attachment-list.v1"


def _decode_attachment_data(value: Any, *, maximum: int = MAX_BLOB_BYTES) -> bytes:
    """Admit the encoded payload before allocating decoded, opaque bytes."""
    if not isinstance(value, str):
        raise InputError("attachment data must be base64 text")
    if len(value) > 4 * ((maximum + 2) // 3):
        raise AttachmentQuotaError("attachment data exceeds its encoded limit")
    try:
        data = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        raise InputError("attachment data must be canonical standard base64") from None
    if len(data) > maximum:
        raise AttachmentQuotaError("attachment data exceeds its decoded byte limit")
    if base64.b64encode(data).decode("ascii") != value:
        raise InputError("attachment data must use canonical base64")
    return data


def _encode_attachment_data(data: bytes) -> str:
    if type(data) is not bytes or len(data) > MAX_BLOB_BYTES:
        raise InputError("attachment data must be bounded bytes")
    return base64.b64encode(data).decode("ascii")


class AnnotationServiceError(InputError):
    """Invalid local service configuration; errors never contain credentials."""


class _Rejected(Exception):
    def __init__(self, code: str) -> None:
        self.code = code


def _credential_name(value: Any) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None:
        raise AnnotationServiceError("credential source must be an environment variable name")


def _timeout(value: Any) -> float:
    if type(value) not in (int, float) or not 0 < value <= 300:
        raise AnnotationServiceError("timeout must be finite and in (0, 300]")
    return float(value)


def _envelope(command: str | None, *, result: Any = None, error: str | None = None) -> dict[str, Any]:
    base = {"format": RESPONSE_FORMAT, "command": command, "ok": error is None}
    return {**base, "result": result} if error is None else {**base, "error": {"code": error}}


def _close_socket(connection: socket.socket) -> None:
    with suppress(OSError):
        connection.shutdown(socket.SHUT_RDWR)
    connection.close()


class _SocketReader:
    """Bounded buffering with a deadline shared by every read and response write.

    Short socket polls make shutdown reliable on Windows even when closing a
    socket from another thread does not immediately wake its pending receive.
    """

    def __init__(self, connection: socket.socket, server: AnnotationServer) -> None:
        self.connection = connection
        self.server = server
        self.deadline = server._deadlines[connection]
        self.buffer = bytearray()

    def check(self) -> None:
        remaining = self.deadline - time.monotonic()
        if self.server._closing or remaining <= 0:
            raise OSError("transport closed")
        self.connection.settimeout(min(0.05, remaining))

    def _receive(self, count: int) -> bytes:
        while True:
            self.check()
            try:
                return self.connection.recv(count)
            except TimeoutError:
                continue

    def readline(self, limit: int) -> bytes:
        while True:
            end = self.buffer.find(b"\n", 0, limit)
            count = end + 1 if end >= 0 else min(len(self.buffer), limit)
            if end >= 0 or count == limit:
                result = bytes(self.buffer[:count])
                del self.buffer[:count]
                return result
            chunk = self._receive(limit - len(self.buffer))
            if not chunk:
                result = bytes(self.buffer)
                self.buffer.clear()
                return result
            self.buffer.extend(chunk)

    def read(self, count: int) -> bytes:
        if self.buffer:
            result = bytes(self.buffer[:count])
            del self.buffer[:count]
            return result
        return self._receive(count)

    def send(self, value: bytes) -> None:
        view = memoryview(value)
        while view:
            self.check()
            try:
                sent = self.connection.send(view[:65536])
            except TimeoutError:
                continue
            if not sent:
                raise OSError("transport closed")
            view = view[sent:]


def _request(stream: _SocketReader, authority: str, token_env: str, limit: int) -> Any:
    consumed = 0

    def line() -> bytes:
        nonlocal consumed
        value = stream.readline(8193)
        consumed += len(value)
        if len(value) > 8192 or consumed > 16384 or not value.endswith(b"\r\n"):
            raise _Rejected("request_invalid")
        return value[:-2]

    if line() != b"POST /v1/events HTTP/1.1":
        raise _Rejected("request_invalid")
    headers: dict[str, str] = {}
    for _ in range(33):
        raw = line()
        if not raw:
            break
        try:
            name, value = raw.decode("ascii").split(":", 1)
        except (UnicodeError, ValueError):
            raise _Rejected("request_invalid") from None
        if re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is None:
            raise _Rejected("request_invalid")
        if any((ord(char) < 32 and char != "\t") or ord(char) == 127 for char in value):
            raise _Rejected("request_invalid")
        key = name.lower()
        if key in headers:
            raise _Rejected("request_invalid")
        headers[key] = value.strip(" \t")
    else:
        raise _Rejected("request_invalid")
    if headers.get("host") != authority or any(
        key in headers
        for key in (
            "origin",
            "transfer-encoding",
            "content-encoding",
            "expect",
            "proxy-authorization",
            "proxy-connection",
        )
    ):
        raise _Rejected("request_invalid")
    try:
        expected = f"Bearer {_token(token_env)}"
    except RemoteAnnotationError:
        raise _Rejected("unauthorized") from None
    if not hmac.compare_digest(headers.get("authorization", ""), expected):
        raise _Rejected("unauthorized")
    length = headers.get("content-length", "")
    if (
        re.fullmatch(r"[0-9]{1,10}", length) is None
        or re.fullmatch(
            r"application/json(?:[ \t]*;[ \t]*charset[ \t]*=[ \t]*utf-8)?",
            headers.get("content-type", ""),
            re.IGNORECASE,
        )
        is None
    ):
        raise _Rejected("request_invalid")
    count = int(length)
    if count > limit:
        raise _Rejected("too_large")
    body = bytearray()
    while len(body) < count:
        chunk = stream.read(min(65536, count - len(body)))
        if not chunk:
            raise _Rejected("request_invalid")
        body.extend(chunk)
    return decode_wire(bytes(body), limit=limit)


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        if not isinstance(server, AnnotationServer):  # pragma: no cover - constructor invariant
            return
        transport = _SocketReader(self.request, server)
        command: str | None = None
        try:
            incoming = _request(transport, server.authority, server.token_env, server.max_wire_bytes)
            transport.check()
            envelope = _object(incoming, {"format", "command", "arguments"}, "event command")
            candidate = envelope["command"]
            if envelope["format"] != COMMAND_FORMAT or not isinstance(candidate, str) or candidate not in _ARGUMENTS:
                raise _Rejected("request_invalid")
            command = candidate
            arguments = _object(envelope["arguments"], _ARGUMENTS[command], "command arguments")
            result = server.dispatch(command, dict(arguments))
            try:
                body = encode_wire(_envelope(command, result=result), limit=server.max_wire_bytes)
            except InputError:
                raise _Rejected("too_large") from None
            status = 200
        except _Rejected as exc:
            status = ERROR_STATUS[exc.code]
            body = encode_wire(_envelope(command, error=exc.code))
        except (AnnotationConflictError, AnnotationExecutionConflict, AttachmentConflictError):
            status, body = 409, encode_wire(_envelope(command, error="conflict"))
        except AnnotationExecutionUncertain:
            status, body = 409, encode_wire(_envelope(command, error="uncertain"))
        except KeyError:
            status, body = 404, encode_wire(_envelope(command, error="not_found"))
        except (RemoteAnnotationError, sqlite3.Error):
            status, body = 503, encode_wire(_envelope(command, error="unavailable"))
        except AttachmentQuotaError:
            status, body = 413, encode_wire(_envelope(command, error="too_large"))
        except AnnotationStoreError as exc:
            code = "unavailable" if isinstance(exc.__cause__, (sqlite3.Error, OSError)) else "request_invalid"
            status, body = ERROR_STATUS[code], encode_wire(_envelope(command, error=code))
        except (InputError, AnnotationPipelineError):
            status, body = 400, encode_wire(_envelope(command, error="request_invalid"))
        except OSError:
            return
        except Exception:
            status, body = 500, encode_wire(_envelope(command, error="internal_error"))
        with suppress(OSError):
            transport.send(
                f"HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n\r\n".encode(
                    "ascii"
                )
                + body
            )


class AnnotationServer(socketserver.TCPServer):
    """Bounded request threads; one private SQLite connection per command.

    ``server_close`` closes all sockets and waits up to one second for handlers.
    Already accepted execution may continue in bounded daemon handler threads;
    cancelling sockets is deliberately not represented as cancelling an operation.
    """

    allow_reuse_address = False

    def __init__(
        self,
        store_path: Path,
        pipelines: Mapping[str, RemoteAnnotationPipeline],
        *,
        token_env: str,
        host: str,
        port: int,
        request_timeout: float,
        max_connections: int,
        max_wire_bytes: int,
        attachment_limits: AttachmentLimits,
    ) -> None:
        self.store_path = store_path
        self.pipelines = MappingProxyType(dict(pipelines))
        self.token_env = token_env
        self.request_timeout = request_timeout
        self.max_wire_bytes = max_wire_bytes
        self.attachment_limits = attachment_limits
        self._admission = threading.BoundedSemaphore(max_connections)
        self._active_lock = threading.Lock()
        self._active: dict[socket.socket, threading.Thread] = {}
        self._deadlines: dict[socket.socket, float] = {}
        self._closing = False
        self.address_family = socket.AF_INET6 if ":" in host else socket.AF_INET
        super().__init__((host, port), _Handler)
        address = f"[{host}]" if ":" in host else host
        self.authority = f"{address}:{self.server_address[1]}"
        self.endpoint = f"http://{self.authority}"

    def process_request(self, request: socket.socket | tuple[bytes, socket.socket], client_address: Any) -> None:
        if not isinstance(request, socket.socket):  # pragma: no cover - TCPServer invariant
            return
        if not self._admission.acquire(blocking=False):
            _close_socket(request)
            return
        started = time.monotonic()

        def run() -> None:
            try:
                self.finish_request(request, client_address)
            except Exception:
                # The handler already maps application errors to redacted JSON.
                # Setup/teardown errors are contained by closing the transport;
                # never log request bodies, credentials or raw exceptions.
                _close_socket(request)
            finally:
                self.shutdown_request(request)
                with self._active_lock:
                    self._active.pop(request, None)
                    self._deadlines.pop(request, None)
                self._admission.release()

        thread = threading.Thread(target=run, name="corpusledger-event-request", daemon=True)
        with self._active_lock:
            if self._closing:
                self._admission.release()
                _close_socket(request)
                return
            self._active[request] = thread
            self._deadlines[request] = started + self.request_timeout
            try:
                thread.start()
            except RuntimeError:
                self._active.pop(request)
                self._deadlines.pop(request)
                self._admission.release()
                _close_socket(request)

    def server_close(self) -> None:
        with self._active_lock:
            self._closing = True
            active = tuple(self._active.items())
        super().server_close()
        for connection, _ in active:
            _close_socket(connection)
        deadline = time.monotonic() + 1
        for _, thread in active:
            thread.join(timeout=max(0, deadline - time.monotonic()))

    def _selected(self, value: Any) -> dict[str, RemoteAnnotationPipeline]:
        if not isinstance(value, dict) or not 1 <= len(value) <= 128:
            raise _Rejected("request_invalid")
        result = {}
        for document_id, pipeline_id in value.items():
            _name(document_id, "document ID")
            identifier(pipeline_id, "registered pipeline ID")
            if pipeline_id not in self.pipelines:
                raise _Rejected("request_invalid")
            result[document_id] = self.pipelines[pipeline_id]
        return result

    def dispatch(self, command: str, arguments: dict[str, Any]) -> Any:
        with AnnotationStore(self.store_path, create=False) as store:
            if command.startswith("attachment_"):
                try:
                    return self._attachment_dispatch(store, command, arguments)
                except (AnnotationConflictError, AttachmentConflictError):
                    raise
                except AnnotationStoreError:
                    # The attachment manager uses InputError for invalid input;
                    # stored descriptor/BLOB/receipt failures are unavailable data.
                    raise _Rejected("unavailable") from None
            if command == "create":
                event = AnnotationEvent.from_dict(arguments["event"])
                # Ensure the complete response fits before publishing a create.
                preview = AnnotationRevision(
                    event.event_id, 1, "0" * 64, None, tuple(doc.document_id for doc in event.documents), event, {}
                )
                try:
                    encode_wire(_envelope(command, result=preview.to_dict()), limit=self.max_wire_bytes)
                except InputError:
                    raise _Rejected("too_large") from None
                return store.put(event).to_dict()
            if command == "get":
                return store.get(**arguments).to_dict()
            if command == "list":
                return [item.to_dict() for item in store.list(**arguments)]
            if command == "history":
                return [item.to_dict() for item in store.history(**arguments)]
            if not store.execution_enabled:
                raise _Rejected("execution_disabled")
            executor = AnnotationExecutor(store)
            if command == "status":
                return executor.get(**arguments).to_dict()
            if command == "operations":
                return [item.to_dict() for item in executor.list(**arguments)]
            if command == "operation_history":
                return [item.to_dict() for item in executor.history(**arguments)]
            selected = self._selected(arguments["pipelines"])
            if command == "begin":
                source = store.get(arguments["event_id"], arguments["expected_revision"])
                binding, _ = executor._plan(source, selected)
                self._binding_fits(binding)
                return executor.begin(**{**arguments, "pipelines": selected}).to_dict()
            operation = executor.get(arguments["operation_id"])
            # Unlike the Python executor's committed retrieval shortcut, the
            # HTTP resume command promises the same fixed registry selection.
            state = operation.to_dict()
            source = store.get(state["request"]["event_id"], state["request"]["expected_revision"])
            binding, _ = executor._plan(source, selected)
            if binding != state["request"]:
                raise _Rejected("conflict")
            self._binding_fits(binding)
            return executor.resume(**{**arguments, "pipelines": selected}).to_dict()

    def _attachment_dispatch(self, store: AnnotationStore, command: str, arguments: dict[str, Any]) -> Any:
        if not store.attachments_enabled:
            raise _Rejected("attachments_disabled")
        manager = AnnotationAttachments(store, self.attachment_limits)
        if command in ("attachment_attach", "attachment_detach"):
            fields = dict(arguments)
            if command == "attachment_attach":
                fields["data"] = _decode_attachment_data(fields["data"], maximum=self.attachment_limits.max_blob_bytes)
                preview = manager.preview_attach(**fields)
                self._attachment_fits(command, preview)
                return manager.attach(**fields).to_dict()
            preview = manager.preview_detach(**fields)
            self._attachment_fits(command, preview)
            return manager.detach(**fields).to_dict()
        if command == "attachment_snapshot_import":
            snapshot = AnnotationAttachmentSnapshot.from_dict(arguments["snapshot"])
            sha256(arguments["expected_snapshot_digest"])
            if snapshot.digest != arguments["expected_snapshot_digest"]:
                raise _Rejected("conflict")
            options = {
                "command_id": arguments["command_id"],
                "expected_revision": arguments["expected_revision"],
                "expected_digest": arguments["expected_digest"],
                "limits": self.attachment_limits,
            }
            preview = preview_import_snapshot(store, snapshot, **options)
            self._attachment_fits(command, preview)
            return import_snapshot(store, snapshot, **options).to_dict()
        integer(arguments["revision"], "pinned revision", 1, 2**63 - 2)
        sha256(arguments["expected_digest"])
        selected = store.get(arguments["event_id"], arguments["revision"])
        if selected.digest != arguments["expected_digest"]:
            raise _Rejected("conflict")
        if command == "attachment_snapshot_export":
            return export_snapshot(store, selected.event_id, selected.revision, limits=self.attachment_limits).to_dict()
        pin = {"event_id": selected.event_id, "revision": selected.revision, "digest": selected.digest}
        items = manager.list(selected.event_id, revision=selected.revision)
        if command == "attachment_list":
            return {"format": ATTACHMENT_LIST_FORMAT, **pin, "attachments": [item.to_dict() for item in items]}
        data = manager.read(selected.event_id, arguments["name"], revision=selected.revision)
        item = next(item for item in items if item.name == arguments["name"])
        return {
            "format": ATTACHMENT_DATA_FORMAT,
            **pin,
            "attachment": item.to_dict(),
            "data": _encode_attachment_data(data),
        }

    def _attachment_fits(self, command: str, preview: AnnotationRevision) -> None:
        try:
            encode_wire(_envelope(command, result=preview.to_dict()), limit=self.max_wire_bytes)
        except InputError:
            raise _Rejected("too_large") from None

    def _binding_fits(self, binding: dict[str, Any]) -> None:
        # All 128 fixed-format completion records, counters and response/state
        # envelope fit in this headroom, independently of annotation/text sizes.
        # Refuse an unreportable operation before begin or resumed worker effects.
        try:
            if self.max_wire_bytes <= 65536:
                raise _Rejected("too_large")
            encode_wire(binding, limit=self.max_wire_bytes - 65536)
        except InputError:
            raise _Rejected("too_large") from None


def create_annotation_server(
    store_path: str | Path,
    pipelines: Mapping[str, RemoteAnnotationPipeline],
    *,
    token_env: str,
    host: str = "127.0.0.1",
    port: int = 0,
    enable_execution_journal: bool = False,
    enable_attachments: bool = False,
    attachment_limits: AttachmentLimits | None = None,
    request_timeout: float = 30,
    max_connections: int = 8,
    max_wire_bytes: int = MAX_WIRE_BYTES,
) -> AnnotationServer:
    """Create a local server; journal and attachment migrations require separate opt-ins."""
    _credential_name(token_env)
    _token(token_env)
    timeout = _timeout(request_timeout)
    try:
        if not isinstance(host, str):
            raise ValueError
        address = ipaddress.ip_address(host)
        if not address.is_loopback or "%" in host:
            raise ValueError
    except ValueError:
        raise AnnotationServiceError("service host must be a literal-loopback address") from None
    if type(port) is not int or not 0 <= port <= 65535:
        raise AnnotationServiceError("invalid service port")
    if type(max_connections) is not int or not 1 <= max_connections <= 64:
        raise AnnotationServiceError("max_connections must be between 1 and 64")
    if type(max_wire_bytes) is not int or not 1024 <= max_wire_bytes <= MAX_WIRE_BYTES:
        raise AnnotationServiceError("wire byte limit must be between 1024 and 16 MiB")
    if type(enable_execution_journal) is not bool:
        raise AnnotationServiceError("execution journal opt-in must be a boolean")
    if type(enable_attachments) is not bool:
        raise AnnotationServiceError("attachment opt-in must be a boolean")
    if attachment_limits is not None and not isinstance(attachment_limits, AttachmentLimits):
        raise AnnotationServiceError("attachment limits must be an AttachmentLimits value")
    limits = AttachmentLimits(**attachment_limits.to_dict()) if attachment_limits is not None else AttachmentLimits()
    if not isinstance(pipelines, Mapping) or len(pipelines) > 128:
        raise AnnotationServiceError("registry must contain at most 128 pipelines")
    registry = dict(pipelines)
    for key, pipeline in registry.items():
        identifier(key, "registered pipeline ID")
        if not isinstance(pipeline, RemoteAnnotationPipeline) or pipeline.pipeline_id != key:
            raise AnnotationServiceError("registry keys must match pinned pipeline IDs")
    path = Path(store_path).resolve()
    server = AnnotationServer(
        path,
        registry,
        token_env=token_env,
        host=str(address),
        port=port,
        request_timeout=timeout,
        max_connections=max_connections,
        max_wire_bytes=max_wire_bytes,
        attachment_limits=limits,
    )
    try:
        with AnnotationStore(path) as store:
            if enable_execution_journal:
                store.enable_execution_journal()
            if enable_attachments:
                store.enable_attachments()
    except Exception:
        server.server_close()
        raise
    return server


def _registry(value: Any) -> dict[str, RemoteAnnotationPipeline]:
    data = _object(value, {"format", "pipelines"}, "service configuration")
    if data["format"] != CONFIG_FORMAT or not isinstance(data["pipelines"], list) or len(data["pipelines"]) > 128:
        raise AnnotationServiceError("unsupported service configuration")
    registry = {}
    for raw in data["pipelines"]:
        item = _object(raw, {"id", "version", "workers"}, "pipeline configuration")
        identifier(item["id"], "pipeline ID")
        if item["id"] in registry or not isinstance(item["workers"], list) or not 1 <= len(item["workers"]) <= 128:
            raise AnnotationServiceError("invalid or duplicate pipeline configuration")
        workers = []
        for raw_worker in item["workers"]:
            worker = _object(
                raw_worker, {"endpoint", "description", "token_env", "timeout", "max_response_bytes"}, "worker"
            )
            workers.append(
                RemoteAnnotationProcessor(
                    **{**worker, "description": ProcessorDescription.from_dict(worker["description"])}
                )
            )
        registry[item["id"]] = RemoteAnnotationPipeline(item["id"], item["version"], tuple(workers))
    return registry


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--token-env", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--enable-execution-journal", action="store_true")
    parser.add_argument("--enable-attachments", action="store_true")
    for name, default in AttachmentLimits().to_dict().items():
        parser.add_argument("--attachment-" + name.replace("_", "-"), type=int, default=default)
    args = parser.parse_args(argv)
    try:
        config = args.config.resolve()
        database = args.store.resolve()
        for protected in (database, *(Path(str(database) + suffix) for suffix in ("-wal", "-shm", "-journal"))):
            if protected == config or (protected.exists() and config.samefile(protected)):
                raise AnnotationServiceError("configuration must not alias the database or its sidecars")
        with config.open("rb") as stream:
            registry = _registry(decode_wire(stream.read(MAX_WIRE_BYTES + 1)))
        with create_annotation_server(
            database,
            registry,
            token_env=args.token_env,
            host=args.host,
            port=args.port,
            enable_execution_journal=args.enable_execution_journal,
            enable_attachments=args.enable_attachments,
            attachment_limits=AttachmentLimits(
                **{name: getattr(args, "attachment_" + name) for name in AttachmentLimits().to_dict()}
            ),
        ) as server:
            print(server.endpoint, flush=True)
            with suppress(KeyboardInterrupt):
                server.serve_forever(poll_interval=0.05)
        return 0
    except (InputError, AnnotationPipelineError, OSError, sqlite3.Error):
        print("annotation service configuration or store is unavailable", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover - exercised through main
    raise SystemExit(main())
