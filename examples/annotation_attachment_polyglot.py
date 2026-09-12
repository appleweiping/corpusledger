"""Original binary-attachment oracle using real Go/Java clients and Python service.

Requires an existing verified worker build and installed Go/JDK; never downloads.
All generated binaries, blobs and databases live in a fresh temporary directory.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import importlib.util
import json
import os
import platform
import queue
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
TOKEN_ENV = "CORPUSLEDGER_ATTACHMENT_ORACLE_TOKEN"
GO_TOKEN_ENV = "CORPUSLEDGER_ATTACHMENT_GO_TOKEN"
JAVA_TOKEN_ENV = "CORPUSLEDGER_ATTACHMENT_JAVA_TOKEN"
RAW_LIMIT = 4 * 1024 * 1024
SNAPSHOT_LIMIT = 12 * 1024 * 1024
WIRE_LIMIT = 16 * 1024 * 1024
TEXT = "A😀 e\u0301\r\n\t終\u00a0Z"
TOKEN_SPANS = ((0, 2, "A😀"), (3, 5, "e\u0301"), (8, 9, "終"), (10, 11, "Z"))


def binary_fixture() -> bytes:
    """The raw hard-limit payload contains every octet, including NUL and 0xff."""
    return bytes(range(256)) * (RAW_LIMIT // 256)


def source_inventory() -> dict[str, str]:
    """Bind the actual runtime and local helpers, not an assumed installed release."""
    files = [
        *sorted((ROOT / "src/corpusledger").glob("*.py")),
        ROOT / "examples/annotation_attachment_polyglot.py",
        ROOT / "interop/verify_workers.py",
        ROOT / "interop/go/attachment_example/main.go",
        ROOT / "interop/go/go.mod",
        ROOT / "interop/java/AttachmentExample.java",
        ROOT / "interop/java/dependencies.json",
    ]
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def runtime_matches_sources(sources: dict[str, str]) -> bool:
    import corpusledger

    directory = Path(corpusledger.__file__).resolve().parent
    actual = {
        "src/corpusledger/" + path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.glob("*.py")
    }
    return actual == {name: digest for name, digest in sources.items() if name.startswith("src/corpusledger/")}


def _json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("helper unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def verified_workers(build_dir: Path) -> dict[str, str]:
    with (build_dir / "build.json").open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("worker build descriptor too large")
    values = json.loads(raw)
    names = ("go_worker", "java_worker", "jackson_core")
    if not isinstance(values, dict) or set(values) != {key for name in names for key in (name, name + "_sha256")}:
        raise ValueError("unexpected worker build descriptor")
    lock = json.loads((ROOT / "interop/java/dependencies.json").read_text(encoding="utf-8"))
    if values["jackson_core_sha256"] != lock["artifacts"][0]["sha256"]:
        raise ValueError("worker dependency does not match existing repository pin")
    for name in names:
        path = Path(values[name])
        if not path.is_file() or not 1 <= path.stat().st_size <= 32 * 1024 * 1024:
            raise ValueError("worker artifact unavailable or too large")
        if hashlib.sha256(path.read_bytes()).hexdigest() != values[name + "_sha256"]:
            raise ValueError("worker artifact checksum mismatch")
    return values


def compile_examples(output: Path, workers: dict[str, str]) -> dict[str, object]:
    output = output.resolve()
    if output == ROOT or ROOT in output.parents or output in ROOT.parents:
        raise ValueError("example build must be outside the source checkout")
    tools = {name: shutil.which(name) for name in ("go", "java", "javac")}
    if not all(tools.values()):
        raise ValueError("Go and JDK must already be installed")
    output.mkdir(exist_ok=False)
    classes = output / "classes"
    classes.mkdir()
    binary = output / ("attachment-upload.exe" if os.name == "nt" else "attachment-upload")
    environment = {**os.environ, "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOWORK": "off"}
    commands = (
        [tools["go"], "build", "-trimpath", "-o", str(binary), "./attachment_example"],
        [
            tools["javac"],
            "--release",
            "21",
            "-Xlint:all",
            "-Werror",
            "-cp",
            workers["jackson_core"],
            "-d",
            str(classes),
            str(ROOT / "interop/java/AttachmentExample.java"),
        ],
    )
    for command in commands:
        result = subprocess.run(
            command,
            cwd=ROOT / "interop/go",
            env=environment,
            capture_output=True,
            timeout=180,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if result.returncode:
            raise RuntimeError("native attachment example compilation failed")
    versions = {}
    for name, flag in (("go", "version"), ("java", "-version"), ("javac", "-version")):
        result = subprocess.run(
            [tools[name], flag],
            capture_output=True,
            timeout=10,
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        versions[name] = (result.stdout + result.stderr).decode("utf-8", "replace").strip()
    return {
        "go": [str(binary)],
        "java": [tools["java"], "-cp", os.pathsep.join((str(classes), workers["jackson_core"])), "AttachmentExample"],
        "tool_versions": versions,
        "go_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "java_class_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(classes.glob("*.class"))
        },
    }


def native_call(command: list[str], configuration: dict, credentials: dict[str, str]) -> dict:
    result = subprocess.run(
        command,
        input=_json(configuration),
        env={**os.environ, **credentials},
        capture_output=True,
        timeout=45,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    if len(result.stdout) > 65536 or len(result.stderr) > 65536:
        raise RuntimeError("native oracle output exceeds bound")
    if any(secret.encode() in result.stdout + result.stderr for secret in credentials.values()):
        raise RuntimeError("native oracle disclosed credential")
    if result.returncode or result.stderr:
        raise RuntimeError("native attachment oracle rejected the workflow")
    value = json.loads(result.stdout)
    if not isinstance(value, dict) or value.get("verified") is not True:
        raise RuntimeError("native attachment oracle did not verify")
    return value


def serve(database: Path, stop_file: Path) -> None:
    from corpusledger.annotation_execution import RemoteAnnotationPipeline
    from corpusledger.annotation_protocol import ProcessorDescription, decode_wire
    from corpusledger.annotation_remote import RemoteAnnotationProcessor
    from corpusledger.annotation_service import create_annotation_server

    configuration = decode_wire(sys.stdin.buffer.read(65537), limit=65536)
    if not isinstance(configuration, dict) or set(configuration) != {"go", "java"}:
        raise ValueError("invalid private harness configuration")
    processors = []
    for name, token_env in (("go", GO_TOKEN_ENV), ("java", JAVA_TOKEN_ENV)):
        item = configuration[name]
        if not isinstance(item, dict) or set(item) != {"endpoint", "description"}:
            raise ValueError("invalid private worker configuration")
        processors.append(
            RemoteAnnotationProcessor(item["endpoint"], ProcessorDescription.from_dict(item["description"]), token_env)
        )
    pipeline = RemoteAnnotationPipeline("attachment.chain", "1", tuple(reversed(processors)))
    server = create_annotation_server(
        database,
        {"attachment.chain": pipeline},
        token_env=TOKEN_ENV,
        enable_execution_journal=True,
        enable_attachments=True,
    )
    closed = threading.Event()

    def stop_when_requested():
        while not closed.wait(0.05):
            if stop_file.exists():
                server.shutdown()
                return

    watcher = threading.Thread(target=stop_when_requested, daemon=True)
    watcher.start()
    try:
        print(server.endpoint, flush=True)
        server.serve_forever(poll_interval=0.05)
    finally:
        closed.set()
        server.server_close()
        watcher.join(timeout=2)


class ServiceProcess:
    def __init__(self, database: Path, stop_file: Path, configuration: dict, credentials: dict[str, str]):
        self.stop_file, self.credentials = stop_file, credentials
        if stop_file.exists():
            raise ValueError("service stop marker must be a new path")
        self.process = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--serve",
                "--database",
                str(database),
                "--stop-file",
                str(stop_file),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, **credentials},
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        ready = queue.Queue()
        threading.Thread(target=lambda: ready.put(self.process.stdout.readline(1024)), daemon=True).start()
        try:
            self.process.stdin.write(_json(configuration))
            self.process.stdin.close()
            endpoint = ready.get(timeout=20).decode("ascii").strip()
            value = urlsplit(endpoint)
            if (
                value.scheme != "http"
                or value.hostname != "127.0.0.1"
                or not value.port
                or value.path
                or value.query
                or value.fragment
            ):
                raise RuntimeError("private service did not start")
            self.endpoint = endpoint
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process.poll() is None:
            self.stop_file.touch(exist_ok=False)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                if os.name == "nt":
                    subprocess.run(
                        ["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                        capture_output=True,
                        timeout=10,
                        creationflags=subprocess.CREATE_NO_WINDOW,
                        check=False,
                    )
                else:
                    self.process.kill()
                self.process.wait(timeout=10)
        if not self.process.stdin.closed:
            self.process.stdin.close()
        if not self.process.stderr.closed:
            logs = self.process.stderr.read(65537)
            if len(logs) > 65536 or any(secret.encode() in logs for secret in self.credentials.values()):
                raise RuntimeError("private service log violated its bound")
        self.process.stdout.close()
        self.process.stderr.close()


def _wire(endpoint: str, token: str, command: str, arguments: dict) -> tuple[int, dict]:
    address = urlsplit(endpoint)
    connection = http.client.HTTPConnection(address.hostname, address.port, timeout=30)
    try:
        connection.request(
            "POST",
            "/v1/events",
            _json({"format": "corpusledger.event-command.v1", "command": command, "arguments": arguments}),
            {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        raw = response.read(WIRE_LIMIT + 1)
        if len(raw) > WIRE_LIMIT:
            raise RuntimeError("oracle response exceeded wire bound")
        return response.status, json.loads(raw)
    finally:
        connection.close()


def _header_bound(endpoint: str, token: str) -> int:
    address = urlsplit(endpoint)
    connection = http.client.HTTPConnection(address.hostname, address.port, timeout=10)
    try:
        connection.putrequest("POST", "/v1/events")
        connection.putheader("Authorization", "Bearer " + token)
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(WIRE_LIMIT + 1))
        connection.endheaders()  # Rejection must happen without accepting a huge body.
        response = connection.getresponse()
        response.read(65537)
        return response.status
    finally:
        connection.close()


def verify(build_dir: Path) -> dict:
    from unittest.mock import patch

    from corpusledger.annotation_client import AnnotationClient, AnnotationClientError
    from corpusledger.annotation_store import AnnotationEvent, AnnotationStore
    from corpusledger.annotations import AnnotationDocument

    if not __debug__:
        raise ValueError("oracle assertions require Python without optimization")
    sources = source_inventory()
    if not runtime_matches_sources(sources):
        raise ValueError("imported runtime does not match the source checkout")
    workers = verified_workers(build_dir)
    helper = _module(ROOT / "interop/verify_workers.py", "_attachment_worker_harness")
    java = shutil.which("java")
    if java is None:
        raise ValueError("Java must already be installed")
    worker_commands = (
        [workers["go_worker"]],
        [
            java,
            "--add-modules",
            "jdk.httpserver",
            "-cp",
            os.pathsep.join((workers["java_worker"], workers["jackson_core"])),
            "ReferenceWorker",
        ],
    )
    with tempfile.TemporaryDirectory(prefix="corpusledger-attachment-polyglot-") as temporary, ExitStack() as stack:
        directory = Path(temporary)
        compiled = compile_examples(directory / "compiled", workers)
        go, java_worker = (stack.enter_context(helper._worker_context(command)) for command in worker_commands)
        credentials = {TOKEN_ENV: secrets.token_urlsafe(32), GO_TOKEN_ENV: go.token, JAVA_TOKEN_ENV: java_worker.token}
        configuration = {
            "go": {"endpoint": go.address.geturl(), "description": go.description.to_dict()},
            "java": {"endpoint": java_worker.address.geturl(), "description": java_worker.description.to_dict()},
        }
        stack.enter_context(patch.dict(os.environ, credentials))
        payload = binary_fixture()
        payload_file = directory / "opaque.bin"
        payload_file.write_bytes(payload)
        sha = hashlib.sha256(payload).hexdigest()
        database = directory / "events.sqlite"
        service = ServiceProcess(database, directory / "stop-1", configuration, credentials)
        stack.callback(service.close)
        client = AnnotationClient(service.endpoint, TOKEN_ENV)
        selected = AnnotationDocument("selected", TEXT)
        sibling = AnnotationDocument("untouched", "Original sibling\r\n🙂 {{not-a-command}}")
        event = AnnotationEvent("attachment.oracle", (selected, sibling), {"flag": True, "big": 2**200}, version=2)
        source = client.create(event)
        upload_config = {
            "endpoint": service.endpoint,
            "event_id": event.event_id,
            "name": "all-octets.bin",
            "payload_file": str(payload_file),
            "expected_revision": 1,
            "expected_digest": source.digest,
            "command_id": "go.upload.original",
        }
        uploaded = native_call(compiled["go"], upload_config, credentials)
        assert uploaded["size"] == RAW_LIMIT and uploaded["sha256"] == sha and uploaded["revision"] == 2
        pin = client.get(event.event_id, revision=2)
        assert pin.digest == uploaded["digest"] and pin.parent_digest == source.digest
        assert len(pin.event.attachments) == 1 and pin.event.attachments[0].sha256 == sha
        service.close()
        service = ServiceProcess(database, directory / "stop-2", configuration, credentials)
        stack.callback(service.close)
        client = AnnotationClient(service.endpoint, TOKEN_ENV)
        read_config = {
            "endpoint": service.endpoint,
            "event_id": event.event_id,
            "name": "all-octets.bin",
            "payload_file": str(payload_file),
            "revision": 2,
            "digest": pin.digest,
        }
        assert native_call(compiled["java"], read_config, credentials)["sha256"] == sha
        assert (
            client.read_attachment(event.event_id, "all-octets.bin", revision=2, expected_digest=pin.digest) == payload
        )
        assert client.list_attachments(event.event_id, revision=2, expected_digest=pin.digest) == pin.event.attachments
        snapshot = client.export_attachment_snapshot(event.event_id, revision=2, expected_digest=pin.digest)
        assert snapshot.source_revision == 2 and snapshot.source_digest == pin.digest
        assert snapshot.blobs[0].data == payload and len(snapshot.to_bytes()) < SNAPSHOT_LIMIT
        imported_service = ServiceProcess(
            directory / "imported.sqlite", directory / "stop-import", configuration, credentials
        )
        stack.callback(imported_service.close)
        imported_client = AnnotationClient(imported_service.endpoint, TOKEN_ENV)
        imported = imported_client.import_attachment_snapshot(
            snapshot, command_id="python.import.snapshot", expected_snapshot_digest=snapshot.digest
        )
        assert imported.revision == 1 and imported.event.digest == pin.event.digest
        assert (
            imported_client.import_attachment_snapshot(
                snapshot, command_id="python.import.snapshot", expected_snapshot_digest=snapshot.digest
            ).digest
            == imported.digest
        )
        imported_read = {**read_config, "endpoint": imported_service.endpoint, "revision": 1, "digest": imported.digest}
        assert native_call(compiled["java"], imported_read, credentials)["sha256"] == sha
        assert [revision.revision for revision in imported_client.history(event.event_id)] == [1]
        imported_service.close()
        selection = {"selected": "attachment.chain"}
        operation = client.begin(
            "attachment.text.operation", event.event_id, selection, expected_revision=2, expected_digest=pin.digest
        )
        assert operation.status == "ready"
        committed = client.resume(operation.operation_id, selection)
        assert committed.status == "committed" and committed.completed_steps == 2
        processed = client.get(event.event_id)
        assert processed.revision == 3 and processed.parent_digest == pin.digest
        assert processed.event.attachments == pin.event.attachments
        assert (
            client.read_attachment(event.event_id, "all-octets.bin", revision=3, expected_digest=processed.digest)
            == payload
        )
        assert _json(processed.event.get_document("untouched").to_dict()) == _json(sibling.to_dict())
        document = processed.event.get_document("selected")
        tokens = sorted(
            (row for row in document.annotations if row.type_name == "token"), key=lambda row: row.features["position"]
        )
        assert tuple((row.start, row.end, row.features["text"]) for row in tokens) == TOKEN_SPANS
        groups = [row for row in document.annotations if row.type_name == "token_group"]
        assert len(groups) == 1 and groups[0].start == 0 and groups[0].end == 11
        assert groups[0].features["count"] == 4 and tuple(groups[0].features["members"]) == tuple(
            row.annotation_id for row in tokens
        )
        assert _json(processed.event.to_dict()["metadata"]) == _json(event.to_dict()["metadata"])
        detached = client.detach(
            event.event_id,
            "all-octets.bin",
            expected_revision=3,
            expected_digest=processed.digest,
            command_id="python.detach.original",
        )
        assert detached.revision == 4 and detached.event.attachments == ()
        assert native_call(compiled["java"], read_config, credentials)["size"] == RAW_LIMIT
        assert (
            client.detach(
                event.event_id,
                "all-octets.bin",
                expected_revision=3,
                expected_digest=processed.digest,
                command_id="python.detach.original",
            ).digest
            == detached.digest
        )
        upload_config["endpoint"] = service.endpoint
        assert native_call(compiled["go"], upload_config, credentials)["digest"] == pin.digest
        assert [revision.revision for revision in client.history(event.event_id)] == [1, 2, 3, 4]
        try:
            client.read_attachment(event.event_id, "all-octets.bin", revision=4, expected_digest=detached.digest)
        except AnnotationClientError as error:
            assert error.code == "not_found"
        else:
            raise AssertionError("detached latest attachment remained visible")
        assert _header_bound(service.endpoint, credentials[TOKEN_ENV]) == 413
        assert client.get(event.event_id).digest == detached.digest
        oversize_status, oversize_error = _wire(
            service.endpoint,
            credentials[TOKEN_ENV],
            "attachment_attach",
            {
                "event_id": event.event_id,
                "name": "too-large.bin",
                "data": base64.b64encode(payload + b"\x00").decode("ascii"),
                "media_type": "application/octet-stream",
                "expected_revision": 4,
                "expected_digest": detached.digest,
                "command_id": "rejected.raw.oversize",
            },
        )
        assert oversize_status == 413 and oversize_error["ok"] is False
        assert oversize_error["error"]["code"] == "too_large"
        assert client.get(event.event_id).digest == detached.digest
        # A valid event and raw BLOB each fit their own limits. Their combined
        # inline snapshot cannot fit 12 MiB, although its request and both write
        # acknowledgements fit the 16 MiB HTTP envelope. This is not a malformed
        # or checksum-forged snapshot masquerading as a byte-limit test.
        limit_event = AnnotationEvent("snapshot.limit.oracle", metadata={"padding": "x" * (7 * 1024 * 1024)}, version=2)
        limit_source = client.create(limit_event)
        limit_upload = {
            **upload_config,
            "event_id": limit_event.event_id,
            "expected_revision": 1,
            "expected_digest": limit_source.digest,
            "command_id": "go.upload.snapshot.limit",
        }
        limit_ack = native_call(compiled["go"], limit_upload, credentials)
        snapshot_status, snapshot_error = _wire(
            service.endpoint,
            credentials[TOKEN_ENV],
            "attachment_snapshot_export",
            {"event_id": limit_event.event_id, "revision": 2, "expected_digest": limit_ack["digest"]},
        )
        assert snapshot_status == 413 and snapshot_error["ok"] is False
        assert snapshot_error["error"]["code"] == "too_large"
        assert client.get(limit_event.event_id).digest == limit_ack["digest"]
        assert [revision.revision for revision in client.history(limit_event.event_id)] == [1, 2]
        service.close()
        with AnnotationStore(database, create=False) as store:
            assert store.attachments_enabled and store.get(event.event_id).revision == 4
            assert store.get(event.event_id, revision=2).digest == pin.digest
        if (
            sources != source_inventory()
            or not runtime_matches_sources(sources)
            or workers != verified_workers(build_dir)
        ):
            raise RuntimeError("source or worker artifact changed during the oracle")
        return {
            "format": "corpusledger.attachment-polyglot-oracle.v1",
            "kind": "original-synthetic-real-process-contract-check",
            "passed": True,
            "network_scope": "literal-loopback only",
            "payload_bytes": len(payload),
            "payload_sha256": sha,
            "all_256_octets": len(set(payload)) == 256,
            "nul_included": b"\x00" in payload,
            "service_processes": 3,
            "native_upload_calls": 3,
            "native_read_calls": 3,
            "event_revisions": [1, 2, 3, 4],
            "text_worker_steps": 2,
            "tokens": 4,
            "reference_groups": 1,
            "history_retained_after_detach": True,
            "idempotent_original_pins": True,
            "wire_16mib_header_rejected": True,
            "blob_4mib_accepted": True,
            "blob_4mib_plus_one_rejected_without_revision": True,
            "blob_oversize_status": oversize_status,
            "snapshot_export_import_verified": True,
            "snapshot_import_revisions": [1],
            "snapshot_12mib_rejected_without_revision": True,
            "snapshot_rejection": snapshot_error["error"]["code"],
            "snapshot_oversize_status": snapshot_status,
            "snapshot_limit_event_revisions": [1, 2],
            "snapshot_sha256": snapshot.digest,
            "source_event_sha256": event.digest,
            "attachment_revision_sha256": pin.digest,
            "final_revision_sha256": detached.digest,
            "tool_versions": compiled["tool_versions"],
            "python_version": platform.python_version(),
            "source_sha256": sources,
            "sources_unchanged_during_run": True,
            "imported_runtime_matches_sources": True,
            "go_binary_sha256": compiled["go_binary_sha256"],
            "java_class_sha256": compiled["java_class_sha256"],
            "worker_hashes": {key: value for key, value in workers.items() if key.endswith("_sha256")},
            "limits_not_claimed": [
                "general SDK",
                "deployment security",
                "malicious code execution",
                "learned NLP quality",
            ],
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--database", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--stop-file", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.serve:
            if args.build_dir is not None or args.database is None or args.stop_file is None:
                raise ValueError("private service arguments invalid")
            serve(args.database, args.stop_file)
        else:
            if args.build_dir is None or args.database is not None or args.stop_file is not None:
                raise ValueError("example arguments invalid")
            print(json.dumps(verify(args.build_dir), sort_keys=True))
        return 0
    except Exception:
        print("attachment_polyglot_oracle_failed", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
