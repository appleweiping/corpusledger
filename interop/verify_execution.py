"""Offline durable Python-service -> Go -> Java process integration verification.

The private --serve mode is a test harness, not a user-facing service launcher.
It consumes only nonsecret, pinned worker configuration through bounded stdin.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from verify_workers import _worker_context

from corpusledger.annotation_execution import RemoteAnnotationPipeline
from corpusledger.annotation_protocol import ProcessorDescription, decode_wire, encode_wire
from corpusledger.annotation_remote import RemoteAnnotationProcessor
from corpusledger.annotation_store import AnnotationEvent
from corpusledger.annotations import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation

SERVICE_TOKEN_ENV = "CORPUSLEDGER_INTEROP_SERVICE_TOKEN"
GO_TOKEN_ENV = "CORPUSLEDGER_INTEROP_GO_TOKEN"
JAVA_TOKEN_ENV = "CORPUSLEDGER_INTEROP_JAVA_TOKEN"
MAX_STARTUP_BYTES = 65536


def serve(database: Path) -> None:
    from corpusledger.annotation_service import create_annotation_server

    configuration = decode_wire(sys.stdin.buffer.read(MAX_STARTUP_BYTES + 1), limit=MAX_STARTUP_BYTES)
    if not isinstance(configuration, dict) or set(configuration) != {"go", "java"}:
        raise ValueError("invalid interop service configuration")
    workers = []
    for name, token_env in (("go", GO_TOKEN_ENV), ("java", JAVA_TOKEN_ENV)):
        item = configuration[name]
        if not isinstance(item, dict) or set(item) != {"endpoint", "description"}:
            raise ValueError("invalid interop worker configuration")
        workers.append(
            RemoteAnnotationProcessor(item["endpoint"], ProcessorDescription.from_dict(item["description"]), token_env)
        )
    # Deliberately reverse registration: declared schema dependencies decide
    # execution order, not a caller-controlled list of endpoints or step order.
    pipeline = RemoteAnnotationPipeline("demo.chain", "1", tuple(reversed(workers)))
    server = create_annotation_server(
        database,
        {"demo.chain": pipeline},
        token_env=SERVICE_TOKEN_ENV,
        enable_execution_journal=True,
    )
    try:
        print(server.endpoint, flush=True)
        server.serve_forever()
    finally:
        server.server_close()


class ServiceProcess:
    def __init__(self, database, configuration, credentials):
        self.credentials = credentials
        self.process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--serve", "--database", str(database)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, **credentials},
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        ready = queue.Queue()

        def read_address():
            ready.put(self.process.stdout.readline(1024))

        threading.Thread(target=read_address, daemon=True).start()
        try:
            self.process.stdin.write(encode_wire(configuration, limit=MAX_STARTUP_BYTES))
            self.process.stdin.close()
            endpoint = ready.get(timeout=15).decode("ascii").strip()
            parsed = urlsplit(endpoint)
            if (
                parsed.scheme != "http"
                or parsed.hostname != "127.0.0.1"
                or not parsed.port
                or parsed.path
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("service did not publish a literal loopback endpoint")
            self.endpoint = endpoint
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if not self.process.stdin.closed:
            self.process.stdin.close()
        if not self.process.stderr.closed:
            logs = self.process.stderr.read()
            assert all(secret.encode() not in logs for secret in self.credentials.values())
        self.process.stdout.close()
        self.process.stderr.close()


def verify(build_dir: Path) -> dict[str, object]:
    from corpusledger.annotation_client import AnnotationClient, AnnotationClientError

    def rejected(code, action):
        try:
            action()
        except AnnotationClientError as error:
            assert error.code == code
        else:
            raise AssertionError("invalid service command unexpectedly succeeded")

    artifacts = json.loads((build_dir / "build.json").read_text(encoding="utf-8"))
    for name in ("go_worker", "java_worker", "jackson_core"):
        assert hashlib.sha256(Path(artifacts[name]).read_bytes()).hexdigest() == artifacts[name + "_sha256"]
    java = shutil.which("java")
    if java is None:
        raise ValueError("Java must already be installed")
    commands = [
        [artifacts["go_worker"]],
        [
            java,
            "--add-modules",
            "jdk.httpserver",
            "-cp",
            os.pathsep.join((artifacts["java_worker"], artifacts["jackson_core"])),
            "ReferenceWorker",
        ],
    ]
    with ExitStack() as stack:
        go, java_worker = (stack.enter_context(_worker_context(command)) for command in commands)
        credentials = {
            GO_TOKEN_ENV: go.token,
            JAVA_TOKEN_ENV: java_worker.token,
            SERVICE_TOKEN_ENV: secrets.token_urlsafe(32),
        }
        configuration = {
            "go": {"endpoint": go.address.geturl(), "description": go.description.to_dict()},
            "java": {"endpoint": java_worker.address.geturl(), "description": java_worker.description.to_dict()},
        }
        seed_type = AnnotationType("seed", {"payload": AnnotationField("object")})
        seed = SpanAnnotation("seed.0", "seed", 0, 0, {"payload": {"flag": True, "number": 1, "big": 10**1000 + 123}})
        document = AnnotationDocument("selected", "A😀 e\u0301\r\n\t終\u00a0Z", (seed_type,), (seed,))
        sibling = AnnotationDocument("untouched", "original sibling\r\n🙂", (seed_type,), (seed,))
        source_event = AnnotationEvent("service.event", (document, sibling), {"flag": True, "number": 1})
        selection = {document.document_id: "demo.chain"}
        # Construct the expected product through the real worker wire too;
        # retain byte/digest comparisons, because Python True == 1 is not JSON identity.
        tokens, _ = go.apply(document, "expected.go")
        expected, _ = java_worker.apply(tokens, "expected.java")
        with (
            tempfile.TemporaryDirectory(prefix="corpusledger-service-check-") as directory,
            ExitStack() as service_stack,
        ):
            database = Path(directory) / "events.sqlite"
            with patch.dict(os.environ, {SERVICE_TOKEN_ENV: credentials[SERVICE_TOKEN_ENV]}):
                service = ServiceProcess(database, configuration, credentials)
                service_stack.callback(service.close)
                client = AnnotationClient(service.endpoint, SERVICE_TOKEN_ENV)
                source = client.create(source_event)
                assert source.revision == 1 and source.event.digest == source_event.digest
                assert client.get(source_event.event_id).digest == source.digest
                assert [item.event_id for item in client.list()] == [source_event.event_id]
                assert [item.revision for item in client.history(source_event.event_id)] == [1]
                with patch.dict(os.environ, {SERVICE_TOKEN_ENV: secrets.token_urlsafe(32)}):
                    rejected("unauthorized", lambda: client.get(source_event.event_id))
                rejected(
                    "request_invalid",
                    lambda: client.begin(
                        "unregistered.operation",
                        source_event.event_id,
                        {document.document_id: "unknown.pipeline"},
                        expected_revision=source.revision,
                        expected_digest=source.digest,
                    ),
                )
                rejected("not_found", lambda: client.status("unregistered.operation"))
                assert client.operations() == ()
                begun = client.begin(
                    "service.operation",
                    source_event.event_id,
                    selection,
                    expected_revision=source.revision,
                    expected_digest=source.digest,
                )
                assert begun.status == "ready" and begun.completed_steps == 0
                assert client.status(begun.operation_id).to_dict() == begun.to_dict()
                service.close()  # Real process termination after the durable begin response.
                restarted = ServiceProcess(database, configuration, credentials)
                service_stack.callback(restarted.close)
                client = AnnotationClient(restarted.endpoint, SERVICE_TOKEN_ENV)
                assert client.status(begun.operation_id).to_dict() == begun.to_dict()
                assert client.get(source_event.event_id).digest == source.digest
                committed = client.resume(begun.operation_id, selection)
                assert committed.status == "committed" and committed.completed_steps == 2
                final = client.get(source_event.event_id)
                assert final.revision == 2 and final.parent_digest == source.digest
                assert final.event.get_document(document.document_id).digest == expected.digest
                assert encode_wire(final.event.get_document(document.document_id).to_dict()) == encode_wire(
                    expected.to_dict()
                )
                assert encode_wire(final.event.get_document(sibling.document_id).to_dict()) == encode_wire(
                    sibling.to_dict()
                )
                assert client.get(source_event.event_id, revision=1).event.digest == source_event.digest
                before = encode_wire(committed.to_dict())
                assert committed.to_dict()["result"] == {"revision": 2, "digest": final.digest}
                assert all(secret.encode() not in before for secret in credentials.values())
                go.close()
                java_worker.close()
                # Repeating a committed key must remain a read: neither worker
                # is alive, and no third event revision or new attempt is allowed.
                assert (
                    encode_wire(
                        client.begin(
                            begun.operation_id,
                            source_event.event_id,
                            selection,
                            expected_revision=source.revision,
                            expected_digest=source.digest,
                        ).to_dict()
                    )
                    == before
                )
                assert encode_wire(client.resume(begun.operation_id, selection).to_dict()) == before
                assert [item.revision for item in client.history(source_event.event_id)] == [1, 2]
                assert client.status(begun.operation_id).status == "committed"
                assert client.list()[0].revision == 2
                rejected(
                    "conflict",
                    lambda: client.resume(begun.operation_id, {sibling.document_id: "demo.chain"}),
                )
                assert encode_wire(client.status(begun.operation_id).to_dict()) == before
                assert [item.operation_id for item in client.operations()] == [begun.operation_id]
                assert client.operations(after_operation_id=begun.operation_id) == ()
                first_page = client.operation_history(begun.operation_id, limit=2)
                remaining = client.operation_history(begun.operation_id, after_version=first_page[-1].version)
                journal = (*first_page, *remaining)
                assert [item.version for item in journal] == list(range(1, committed.version + 1))
                assert journal[0].status == "ready" and encode_wire(journal[-1].to_dict()) == before
                assert client.operation_history(begun.operation_id, after_version=committed.version) == ()
                restarted.close()  # Close SQLite in the child before test directory cleanup on Windows.
                return {
                    "kind": "checked-in-cross-language-service-contract-test",
                    "passed": True,
                    "processes": ["Python client", "Python event service", "Go token worker", "Java group worker"],
                    "checks": [
                        "authenticated-create-get-list-history",
                        "begin-status-before-worker-execution",
                        "separate-service-process-restart-and-resume",
                        "server-registered-dependency-pipeline",
                        "exact-source-history-and-untouched-sibling",
                        "Unicode-codepoints-and-1001-digit-integer",
                        "atomic-operation-event-revision-result",
                        "committed-key-idempotence-with-workers-stopped",
                        "authentication-and-unknown-registry-rejected-without-operation",
                        "committed-selection-change-rejected-without-mutation",
                        "exclusive-cursor-operation-history-pagination",
                        "secrets-not-in-journal-or-service-log",
                    ],
                    "source_event_digest": source_event.digest,
                    "final_event_digest": final.event.digest,
                    "final_revision_digest": final.digest,
                    "attempts": committed.to_dict()["attempts"],
                    "revision": final.revision,
                    "status": committed.status,
                }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path)
    parser.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--database", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.serve:
        if args.database is None or args.build_dir is not None:
            parser.error("private service harness requires only a database path")
        try:
            serve(args.database)
        except Exception:
            # Test launcher errors must not expose documents or credentials.
            print("interop_service_failed", file=sys.stderr)
            raise SystemExit(2) from None
    else:
        if args.build_dir is None or args.database is not None:
            parser.error("verification requires only --build-dir")
        print(json.dumps(verify(args.build_dir), sort_keys=True))


if __name__ == "__main__":
    main()
