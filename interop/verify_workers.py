"""Real Python -> Go -> Java protocol regression checks against compiled workers.

Requires the development CorpusLedger checkout/installation and build.json from
build_workers.py. No network, corpus downloads, or reference project code run.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import http.client
import json
import os
import queue
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from corpusledger.annotation_execution import (
    AnnotationExecutionUncertain,
    AnnotationExecutor,
    RemoteAnnotationPipeline,
)
from corpusledger.annotation_pipeline import AnnotationPipeline
from corpusledger.annotation_protocol import (
    AnnotationRequest,
    AnnotationResponse,
    ProcessorDescription,
    decode_wire,
    encode_wire,
)
from corpusledger.annotation_remote import RemoteAnnotationProcessor
from corpusledger.annotation_store import AnnotationEvent, AnnotationStore
from corpusledger.annotations import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation


class Worker:
    def __init__(self, argv, *, token=None):
        self.argv = argv
        self.token = token or secrets.token_urlsafe(32)
        self.process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "CORPUSLEDGER_WORKER_TOKEN": self.token},
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        ready = queue.Queue()

        def read_address():
            ready.put(self.process.stdout.readline(1024))

        reader = threading.Thread(target=read_address, daemon=True)
        reader.start()
        try:
            address = ready.get(timeout=15).decode("ascii").strip()
            self.address = urlsplit(address)
            if self.address.scheme != "http" or self.address.hostname not in {"127.0.0.1", "::1"}:
                raise ValueError("worker did not publish a literal loopback address")
            if self.address.path or self.address.query or self.address.fragment or not self.address.port:
                raise ValueError("invalid worker startup address")
            self.description = ProcessorDescription.from_dict(self.request("GET", "/v1/info")[1])
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
        self.process.stdout.close()
        self.process.stderr.close()

    def restart(self):
        port, token, argv = self.address.port, self.token, self.argv
        self.close()
        self.__init__([*argv, "--port", str(port)], token=token)

    def request(self, method, path, body=None, *, authorized=True, extra_headers=None):
        raw = encode_wire(body) if body is not None and not isinstance(body, bytes) else body
        headers = {"Connection": "close"}
        if authorized:
            headers["Authorization"] = "Bearer " + self.token
        if raw is not None:
            headers["Content-Type"] = "application/json"
        headers.update(extra_headers or {})
        # HTTPConnection has no environment-proxy or redirect behavior.
        connection = http.client.HTTPConnection(self.address.hostname, self.address.port, timeout=12)
        try:
            connection.request(method, path, body=raw, headers=headers)
            response = connection.getresponse()
            payload = response.read(16 * 1024 * 1024 + 1)
            return response.status, decode_wire(payload)
        finally:
            connection.close()

    def apply(self, document, step):
        request = AnnotationRequest("interop.operation", step, self.description, document)
        status, response = self.request("POST", "/v1/process", request.to_dict())
        assert status == 200, response
        result = AnnotationResponse.from_dict(response).apply(request)
        return result.document, request


def check_rejection(worker, request):
    assert worker.request("GET", "/v1/info", authorized=False)[0] == 401
    assert worker.request("GET", "/v1/info", extra_headers={"Origin": "https://example.invalid"})[0] == 400
    assert worker.request("GET", "/v1/info?secret=never-print")[0] == 404
    for raw in (
        b'{"format":1,"format":2}',
        b'{"secret":"\xff"}',
        b'{"secret":"\\ud800"}',
        b"[1e400]",
        b"{} []",
        b"[" * 65 + b"0" + b"]" * 65,
        b"[" + b"1" * 4301 + b"]",
    ):
        status, error = worker.request("POST", "/v1/process", raw)
        assert status == 400 and error == {"error": "request_rejected"}
    for mutate in (
        lambda r: r.update(extra=True),
        lambda r: r["processor"].update(version="wrong"),
        lambda r: r["document"].update(text_sha256="f" * 64),
        lambda r: r["document"].update(offset_unit="utf16"),
        lambda r: r["document"]["annotations"][0]["features"].update(big=True),
    ):
        value = copy.deepcopy(request.to_dict())
        mutate(value)
        status, error = worker.request("POST", "/v1/process", value)
        assert status == 400 and error == {"error": "request_rejected"}


def check_slow_request(worker):
    # Stop sending halfway through a body. The worker must close the connection
    # rather than retain a parser/worker indefinitely; never print the token.
    with socket.create_connection((worker.address.hostname, worker.address.port), timeout=12) as stream:
        stream.settimeout(12)
        stream.sendall(
            (
                "POST /v1/process HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer "
                + worker.token
                + "\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{"
            ).encode()
        )
        started = time.monotonic()
        try:
            while stream.recv(4096):
                pass
        except ConnectionResetError:
            pass  # Windows may report the enforced disconnect as a reset, not EOF.
        assert time.monotonic() - started < 11


def check_startup_rejection(command):
    for extra, token in (([], None), (["--host", "0.0.0.0"], secrets.token_urlsafe(32))):
        env = dict(os.environ)
        env.pop("CORPUSLEDGER_WORKER_TOKEN", None)
        if token is not None:
            env["CORPUSLEDGER_WORKER_TOKEN"] = token
        completed = subprocess.run(
            [*command, *extra],
            env=env,
            capture_output=True,
            check=False,
            timeout=15,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        assert completed.returncode == 2 and completed.stdout == b""
        assert completed.stderr.strip() == b"worker_start_or_run_failed"


def check_execution(go, java_worker, remote_go, remote_java, document, expected):
    """Persist a real first step, lose the second worker, then reopen and recover."""
    sibling = AnnotationDocument("untouched.sibling", "exact sibling\r\n🙂")
    event = AnnotationEvent("durable.event", (document, sibling), {"flag": True, "number": 1})
    plan = {document.document_id: RemoteAnnotationPipeline("demo.chain", "1", (remote_java, remote_go))}
    real_execute = RemoteAnnotationProcessor.execute
    successful_calls = []
    stop_after_first = True

    def observed_execute(worker, request):
        nonlocal stop_after_first
        response = real_execute(worker, request)
        successful_calls.append(worker.description.name)
        if stop_after_first and worker.description.name == "demo.go.tokens":
            stop_after_first = False
            java_worker.close()  # Actual subsequent HTTP failure, not a fake OCR/NLP response.
        return response

    with tempfile.TemporaryDirectory(prefix="corpusledger-execution-check-") as directory:
        database = Path(directory) / "events.sqlite"
        with patch.object(RemoteAnnotationProcessor, "execute", observed_execute):
            with AnnotationStore(database) as store:
                source = store.put(event)
                store.enable_execution_journal()
                executor = AnnotationExecutor(store)
                begun = executor.begin(
                    "durable.op", event.event_id, plan, expected_revision=source.revision, expected_digest=source.digest
                )
                assert begun.status == "ready" and begun.completed_steps == 0
                try:
                    executor.resume("durable.op", plan)
                except AnnotationExecutionUncertain:
                    pass
                else:
                    raise AssertionError("stopped second worker was not reported uncertain")
                pending = executor.get("durable.op")
                assert pending.status == "uncertain" and pending.completed_steps == 1
                assert store.get(event.event_id).digest == source.digest
                assert store.get(event.event_id).event.digest == event.digest
            java_worker.restart()
            assert java_worker.description == remote_java.description
            with AnnotationStore(database, create=False) as store:
                executor = AnnotationExecutor(store)
                assert store.execution_enabled
                try:
                    executor.resume("durable.op", plan)
                except AnnotationExecutionUncertain:
                    pass
                else:
                    raise AssertionError("uncertain work replayed without acknowledgement")
                assert successful_calls == ["demo.go.tokens"]
                committed = executor.resume("durable.op", plan, retry_uncertain=True)
                assert committed.status == "committed" and committed.completed_steps == 2
                revision = store.get(event.event_id)
                assert revision.revision == 2 and revision.parent_digest == source.digest
                assert committed.to_dict()["result"] == {"revision": 2, "digest": revision.digest}
                assert revision.event.get_document(document.document_id).digest == expected.digest
                assert revision.event.get_document(sibling.document_id).digest == sibling.digest
                assert encode_wire(revision.event.get_document(sibling.document_id).to_dict()) == encode_wire(
                    sibling.to_dict()
                )
                assert store.get(event.event_id, 1).event.digest == event.digest
                assert [item.revision for item in store.history(event.event_id)] == [1, 2]
                assert successful_calls == ["demo.go.tokens", "demo.java.group"]
                before = committed.to_dict()
                with patch.object(
                    RemoteAnnotationProcessor, "verify", side_effect=AssertionError("unexpected reverify")
                ):
                    assert (
                        executor.begin(
                            "durable.op",
                            event.event_id,
                            plan,
                            expected_revision=source.revision,
                            expected_digest=source.digest,
                        ).to_dict()
                        == before
                    )
                    assert executor.resume("durable.op", plan).to_dict() == before
                assert len(store.history(event.event_id)) == 2
                serialized = json.dumps(before, sort_keys=True)
                assert go.token not in serialized and java_worker.token not in serialized
                return {
                    "status": committed.status,
                    "revision": revision.revision,
                    "attempts": before["attempts"],
                    "successful_worker_calls": successful_calls,
                    "source_event_digest": event.digest,
                    "final_event_digest": revision.event.digest,
                }


def verify(build_dir: Path) -> dict[str, object]:
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
    for command in commands:
        check_startup_rejection(command)
    with ExitStack() as stack:
        go, java_worker = (stack.enter_context(_worker_context(command)) for command in commands)
        seed_type = AnnotationType("seed", {"big": AnnotationField("integer")})
        seed = SpanAnnotation("seed.0", "seed", 0, 0, {"big": 10**1000 + 123})
        document = AnnotationDocument("unicode", "A😀 e\u0301\r\n\t終\u00a0Z", (seed_type,), (seed,))
        original = document.to_dict()
        tokens, go_request = go.apply(document, "go")
        grouped, java_request = java_worker.apply(tokens, "java")
        with patch.dict(
            os.environ, {"CORPUSLEDGER_VERIFY_GO": go.token, "CORPUSLEDGER_VERIFY_JAVA": java_worker.token}
        ):
            remote_go = RemoteAnnotationProcessor(go.address.geturl(), go.description, "CORPUSLEDGER_VERIFY_GO")
            remote_java = RemoteAnnotationProcessor(
                java_worker.address.geturl(), java_worker.description, "CORPUSLEDGER_VERIFY_JAVA"
            )
            # Reverse registration deliberately: the declared token dependency,
            # not caller listing order, must determine actual execution order.
            pipeline = AnnotationPipeline(
                [remote_java.as_processor("integrated", "java"), remote_go.as_processor("integrated", "go")]
            )
            integrated = pipeline.run(document)
            assert integrated.document.digest == grouped.digest
            assert encode_wire(integrated.document.to_dict()) == encode_wire(grouped.to_dict())
            assert [step.name for step in integrated.steps] == ["demo.go.tokens", "demo.java.group"]
            execution = check_execution(go, java_worker, remote_go, remote_java, document, grouped)
        annotations = [a for a in grouped.annotations if a.type_name == "token"]
        assert [(a.start, a.end) for a in annotations] == [(0, 2), (3, 5), (8, 9), (10, 11)]
        assert [a.features["text"] for a in annotations] == ["A😀", "e\u0301", "終", "Z"]
        group = grouped.get("demo.java.group.0")
        assert tuple(group.features["members"]) == tuple(a.annotation_id for a in annotations)
        assert group.features["count"] == 4 and (group.start, group.end) == (0, 11)
        assert grouped.get("seed.0").features["big"] == 10**1000 + 123
        assert document.to_dict() == original and grouped.text == document.text
        for text in ("", " \t\r\n\u00a0"):
            empty, _ = go.apply(AnnotationDocument("empty", text), "empty.go")
            empty_grouped, _ = java_worker.apply(empty, "empty.java")
            empty_group = empty_grouped.get("demo.java.group.0")
            assert (empty_group.start, empty_group.end, empty_group.features["count"]) == (0, 0, 0)
            assert empty_group.features["members"] == ()
        # Adjacent integers beyond binary64/uint64 retain distinct sort positions.
        changed = tuple(
            SpanAnnotation(a.annotation_id, a.type_name, a.start, a.end, {**a.features, "position": 2**200 + i})
            if a.type_name == "token"
            else a
            for i, a in enumerate(tokens.annotations)
        )
        large, _ = java_worker.apply(
            AnnotationDocument(tokens.document_id, tokens.text, tokens.annotation_types, changed), "big.positions"
        )
        assert large.get("demo.java.group.0").features["members"] == group.features["members"]
        many_tokens, _ = go.apply(AnnotationDocument("many.supplementary", "😀x " * 5000), "many.go")
        many_groups, _ = java_worker.apply(many_tokens, "many.java")
        many_group = many_groups.get("demo.java.group.0")
        assert many_group.features["count"] == 5000
        assert (many_group.start, many_group.end) == (0, 14999)
        for worker, request in ((go, go_request), (java_worker, java_request)):
            check_rejection(worker, request)
            check_slow_request(worker)
            assert worker.request("GET", "/v1/info")[0] == 200
        return {
            "kind": "checked-in-cross-language-contract-test",
            "passed": True,
            "chain": [go.description.to_dict(), java_worker.description.to_dict()],
            "source_digest": document.digest,
            "result_digest": grouped.digest,
            "durable_execution": execution,
            "checks": [
                "Python-Go-Java",
                "RemoteAnnotationProcessor-dependency-pipeline",
                "SQLite-v2-real-worker-loss-and-explicit-recovery",
                "atomic-event-commit-and-operation-idempotence",
                "Unicode-codepoints",
                "empty-anchor",
                "1001-digit-integer",
                "above-binary64-position-order",
                "5000-supplementary-token-spans",
                "schema-and-identity-rejection",
                "strict-JSON",
                "authentication",
                "missing-token-and-nonloopback-startup-rejection",
                "partial-body-timeout",
                "source-unchanged",
            ],
        }


@contextmanager
def _worker_context(command):
    worker = Worker(command)
    try:
        yield worker
    finally:
        worker.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.build_dir), sort_keys=True))


if __name__ == "__main__":
    main()
