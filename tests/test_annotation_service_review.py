"""Independent regressions for client selection and backend/CLI error boundaries."""

from __future__ import annotations

import hashlib
import http.client
import json
import socket
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from corpusledger import AnnotationDocument, AnnotationType
from corpusledger.annotation_client import AnnotationClient, AnnotationClientError
from corpusledger.annotation_protocol import ProcessorDescription
from corpusledger.annotation_service import CONFIG_FORMAT, create_annotation_server, main
from corpusledger.annotation_store import AnnotationEvent, AnnotationStore


def operation() -> dict[str, Any]:
    first = ProcessorDescription("first", "1", "a" * 64, produces=(AnnotationType("first"),))
    second = ProcessorDescription(
        "second", "1", "b" * 64, requires=(AnnotationType("first"),), produces=(AnnotationType("second"),)
    )
    request = {
        "event_id": "event",
        "expected_revision": 1,
        "expected_digest": "c" * 64,
        "steps": [
            {
                "step_id": f"s{index:03d}",
                "document_id": "doc",
                "pipeline_id": "pipeline",
                "pipeline_version": "1",
                "worker": {"processor": description.to_dict(), "endpoint_sha256": "d" * 64},
            }
            for index, description in enumerate((first, second))
        ],
    }
    return {
        "format": "corpusledger.annotation-operation.v1",
        "operation_id": "operation",
        "version": 1,
        "request": request,
        "request_digest": checksum(request),
        "status": "ready",
        "completed": [],
        "attempts": [0, 0],
        "reservation": None,
        "error": None,
        "result": None,
    }


def checksum(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@pytest.mark.parametrize(
    "invoke",
    [
        lambda client: client.get(None),
        lambda client: client.history(None),
        lambda client: client.history("event", after_revision=None),
        lambda client: client.status(None),
        lambda client: client.operation_history(None),
        lambda client: client.operation_history("operation", after_version=None),
        lambda client: client.begin(None, "event", {"doc": "pipeline"}, expected_revision=1, expected_digest="c" * 64),
        lambda client: client.begin(
            "operation", None, {"doc": "pipeline"}, expected_revision=1, expected_digest="c" * 64
        ),
        lambda client: client.begin(
            "operation", "event", {"doc": "pipeline"}, expected_revision=None, expected_digest="c" * 64
        ),
    ],
)
def test_required_null_arguments_are_rejected_before_transport(
    monkeypatch: pytest.MonkeyPatch, invoke: Callable[[AnnotationClient], Any]
) -> None:
    def forbidden(*_args: Any) -> Any:
        raise AssertionError("invalid required-null argument reached transport")

    monkeypatch.setattr(AnnotationClient, "_exchange", forbidden)
    with pytest.raises(AnnotationClientError):
        invoke(AnnotationClient("http://127.0.0.1:43120", "CLIENT_TOKEN"))


@pytest.mark.parametrize("command", ["begin", "resume"])
def test_every_step_must_match_selected_pipeline_not_just_last_step(
    monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    value = operation()
    value["request"]["steps"][0]["pipeline_id"] = "unrequested"
    value["request_digest"] = checksum(value["request"])
    monkeypatch.setattr(AnnotationClient, "_exchange", lambda *_args: value)
    client = AnnotationClient("http://127.0.0.1:43120", "CLIENT_TOKEN")
    with pytest.raises(AnnotationClientError, match="response_invalid"):
        if command == "begin":
            client.begin("operation", "event", {"doc": "pipeline"}, expected_revision=1, expected_digest="c" * 64)
        else:
            client.resume("operation", {"doc": "pipeline"})


def test_sqlite_busy_is_redacted_unavailable_not_invalid_user_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SERVICE_REVIEW_TOKEN", "review-test-secret-not-production")
    path = tmp_path / "events.sqlite"
    with create_annotation_server(path, {}, token_env="SERVICE_REVIEW_TOKEN", request_timeout=2) as server:

        class ImmediateStore(AnnotationStore):
            def __init__(self, path: str | Path, *, create: bool = True) -> None:
                super().__init__(path, create=create, timeout=0)

        monkeypatch.setattr("corpusledger.annotation_service.AnnotationStore", ImmediateStore)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        client = AnnotationClient(server.endpoint, "SERVICE_REVIEW_TOKEN", timeout=2)
        event = AnnotationEvent("event", (AnnotationDocument("doc", "payload"),))
        try:
            with closing(sqlite3.connect(path)) as writer:
                writer.execute("BEGIN IMMEDIATE")
                with pytest.raises(AnnotationClientError) as caught:
                    client.create(event)
                assert caught.value.code == "unavailable"
                assert "locked" not in str(caught.value)
                writer.rollback()
            assert client.list() == ()
            assert client.create(event).event.digest == event.digest
        finally:
            server.shutdown()
            thread.join(timeout=3)
            assert not thread.is_alive()


def test_duplicate_worker_configuration_fails_cli_cleanly_before_store_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SERVICE_REVIEW_TOKEN", "review-test-secret-not-production")
    worker = {
        "endpoint": "http://127.0.0.1:43120",
        "description": ProcessorDescription("worker", "1", "a" * 64, produces=(AnnotationType("token"),)).to_dict(),
        "token_env": "WORKER_REVIEW_TOKEN",
        "timeout": 1,
        "max_response_bytes": 4096,
    }
    config = tmp_path / "configuration.json"
    config.write_text(
        json.dumps(
            {"format": CONFIG_FORMAT, "pipelines": [{"id": "pipeline", "version": "1", "workers": [worker, worker]}]}
        ),
        encoding="utf-8",
    )
    path = tmp_path / "must-not-create.sqlite"
    assert main(["--store", str(path), "--config", str(config), "--token-env", "SERVICE_REVIEW_TOKEN"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "annotation service configuration or store is unavailable\n"
    assert not path.exists()


@pytest.mark.parametrize("authority", ["127.0.0.1:80", "[::1]:80"])
def test_default_port_host_header_matches_canonical_service_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, authority: str
) -> None:
    monkeypatch.setenv("SERVICE_REVIEW_TOKEN", "review-test-secret-not-production")
    with create_annotation_server(tmp_path / "events.sqlite", {}, token_env="SERVICE_REVIEW_TOKEN") as server:
        # Keep actual traffic on an OS-selected unprivileged IPv4 port, while
        # preserving HTTPConnection's real default-port Host generation rules.
        # No dependency on an available/bindable port80 or IPv6 interface.
        physical_address = ("127.0.0.1", server.server_address[1])
        server.authority = authority

        class RedirectConnection(http.client.HTTPConnection):
            def connect(self) -> None:
                self.sock = socket.create_connection(physical_address, timeout=self.timeout)

        monkeypatch.setattr("corpusledger.annotation_client.http.client.HTTPConnection", RedirectConnection)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        thread.start()
        try:
            client = AnnotationClient(f"http://{authority}", "SERVICE_REVIEW_TOKEN", timeout=2)
            original = AnnotationEvent("event", (AnnotationDocument("doc", "default-port request"),))
            assert client.create(original).event.digest == original.digest
        finally:
            server.shutdown()
            thread.join(timeout=3)
            assert not thread.is_alive()
