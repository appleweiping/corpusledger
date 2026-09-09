"""Real loopback HTTP framing, durable operation recovery and typed-client tests."""

from __future__ import annotations

import copy
import json
import socket
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pytest

from corpusledger.annotation_client import AnnotationClient, AnnotationClientError
from corpusledger.annotation_execution import RemoteAnnotationPipeline
from corpusledger.annotation_protocol import MAX_WIRE_BYTES, AnnotationResponse, ProcessorDescription, encode_wire
from corpusledger.annotation_remote import RemoteAnnotationProcessor
from corpusledger.annotation_service import (
    COMMAND_FORMAT,
    CONFIG_FORMAT,
    RESPONSE_FORMAT,
    AnnotationServer,
    _registry,
    create_annotation_server,
    main,
)
from corpusledger.annotation_store import AnnotationEvent, AnnotationStore
from corpusledger.annotations import AnnotationDocument, AnnotationType, SpanAnnotation
from corpusledger.errors import InputError

TOKEN_ENV = "CORPUSLEDGER_TEST_EVENT_TOKEN"
TOKEN = "only-a-local-test-secret-3498"


@pytest.fixture(autouse=True)
def credential(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, TOKEN)


def source(name: str = "event") -> AnnotationEvent:
    return AnnotationEvent(
        name,
        (AnnotationDocument("original", "😀 x\r\n雪"), AnnotationDocument("sibling", "KEEP")),
        {"typed": [True, 1, 1.0]},
    )


def registry() -> dict[str, RemoteAnnotationPipeline]:
    description = ProcessorDescription("span", "1", "a" * 64, produces=(AnnotationType("span"),))
    worker = RemoteAnnotationProcessor("http://127.0.0.1:49998", description, TOKEN_ENV)
    return {"spans": RemoteAnnotationPipeline("spans", "1", (worker,))}


@contextmanager
def running(path: Path, pipelines: Any = None, **kwargs: Any) -> Iterator[AnnotationServer]:
    server = create_annotation_server(path, {} if pipelines is None else pipelines, token_env=TOKEN_ENV, **kwargs)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def command(name: str = "list", arguments: Any = None) -> bytes:
    return encode_wire(
        {
            "format": COMMAND_FORMAT,
            "command": name,
            "arguments": {"after_event_id": None, "limit": 100} if arguments is None else arguments,
        }
    )


def request(
    server: AnnotationServer, body: bytes | None = None, *, headers: bytes = b"", path: bytes = b"/v1/events"
) -> bytes:
    payload = command() if body is None else body
    return (
        b"POST "
        + path
        + b" HTTP/1.1\r\nHost: "
        + server.authority.encode()
        + b"\r\nAuthorization: Bearer "
        + TOKEN.encode()
        + b"\r\nContent-Type: application/json\r\nContent-Length: "
        + str(len(payload)).encode()
        + b"\r\n"
        + headers
        + b"\r\n"
        + payload
    )


def raw(server: AnnotationServer, data: bytes, *, finish: bool = False) -> tuple[int, Any]:
    with socket.create_connection(server.server_address[:2], timeout=2) as connection:
        connection.sendall(data)
        if finish:
            connection.shutdown(socket.SHUT_WR)
        result = bytearray()
        while True:
            try:
                chunk = connection.recv(65536)
            except ConnectionResetError:
                break
            if not chunk:
                break
            result.extend(chunk)
    header, body = bytes(result).split(b"\r\n\r\n", 1)
    assert b"Access-Control" not in header
    assert TOKEN.encode() not in result
    return int(header.split(b" ")[1]), json.loads(body)


@contextmanager
def peer(callback: Callable[[socket.socket], None]) -> Iterator[str]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(2)
    sockets: list[socket.socket] = []

    def run() -> None:
        try:
            connection, _ = listener.accept()
            sockets.append(connection)
            with connection:
                connection.settimeout(2)
                callback(connection)
        except OSError:
            return

    thread = threading.Thread(target=run)
    thread.start()
    try:
        yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        listener.close()
        for connection in sockets:
            with suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            connection.close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def response(body: Any, *, status: int = 200, headers: bytes = b"") -> bytes:
    data = encode_wire(body)
    return (
        f"HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n".encode()
        + headers
        + b"\r\n"
        + data
    )


def answer_server(data: bytes) -> Callable[[socket.socket], None]:
    def answer(connection: socket.socket) -> None:
        with connection.makefile("rb") as incoming:
            incoming.readline(8192)
            headers = {}
            while header := incoming.readline(8192).strip():
                name, value = header.split(b":", 1)
                headers[name.lower()] = value.strip()
            incoming.read(int(headers[b"content-length"]))
        connection.sendall(data)

    return answer


def successful(name: str, value: Any) -> dict[str, Any]:
    return {"format": RESPONSE_FORMAT, "command": name, "ok": True, "result": value}


def test_events_exact_revisions_pagination_and_explicit_migration(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite"
    with running(path) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        first = client.create(source("a"))
        assert first.event.digest == source("a").digest
        client.create(source("b"))
        assert client.get("a").digest == first.digest
        assert client.get("a", 1).event.to_dict() == source("a").to_dict()
        assert [item.event_id for item in client.list(limit=1)] == ["a"]
        assert [item.event_id for item in client.list(after_event_id="a")] == ["b"]
        with AnnotationStore(path, create=False) as store:
            assert not store.execution_enabled
            store.put(AnnotationEvent("a", source("a").documents, {"revision": 2}), expected_revision=1)
        history = client.history("a")
        assert [item.revision for item in history] == [1, 2]
        assert history[1].parent_digest == history[0].digest
        assert len(client.history("a", after_revision=1)) == 1
        for call, code in (
            (lambda: client.create(source("a")), "conflict"),
            (lambda: client.get("missing"), "not_found"),
            (lambda: client.status("op"), "execution_disabled"),
        ):
            with pytest.raises(AnnotationClientError) as caught:
                call()
            assert caught.value.code == code
    with running(path, enable_execution_journal=True) as server:
        assert AnnotationClient(server.endpoint, TOKEN_ENV).get("a").revision == 2
        with AnnotationStore(path, create=False) as store:
            assert store.execution_enabled


@pytest.fixture
def workers(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", lambda _: None)

    def execute(worker: RemoteAnnotationProcessor, incoming: Any) -> AnnotationResponse:
        calls.append(incoming.operation_id)
        return AnnotationResponse(
            incoming.operation_id,
            incoming.step_id,
            worker.description,
            incoming.document.digest,
            (SpanAnnotation("whole", "span", 0, len(incoming.document.text)),),
        )

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", execute)
    return calls


def test_restart_resume_and_committed_binding(tmp_path: Path, workers: list[str]) -> None:
    path = tmp_path / "events.sqlite"
    with running(path, registry(), enable_execution_journal=True) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        revision = client.create(source())
        op = client.begin("op", "event", {"original": "spans"}, expected_revision=1, expected_digest=revision.digest)
        assert op.status == "ready" and not workers
        assert client.status("op").to_dict() == op.to_dict()
    with running(path, registry()) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        complete = client.resume("op", {"original": "spans"})
        assert complete.status == "committed" and complete.completed_steps == 1
        assert workers == ["op"]
        assert client.get("event").event.get_document("sibling").digest == source().get_document("sibling").digest
        assert client.get("event").revision == 2
        assert client.resume("op", {"original": "spans"}).to_dict() == complete.to_dict()
        assert (
            client.begin(
                "op", "event", {"original": "spans"}, expected_revision=1, expected_digest=revision.digest
            ).to_dict()
            == complete.to_dict()
        )
        assert len(workers) == 1
        assert client.operations()[0].operation_id == "op"
        assert not client.operations(after_operation_id="op")
        history = client.operation_history("op")
        assert [item.status for item in history] == ["ready", "reserved", "ready", "committed"]
        assert client.operation_history("op", after_version=2, limit=1)[0].version == 3
        with pytest.raises(AnnotationClientError, match="conflict"):
            client.resume("op", {"sibling": "spans"})
        with pytest.raises(AnnotationClientError, match="request_invalid"):
            client.begin(
                "unknown", "event", {"original": "not-registered"}, expected_revision=1, expected_digest=revision.digest
            )
        with pytest.raises(AnnotationClientError, match="not_found"):
            client.status("unknown")
        with pytest.raises(AnnotationClientError, match="conflict"):
            client.begin("different", "event", {"original": "spans"}, expected_revision=1, expected_digest="b" * 64)


def test_uncertain_response_requires_explicit_retry(
    tmp_path: Path, workers: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    good = RemoteAnnotationProcessor.execute

    def fail(*_: Any) -> None:
        raise RuntimeError("SECRET worker detail")

    with running(tmp_path / "store", registry(), enable_execution_journal=True) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        revision = client.create(source())
        client.begin("op", "event", {"original": "spans"}, expected_revision=1, expected_digest=revision.digest)
        monkeypatch.setattr(RemoteAnnotationProcessor, "execute", fail)
        with pytest.raises(AnnotationClientError, match="uncertain") as caught:
            client.resume("op", {"original": "spans"})
        assert "SECRET" not in "".join(traceback.format_exception(caught.value))
        assert client.status("op").status == "uncertain"
        monkeypatch.setattr(RemoteAnnotationProcessor, "execute", good)
        with pytest.raises(AnnotationClientError, match="uncertain"):
            client.resume("op", {"original": "spans"})
        assert not workers and client.get("event").revision == 1
        assert client.resume("op", {"original": "spans"}, retry_uncertain=True).status == "committed"


@pytest.mark.parametrize(
    "header",
    [
        b"Content-Length: 1\r\n",
        b"Transfer-Encoding: chunked\r\n",
        b"Content-Encoding: identity\r\n",
        b"Origin: http://localhost\r\n",
        b"Origin: null\r\n",
        b"Host: attacker\r\n",
        b"Expect: 100-continue\r\n",
        b"Proxy-Authorization: secret\r\n",
        b"Proxy-Connection: close\r\n",
        b"Authorization: Bearer other\r\n",
        b" folded: value\r\n",
        b"badheader\r\n",
        b"Bad Name: value\r\n",
        b"X: value\x00\r\n",
        b"X: \xff\r\n",
        b"Content-Type: application/json\r\n",
        b"X: a\r\nx: b\r\n",
        b"X: " + b"a" * 8192 + b"\r\n",
        b"".join(f"X-{i}: a\r\n".encode() for i in range(33)),
    ],
)
def test_rejects_ambiguous_or_browser_headers(tmp_path: Path, header: bytes) -> None:
    with running(tmp_path / "store") as server:
        status, value = raw(server, request(server, headers=header))
        assert status == 400 and value["error"]["code"] == "request_invalid"


@pytest.mark.parametrize(
    "change",
    [
        lambda data: data.replace(b"POST /v1/events", b"GET /v1/events"),
        lambda data: data.replace(b"POST /v1/events", b"POST http://127.0.0.1/v1/events"),
        lambda data: data.replace(b"HTTP/1.1", b"HTTP/1.0"),
        lambda data: data.replace(b"\r\n", b"\n"),
        lambda data: data.replace(b"Content-Type: application/json", b"Content-Type: application /json"),
        lambda data: data.replace(b"Content-Length: ", b"Content-Length: +"),
        lambda data: data.replace(b"Host: ", b"Host: localhost:"),
    ],
)
def test_bad_request_target_and_fields(tmp_path: Path, change: Any) -> None:
    with running(tmp_path / "store") as server:
        assert raw(server, change(request(server)), finish=True)[0] == 400


@pytest.mark.parametrize(
    "payload",
    [
        b'{"format":1,"format":2}',
        b"NaN",
        b"{}",
        b"[]",
        b"\xff",
        command("unknown", {}),
        command("list", {"limit": 1}),
        command("list", {"limit": 1, "after_event_id": None, "path": "SECRET"}),
        command("get", {"event_id": "event", "revision": True}),
        command("list", {"limit": True, "after_event_id": None}),
    ],
)
def test_closed_json_and_argument_contract(tmp_path: Path, payload: bytes) -> None:
    with running(tmp_path / "store") as server:
        status, value = raw(server, request(server, payload))
        assert status == 400 and "SECRET" not in json.dumps(value)


def test_auth_rotation_missing_credentials_and_no_proxy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
        monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
        assert client.list() == ()
        assert raw(server, request(server).replace(TOKEN.encode(), b"wrong"))[0] == 401
        assert raw(server, request(server).replace(b"Authorization:", b"Unused:"))[0] == 401
        monkeypatch.delenv(TOKEN_ENV)
        assert raw(server, request(server))[0] == 401
        with pytest.raises(AnnotationClientError):
            client.list()
        monkeypatch.setenv(TOKEN_ENV, "rotated-private-token-23984")
        assert client.list() == ()


def test_request_and_response_limits_do_not_publish_create(tmp_path: Path) -> None:
    with running(tmp_path / "store", max_wire_bytes=1024) as server:
        assert raw(server, request(server, b"x" * 1025))[0] == 413
        # Input fits, but wrapping the materialized revision would not.
        event = AnnotationEvent("event", (AnnotationDocument("doc", "x" * 600),))
        body = command("create", {"event": event.to_dict()})
        assert len(body) < 1024
        assert raw(server, request(server, body))[0] == 413
        assert AnnotationClient(server.endpoint, TOKEN_ENV).list() == ()
        with AnnotationStore(server.store_path, create=False) as store:
            store.put(event)
        with pytest.raises(AnnotationClientError, match="too_large"):
            AnnotationClient(server.endpoint, TOKEN_ENV).get("event")


def test_short_body_and_absolute_header_and_body_deadlines(tmp_path: Path) -> None:
    with running(tmp_path / "store", request_timeout=0.12) as server:
        assert raw(server, request(server)[:-3], finish=True)[0] == 400
        for initial in (b"P", request(server).split(b"\r\n\r\n")[0] + b"\r\n\r\n{"):
            started = time.monotonic()
            with socket.create_connection(server.server_address[:2], timeout=2) as connection:
                connection.sendall(initial)
                for _ in range(12):
                    time.sleep(0.02)
                    try:
                        connection.sendall(b" ")
                    except OSError:
                        break
                with suppress(OSError):
                    assert connection.recv(4096) == b""
            assert time.monotonic() - started < 1
    assert not any(t.name == "corpusledger-event-deadline" and t.is_alive() for t in threading.enumerate())


def test_admission_rejects_excess_and_shutdown_closes_idle_connection(tmp_path: Path) -> None:
    with running(tmp_path / "store", max_connections=1, request_timeout=2) as server:
        first = socket.create_connection(server.server_address[:2], timeout=2)
        first.sendall(b"P")
        deadline = time.monotonic() + 1
        while not server._active and time.monotonic() < deadline:
            time.sleep(0.005)
        assert len(server._active) == 1
        with socket.create_connection(server.server_address[:2], timeout=2) as second:
            assert second.recv(1) == b""
        server.shutdown()
        server.server_close()
        assert first.recv(1) == b""
        first.close()
        assert not server._active


def test_concurrent_create_uses_independent_connections(tmp_path: Path) -> None:
    with running(tmp_path / "store") as server:

        def create() -> str:
            try:
                AnnotationClient(server.endpoint, TOKEN_ENV).create(source())
                return "created"
            except AnnotationClientError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: create(), range(4)))
        assert results.count("created") == 1 and results.count("conflict") == 3


@pytest.mark.parametrize(
    "options",
    [
        {"host": "0.0.0.0"},
        {"host": "localhost"},
        {"host": 123},
        {"host": True},
        {"host": "::1%zone"},
        {"port": True},
        {"port": -1},
        {"port": 65536},
        {"request_timeout": True},
        {"request_timeout": float("nan")},
        {"request_timeout": 10**400},
        {"max_connections": 0},
        {"max_connections": True},
        {"max_wire_bytes": True},
        {"max_wire_bytes": MAX_WIRE_BYTES + 1},
        {"enable_execution_journal": 1},
    ],
)
def test_invalid_config_does_not_create_database(tmp_path: Path, options: Any) -> None:
    with pytest.raises(InputError):
        create_annotation_server(tmp_path / "absent", {}, token_env=TOKEN_ENV, **options)
    assert not (tmp_path / "absent").exists()


def test_port_conflict_does_not_migrate_store(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with AnnotationStore(path):
        pass
    with running(tmp_path / "other") as server, pytest.raises(OSError):
        create_annotation_server(
            path, {}, token_env=TOKEN_ENV, port=server.server_address[1], enable_execution_journal=True
        )
    with AnnotationStore(path, create=False) as store:
        assert not store.execution_enabled


@pytest.mark.parametrize(
    "data",
    [
        response(successful("list", []), status=302, headers=b"Location: http://127.0.0.1:1\r\n"),
        response(successful("list", []), headers=b"Content-Length: 3\r\n"),
        response(successful("list", []), headers=b"Transfer-Encoding: chunked\r\n"),
        response(successful("list", []), headers=b"Content-Encoding: identity\r\n"),
        response(successful("list", []), headers=b"malformed private SECRET\r\n"),
        b"SECRET bad status\r\n\r\n",
        response(successful("list", [])).replace(b"application/json", b"application /json"),
        response(successful("list", []))[:-2],
        response(successful("list", [])).replace(b"Content-Length: ", b"Content-Length: 999999999"),
        response({"format": RESPONSE_FORMAT, "command": "list", "ok": 1, "result": []}),
        response({**successful("list", []), "SECRET": True}),
        response(successful("get", [])),
        response({**successful("list", []), "format": "wrong"}),
        response({"format": RESPONSE_FORMAT, "command": "list", "ok": False, "error": {"code": "SECRET"}}, status=400),
        response({"format": RESPONSE_FORMAT, "command": "get", "ok": False, "error": {"code": "conflict"}}, status=409),
    ],
)
def test_client_rejects_malformed_transport_without_secret_chains(data: bytes) -> None:
    with peer(answer_server(data)) as endpoint:
        with pytest.raises(AnnotationClientError) as caught:
            AnnotationClient(endpoint, TOKEN_ENV).list()
        rendered = "".join(traceback.format_exception(caught.value))
        assert "SECRET" not in rendered and TOKEN not in rendered


@pytest.mark.parametrize("stage", ["header", "body", "send"])
def test_client_absolute_deadline_no_timer_leak(stage: str) -> None:
    def slow(connection: socket.socket) -> None:
        if stage == "send":
            time.sleep(0.4)
            return
        connection.recv(65536)
        data = response(successful("list", []))
        if stage == "body":
            headers, body = data.split(b"\r\n\r\n", 1)
            connection.sendall(headers + b"\r\n\r\n")
            data = body
        for byte in data:
            connection.sendall(bytes([byte]))
            time.sleep(0.025)

    with peer(slow) as endpoint:
        client = AnnotationClient(endpoint, TOKEN_ENV, timeout=0.1)
        started = time.monotonic()
        with pytest.raises(AnnotationClientError):
            if stage == "send":
                client.create(AnnotationEvent("event", (AnnotationDocument("doc", "x" * (8 * 1024 * 1024)),)))
            else:
                client.list()
        assert time.monotonic() - started < 2
    assert not any(t.name == "corpusledger-event-client-deadline" and t.is_alive() for t in threading.enumerate())


def test_client_connection_refused() -> None:
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        endpoint = f"http://127.0.0.1:{reserved.getsockname()[1]}"
    with pytest.raises(AnnotationClientError, match="transport_failed"):
        AnnotationClient(endpoint, TOKEN_ENV, timeout=0.2).list()


@pytest.mark.parametrize(
    "call",
    [
        lambda c: c.get("event", True),
        lambda c: c.list(limit=True),
        lambda c: c.history("event", after_revision=True),
        lambda c: c.operations(limit=0),
        lambda c: c.operation_history("op", after_version=-1),
        lambda c: c.resume("op", {"doc": "spans"}, retry_uncertain=1),
        lambda c: c.resume("op", {}),
        lambda c: c.resume("op", []),
        lambda c: c.begin("op", "event", [], expected_revision=1, expected_digest="a" * 64),
        lambda c: c.create({}),
        lambda c: c.get(""),
        lambda c: c.operations(after_operation_id="bad ID"),
        lambda c: c.begin("op", "event", {"doc": "spans"}, expected_revision=True, expected_digest="a" * 64),
        lambda c: c.begin("op", "event", {"doc": "spans"}, expected_revision=1, expected_digest="bad"),
    ],
)
def test_client_invalid_typed_arguments_rejected_before_network(call: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_: Any) -> None:
        pytest.fail("invalid arguments reached HTTP")

    monkeypatch.setattr(AnnotationClient, "_exchange", forbidden)
    with pytest.raises(AnnotationClientError):
        call(AnnotationClient("http://127.0.0.1:1", TOKEN_ENV))


def test_client_create_rejects_self_consistent_bool_integer_substitution(tmp_path: Path) -> None:
    sent = AnnotationEvent("event", metadata={"flag": True})
    changed = AnnotationEvent("event", metadata={"flag": 1})
    assert sent == changed  # Why dataclass/Python equality is insufficient here.
    with AnnotationStore(tmp_path / "other") as store:
        altered = store.put(changed).to_dict()
    with (
        peer(answer_server(response(successful("create", altered)))) as endpoint,
        pytest.raises(AnnotationClientError, match="response_invalid"),
    ):
        AnnotationClient(endpoint, TOKEN_ENV).create(sent)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra=True),
        lambda value: value.update(format="wrong"),
        lambda value: value.update(revision=True),
        lambda value: value.update(parent_digest="a" * 64),
        lambda value: value.update(documents=["sibling", "original"]),
        lambda value: value.update(documents=["original"]),
        lambda value: value.update(provenance=[]),
        lambda value: value.update(digest="a" * 64),
        lambda value: value["event"].update(id="different"),
        lambda value: value["event"].update(metadata={"changed": True}),
    ],
)
def test_client_validates_materialized_revision(tmp_path: Path, mutate: Any) -> None:
    with AnnotationStore(tmp_path / "store") as store:
        value = store.put(source()).to_dict()
    mutate(value)
    with (
        peer(answer_server(response(successful("get", value)))) as endpoint,
        pytest.raises(AnnotationClientError, match="response_invalid"),
    ):
        AnnotationClient(endpoint, TOKEN_ENV).get("event")


def test_cli_config_is_closed_and_alias_safe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    pipeline = registry()["spans"]
    worker = pipeline.processors[0]
    config = {
        "format": CONFIG_FORMAT,
        "pipelines": [
            {
                "id": "spans",
                "version": "1",
                "workers": [
                    {
                        "endpoint": worker.endpoint,
                        "description": worker.description.to_dict(),
                        "token_env": TOKEN_ENV,
                        "timeout": 10,
                        "max_response_bytes": MAX_WIRE_BYTES,
                    }
                ],
            }
        ],
    }
    assert _registry(config) == registry()
    invalid = copy.deepcopy(config)
    invalid["pipelines"][0]["workers"][0]["code"] = "SECRET"
    with pytest.raises(InputError):
        _registry(invalid)
    config_path = tmp_path / "service.json"
    config_path.write_bytes(encode_wire(config))
    assert main(["--store", str(config_path), "--config", str(config_path), "--token-env", TOKEN_ENV]) == 2
    assert config_path.read_bytes() == encode_wire(config)
    alias = tmp_path / "alias"
    alias.hardlink_to(config_path)
    assert main(["--store", str(alias), "--config", str(config_path), "--token-env", TOKEN_ENV]) == 2
    monkeypatch.setattr(AnnotationServer, "serve_forever", lambda *a, **kw: None)
    assert (
        main(
            [
                "--store",
                str(tmp_path / "db"),
                "--config",
                str(config_path),
                "--token-env",
                TOKEN_ENV,
                "--enable-execution-journal",
            ]
        )
        == 0
    )
    assert "http://127.0.0.1:" in capsys.readouterr().out


def test_pipeline_validation_errors_are_client_errors(tmp_path: Path, workers: list[str]) -> None:
    already = AnnotationEvent("event", (AnnotationDocument("original", "text", (AnnotationType("span"),)),))
    with running(tmp_path / "store", registry(), enable_execution_journal=True) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        revision = client.create(already)
        with pytest.raises(AnnotationClientError, match="request_invalid"):
            client.begin("op", "event", {"original": "spans"}, expected_revision=1, expected_digest=revision.digest)
        assert workers == []
