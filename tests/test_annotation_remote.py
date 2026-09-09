"""Real loopback HTTP and adversarial socket tests for the worker boundary."""

from __future__ import annotations

import json
import socket
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from corpusledger import annotation_remote as remote
from corpusledger.annotation_pipeline import AnnotationPipeline
from corpusledger.annotation_protocol import (
    AnnotationRequest,
    AnnotationResponse,
    ProcessorDescription,
    encode_wire,
)
from corpusledger.annotation_remote import RemoteAnnotationError, RemoteAnnotationProcessor, loopback_endpoint
from corpusledger.annotations import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation

SECRET = "test-only-private-bearer-credential"
TOKEN_ENV = "CORPUSLEDGER_TEST_WORKER_TOKEN"
TOKEN = AnnotationType("token", {"surface": AnnotationField()})
DESCRIPTION = ProcessorDescription("test.tokens", "1", "a" * 64, produces=(TOKEN,))


def request() -> AnnotationRequest:
    return AnnotationRequest("op-1", "tokenize", DESCRIPTION, AnnotationDocument("doc", "😀 first\r\nsecond"))


def valid_response(incoming: dict[str, Any]) -> dict[str, Any]:
    parsed = AnnotationRequest.from_dict(incoming)
    return AnnotationResponse(
        parsed.operation_id,
        parsed.step_id,
        parsed.processor,
        parsed.document.digest,
        (SpanAnnotation("token-1", "token", 2, 7, {"surface": "first"}),),
        3,
    ).to_dict()


@contextmanager
def worker(
    *,
    info: Any = None,
    transform: Callable[[dict[str, Any]], Any] = valid_response,
    credential: str = SECRET,
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    observed: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args: Any) -> None:
            pass

        def handle_request(self) -> None:
            self.connection.settimeout(2)
            size = int(self.headers.get("Content-Length", "0"))
            assert size <= 16 * 1024 * 1024
            body = self.rfile.read(size) if size else None
            observed.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "auth": self.headers.get("Authorization"),
                    "body": json.loads(body) if body else None,
                }
            )
            status = 200 if self.headers.get("Authorization") == f"Bearer {credential}" else 401
            payload = (
                (
                    (DESCRIPTION.to_dict() if info is None else info)
                    if self.command == "GET"
                    else transform(observed[-1]["body"])
                )
                if status == 200
                else {"error": "private server diagnostic"}
            )
            raw = encode_wire(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(raw)

        do_GET = handle_request
        do_POST = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), name="annotation-test-http")
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", observed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def read_request_headers(connection: socket.socket) -> bytes:
    buffer = bytearray()
    while b"\r\n\r\n" not in buffer:
        chunk = connection.recv(1)
        if not chunk:
            raise OSError("client closed before request headers")
        buffer.extend(chunk)
        if len(buffer) > 16384:
            raise OSError("test client headers exceeded bound")
    return bytes(buffer)


@contextmanager
def peer(
    respond: Callable[[socket.socket, threading.Event], None],
) -> Iterator[tuple[str, threading.Event]]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.05)
    accepted, stopped = threading.Event(), threading.Event()
    failures: list[BaseException] = []

    def serve() -> None:
        connection = None
        try:
            while not stopped.is_set():
                try:
                    connection, _ = listener.accept()
                    break
                except TimeoutError:
                    continue
            if connection is not None:
                connection.settimeout(1)
                accepted.set()
                respond(connection, stopped)
        except (OSError, TimeoutError):
            # Expected when the client aborts a deliberately slow/faulty response.
            pass
        except BaseException as error:
            failures.append(error)
        finally:
            if connection is not None:
                connection.close()

    thread = threading.Thread(target=serve, name="annotation-test-raw-peer")
    thread.start()
    try:
        yield f"http://127.0.0.1:{listener.getsockname()[1]}", accepted
    finally:
        stopped.set()
        listener.close()
        thread.join(timeout=3)
        assert not thread.is_alive(), "test peer leaked a socket thread"
        if failures:
            raise failures[0]


def reply(raw: bytes) -> Callable[[socket.socket, threading.Event], None]:
    def send(connection: socket.socket, _stopped: threading.Event) -> None:
        read_request_headers(connection)
        connection.sendall(raw)

    return send


@pytest.fixture(autouse=True)
def credential(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, SECRET)


@pytest.fixture
def timers(monkeypatch: pytest.MonkeyPatch) -> list[threading.Timer]:
    created: list[threading.Timer] = []
    original = threading.Timer

    def tracked(*args: Any, **kwargs: Any) -> threading.Timer:
        timer = original(*args, **kwargs)
        created.append(timer)
        return timer

    monkeypatch.setattr(remote.threading, "Timer", tracked)
    return created


def test_real_http_verifies_before_post_and_applies_through_pipeline() -> None:
    incoming = request()
    original = incoming.document.to_dict()
    with worker() as (endpoint, calls):
        client = RemoteAnnotationProcessor(endpoint + "/", DESCRIPTION, TOKEN_ENV)
        result = AnnotationPipeline((client.as_processor("op-1", "tokenize"),)).run(incoming.document)
        assert [(call["method"], call["path"]) for call in calls] == [("GET", "/v1/info"), ("POST", "/v1/process")]
        assert all(call["auth"] == f"Bearer {SECRET}" for call in calls)
        assert calls[1]["body"] == incoming.to_dict()
        assert result.document.span_text("token-1") == "first"
        assert result.input_digest == incoming.document.digest
        assert result.document.text == incoming.document.text
        assert result.steps[0].name == DESCRIPTION.name
        assert SECRET not in json.dumps(client.identity)
        assert TOKEN_ENV not in json.dumps(client.identity)
        assert client.endpoint == endpoint
    assert incoming.document.to_dict() == original


def test_changed_info_blocks_post_and_wrong_local_request_never_connects() -> None:
    with worker(info=replace(DESCRIPTION, config_sha256="b" * 64).to_dict()) as (endpoint, calls):
        client = RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV)
        with pytest.raises(RemoteAnnotationError, match="identity"):
            client.execute(request())
        assert [call["method"] for call in calls] == ["GET"]
        with pytest.raises(RemoteAnnotationError, match="pinned worker"):
            client.execute(replace(request(), processor=replace(DESCRIPTION, version="2")))
        with pytest.raises(RemoteAnnotationError, match="pinned worker"):
            client.execute(None)  # type: ignore[arg-type]
        assert len(calls) == 1


@pytest.mark.parametrize("mode", ["digest", "operation", "step", "processor", "extra", "outside", "feature", "type"])
def test_untrusted_http_response_does_not_mutate_input(mode: str) -> None:
    def malformed(raw: dict[str, Any]) -> Any:
        result = valid_response(raw)
        if mode in {"digest", "operation", "step"}:
            field = {"digest": "input_digest", "operation": "operation_id", "step": "step_id"}[mode]
            result[field] = "b" * 64 if mode == "digest" else "other"
        elif mode == "processor":
            result["processor"]["version"] = "2"
        elif mode == "extra":
            result["document"] = "forbidden replacement"
        elif mode == "outside":
            result["annotations"][0]["end"] = 500
        elif mode == "feature":
            result["annotations"][0]["features"]["surface"] = False
        else:
            result["annotations"][0]["type"] = "undeclared"
        return result

    incoming = request()
    original = incoming.document.digest
    with worker(transform=malformed) as (endpoint, calls):
        with pytest.raises(RemoteAnnotationError, match="result could not be validated"):
            RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).execute(incoming)
        assert [call["method"] for call in calls] == ["GET", "POST"]
    assert incoming.document.digest == original


def test_credentials_rotate_without_entering_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    rotated = "rotated-test-only-credential"
    with worker(credential=rotated) as (endpoint, calls):
        client = RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV)
        identity = client.identity
        with pytest.raises(RemoteAnnotationError):
            client.execute(request())
        assert [call["method"] for call in calls] == ["GET"]
        monkeypatch.setenv(TOKEN_ENV, rotated)
        assert client.execute(request()).annotations[0].annotation_id == "token-1"
        assert client.identity == identity
        assert [call["method"] for call in calls] == ["GET", "GET", "POST"]


@pytest.mark.parametrize("value", [None, "short", "x" * 4097, "x" * 16 + "\n", "é" * 16, " " * 16])
def test_bad_or_missing_credential_fails_without_network(value: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    if value is None:
        monkeypatch.delenv(TOKEN_ENV)
    else:
        monkeypatch.setenv(TOKEN_ENV, value)
    with worker() as (endpoint, calls):
        with pytest.raises(RemoteAnnotationError):
            RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).verify()
        assert calls == []


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1",
        "http://localhost",
        "http://example.com",
        "http://192.0.2.1",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1/path",
        "http://127.0.0.1?q=1",
        "http://127.0.0.1#fragment",
        "http://127.0.0.1:0",
        "http://127.0.0.1:65536",
        "http://[::1%zone]",
        "http://127.0.0.1\r\nInjected: yes",
        None,
    ],
)
def test_endpoint_rejects_implicit_routing(endpoint: Any) -> None:
    with pytest.raises(RemoteAnnotationError, match="literal-loopback"):
        loopback_endpoint(endpoint)


def test_loopback_canonicalization_and_strict_client_options() -> None:
    assert loopback_endpoint("http://127.0.0.1/") == ("http://127.0.0.1:80", "127.0.0.1", 80)
    assert loopback_endpoint("http://[::1]:8123/") == ("http://[::1]:8123", "::1", 8123)
    client = RemoteAnnotationProcessor("http://127.0.0.1:80", DESCRIPTION, TOKEN_ENV)
    for change in (
        {"description": None},
        {"token_env": "invalid-name"},
        {"timeout": True},
        {"timeout": float("nan")},
        {"timeout": float("inf")},
        {"timeout": 0},
        {"max_response_bytes": True},
        {"max_response_bytes": 0},
        {"max_response_bytes": 16 * 1024 * 1024 + 1},
    ):
        with pytest.raises(RemoteAnnotationError):
            replace(client, **change)


def test_environment_proxies_are_not_used(monkeypatch: pytest.MonkeyPatch) -> None:
    with worker() as (proxy, proxy_calls), worker() as (endpoint, calls):
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            monkeypatch.setenv(name, proxy)
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).execute(request())
        assert len(calls) == 2 and proxy_calls == []


def test_redirect_does_not_forward_bearer_to_another_origin() -> None:
    with worker() as (target, calls):
        response = f"HTTP/1.1 302 Found\r\nLocation: {target}/v1/info\r\nContent-Length: 0\r\n\r\n".encode()
        with (
            peer(reply(response)) as (endpoint, _accepted),
            pytest.raises(RemoteAnnotationError, match="status 302"),
        ):
            remote._exchange(endpoint, TOKEN_ENV, 1, 10000)
        assert calls == []


@pytest.mark.parametrize(
    "headers",
    [
        b"Content-Length: 2\r\nContent-Length: 2\r\nContent-Type: application/json\r\n",
        b"Content-Length: 2, 2\r\nContent-Type: application/json\r\n",
        b"Content-Length: +2\r\nContent-Type: application/json\r\n",
        b"Content-Type: application/json\r\n",
        b"Content-Length: 2\r\nContent-Type: text/plain\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\nContent-Type: application/json\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n",
        b"Content-Length: 2\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\n",
    ],
)
def test_ambiguous_or_unsupported_http_framing(headers: bytes) -> None:
    with (
        peer(reply(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n{}")) as (endpoint, _accepted),
        pytest.raises(RemoteAnnotationError, match="framing"),
    ):
        remote._exchange(endpoint, TOKEN_ENV, 1, 1000)


@pytest.mark.parametrize("length,body,limit,message", [(99, b"{}", 10, "byte limit"), (3, b"{}", 1000, "incomplete")])
def test_oversized_advertisement_and_short_body(length: int, body: bytes, limit: int, message: str) -> None:
    raw = f"HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nContent-Type: application/json\r\n\r\n".encode() + body
    with peer(reply(raw)) as (endpoint, _accepted), pytest.raises(RemoteAnnotationError, match=message):
        remote._exchange(endpoint, TOKEN_ENV, 1, limit)


def test_refused_connection_is_controlled_and_does_not_start_timer(timers: list[threading.Timer]) -> None:
    reservation = socket.socket()
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
    # Keep the bound port reserved but not listening; no ephemeral port reuse race.
    try:
        with pytest.raises(RemoteAnnotationError, match="transport"):
            remote._exchange(f"http://127.0.0.1:{port}", TOKEN_ENV, 0.2, 1000)
    finally:
        reservation.close()
    assert timers == []


@pytest.mark.parametrize("stage", ["headers", "body", "send"])
def test_absolute_deadline_interrupts_trickling_and_blocked_send_without_timer_leaks(
    stage: str, timers: list[threading.Timer]
) -> None:
    def slow(connection: socket.socket, stopped: threading.Event) -> None:
        if stage == "send":
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            stopped.wait(2)
            return
        read_request_headers(connection)
        if stage == "headers":
            data = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n"
        else:
            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n")
            data = b" " * 1000
        for byte in data:
            if stopped.wait(0.03):
                break
            connection.sendall(bytes((byte,)))

    payload = {"text": "x" * (12 * 1024 * 1024)} if stage == "send" else None
    with peer(slow) as (endpoint, accepted):
        started = time.monotonic()
        with pytest.raises(RemoteAnnotationError):
            remote._exchange(endpoint, TOKEN_ENV, 0.2, 2000, payload)
        elapsed = time.monotonic() - started
        assert accepted.is_set()
        assert elapsed < 2, f"absolute socket deadline failed: {elapsed:.3f}s"
    assert timers and all(not timer.is_alive() for timer in timers)


def test_success_also_cancels_timer(timers: list[threading.Timer]) -> None:
    with worker() as (endpoint, _calls):
        RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).execute(request())
    assert len(timers) == 2 and all(not timer.is_alive() for timer in timers)


def test_malformed_header_line_is_not_silently_accepted() -> None:
    raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nmalformed-header\r\n\r\n{}"
    with peer(reply(raw)) as (endpoint, _accepted), pytest.raises(RemoteAnnotationError):
        remote._exchange(endpoint, TOKEN_ENV, 1, 1000)


def test_private_worker_payload_is_redacted_from_exception_traceback() -> None:
    private_marker = "private-payload-marker-271828"

    def malformed(raw: dict[str, Any]) -> Any:
        result = valid_response(raw)
        result["annotations"][0]["id"] = private_marker
        result["annotations"][0]["features"]["surface"] = False
        return result

    with worker(transform=malformed) as (endpoint, _calls):
        with pytest.raises(RemoteAnnotationError) as caught:
            RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).execute(request())
        rendered = "".join(traceback.format_exception(caught.value))
        assert private_marker not in rendered
        assert SECRET not in rendered


def test_bad_status_line_does_not_leak_remote_text_through_exception_chain() -> None:
    private = b"PRIVATE-REMOTE-STATUS-DATA"
    with peer(reply(private + b"\r\n\r\n")) as (endpoint, _accepted):
        with pytest.raises(RemoteAnnotationError) as caught:
            remote._exchange(endpoint, TOKEN_ENV, 1, 1000)
        assert private.decode() not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("body", [b'{"private-wire-key":1,"private-wire-key":2}', b"NaN", b'"\\ud800"', b"\xff"])
def test_ambiguous_json_over_real_http_is_rejected_and_redacted(body: bytes) -> None:
    raw = f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\nContent-Type: application/json\r\n\r\n".encode() + body
    with peer(reply(raw)) as (endpoint, _accepted):
        with pytest.raises(RemoteAnnotationError) as caught:
            RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).verify()
        assert "private-wire-key" not in "".join(traceback.format_exception(caught.value))


def test_excessively_long_header_is_rejected_with_timer_cleanup(timers: list[threading.Timer]) -> None:
    raw = b"HTTP/1.1 200 OK\r\nX-Too-Long: " + b"x" * 70000 + b"\r\n\r\n"
    with peer(reply(raw)) as (endpoint, _accepted), pytest.raises(RemoteAnnotationError):
        remote._exchange(endpoint, TOKEN_ENV, 1, 1000)
    assert timers and all(not timer.is_alive() for timer in timers)


def test_invalid_internal_media_type_whitespace_is_not_normalized_into_json() -> None:
    body = encode_wire(DESCRIPTION.to_dict())
    raw = (
        f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\nContent-Type: appli cation / js on\r\n\r\n".encode() + body
    )
    with peer(reply(raw)) as (endpoint, _accepted), pytest.raises(RemoteAnnotationError):
        RemoteAnnotationProcessor(endpoint, DESCRIPTION, TOKEN_ENV).verify()
