"""Canonical default-port Host headers, without binding a privileged TCP port."""

import http.client
import socket
import threading
from contextlib import contextmanager

import pytest

from corpusledger.annotation_remote import _exchange


@contextmanager
def capture_request():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(3)
    requests = []
    failures = []

    def serve():
        try:
            with listener.accept()[0] as connection:
                connection.settimeout(3)
                data = bytearray()
                while b"\r\n\r\n" not in data:
                    chunk = connection.recv(8192)
                    if not chunk:
                        raise AssertionError("incomplete test request")
                    data.extend(chunk)
                requests.append(bytes(data))
                connection.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n"
                    b"Connection: close\r\n\r\n{}"
                )
        except Exception as exc:
            failures.append(exc)

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        yield listener.getsockname(), requests
    finally:
        thread.join(4)
        listener.close()
        assert not thread.is_alive()
        assert not failures


@pytest.mark.parametrize("endpoint,authority", [("http://127.0.0.1", "127.0.0.1:80"), ("http://[::1]", "[::1]:80")])
def test_worker_transport_keeps_explicit_canonical_default_port(monkeypatch, endpoint, authority):
    monkeypatch.setenv("CORPUSLEDGER_AUTHORITY_TEST_TOKEN", "authority-contract-test-secret")
    with capture_request() as (physical_address, requests):
        # Keep HTTPConnection's logical host/port; only redirect its physical
        # connection to an ephemeral test listener. No DNS/privileged port used.
        def connect(connection):
            connection.sock = socket.create_connection(physical_address, timeout=2)

        monkeypatch.setattr(http.client.HTTPConnection, "connect", connect)
        assert _exchange(endpoint, "CORPUSLEDGER_AUTHORITY_TEST_TOKEN", 2, 1024) == {}
    headers = requests[0].split(b"\r\n")
    assert [line for line in headers if line.lower().startswith(b"host:")] == [f"Host: {authority}".encode()]
