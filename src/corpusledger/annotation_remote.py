"""Bounded, authenticated loopback transport for pinned annotation processors."""

from __future__ import annotations

import hashlib
import http.client
import ipaddress
import os
import re
import socket
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .annotation_pipeline import AnnotationPipelineError, AnnotationProcessor
from .annotation_protocol import (
    MAX_WIRE_BYTES,
    AnnotationProtocolError,
    AnnotationRequest,
    AnnotationResponse,
    ProcessorDescription,
    decode_wire,
    encode_wire,
    identifier,
)
from .errors import InputError


class RemoteAnnotationError(AnnotationProtocolError):
    """A configured worker is unavailable or violates the pinned contract."""


def loopback_endpoint(value: Any) -> tuple[str, str, int]:
    """Reject DNS names, credentials, fragments, paths and non-loopback routing."""
    try:
        if not isinstance(value, str) or any(ord(char) <= 32 for char in value):
            raise ValueError
        parts = urlsplit(value)
        if (
            parts.scheme != "http"
            or not parts.hostname
            or "%" in parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.path not in ("", "/")
            or parts.query
            or parts.fragment
        ):
            raise ValueError
        address = ipaddress.ip_address(parts.hostname)
        if not address.is_loopback:
            raise ValueError
        port = parts.port if parts.port is not None else 80
        if not 1 <= port <= 65535:
            raise ValueError
        host = str(address)
        authority = f"[{host}]" if address.version == 6 else host
        return f"http://{authority}:{port}", host, port
    except ValueError:
        raise RemoteAnnotationError("worker endpoint must be an HTTP literal-loopback origin") from None


def _token(environment_name: str) -> str:
    value = os.environ.get(environment_name)
    if not value or not 16 <= len(value) <= 4096 or any(not 33 <= ord(char) <= 126 for char in value):
        raise RemoteAnnotationError("worker credential is missing or invalid")
    return value


def _response_length(response: http.client.HTTPResponse, limit: int) -> int:
    headers = response.getheaders()
    lengths = [value for key, value in headers if key.lower() == "content-length"]
    types = [value for key, value in headers if key.lower() == "content-type"]
    if (
        response.msg.defects
        or len(lengths) != 1
        or re.fullmatch(r"[0-9]{1,10}", lengths[0]) is None
        or len(types) != 1
        or re.fullmatch(
            r"application/json(?:[ \t]*;[ \t]*charset[ \t]*=[ \t]*utf-8)?",
            types[0].strip(" \t"),
            flags=re.IGNORECASE,
        )
        is None
        or any(key.lower() in {"transfer-encoding", "content-encoding"} for key, _ in headers)
    ):
        raise RemoteAnnotationError("worker returned unsupported or ambiguous HTTP framing")
    length = int(lengths[0])
    if length > limit:
        raise RemoteAnnotationError("worker response exceeds byte limit")
    return length


def _exchange(endpoint: str, token_env: str, timeout: float, limit: int, payload: Any = None) -> Any:
    origin, host, port = loopback_endpoint(endpoint)
    token = _token(token_env)
    body = encode_wire(payload) if payload is not None else None
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    started = time.monotonic()
    deadline: threading.Timer | None = None
    connected_socket: socket.socket | None = None
    try:
        # Numeric literal only: no environment proxy or hostname resolution policy.
        connection.connect()
        connected_socket = connection.sock
        if connected_socket is None:
            raise RemoteAnnotationError("worker connection is unavailable")
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            raise RemoteAnnotationError("worker request deadline exceeded")
        active_socket = connected_socket

        def abort() -> None:
            # shutdown, not close alone: HTTPResponse's makefile can otherwise
            # keep a socket alive while a slow peer trickles headers or a body.
            with suppress(OSError):
                active_socket.shutdown(socket.SHUT_RDWR)
            active_socket.close()

        deadline = threading.Timer(remaining, abort)
        deadline.daemon = True
        deadline.start()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Connection": "close",
            "Host": origin.removeprefix("http://"),
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(
            "POST" if body is not None else "GET", "/v1/process" if body is not None else "/v1/info", body, headers
        )
        with connection.getresponse() as response:
            if response.status != 200:
                raise RemoteAnnotationError(f"worker HTTP request failed with status {response.status}")
            expected = _response_length(response, limit)
            received = bytearray()
            while chunk := response.read1(min(65536, limit + 1 - len(received))):
                received.extend(chunk)
                if len(received) > limit:
                    raise RemoteAnnotationError("worker response exceeds byte limit")
            if len(received) != expected or time.monotonic() - started > timeout:
                raise RemoteAnnotationError("worker response is incomplete or exceeded its deadline")
        try:
            return decode_wire(bytes(received), limit=limit)
        except AnnotationProtocolError:
            raise RemoteAnnotationError("worker JSON response is invalid") from None
    except (OSError, http.client.HTTPException):
        raise RemoteAnnotationError("worker transport failed or exceeded its deadline") from None
    finally:
        if deadline is not None:
            deadline.cancel()
            deadline.join(timeout=1)
        connection.close()
        if connected_socket is not None:
            connected_socket.close()


@dataclass(frozen=True, slots=True)
class RemoteAnnotationProcessor:
    """A fixed worker origin, credential source and exact processor description.

    The origin is HTTP loopback only, with no redirect or proxy support. The
    deadline bounds connect plus socket exchange, not local JSON/schema work.
    Credentials rotate via the named environment variable and are never part of
    the persisted identity. This is not a public-internet transport or attestation.
    """

    endpoint: str
    description: ProcessorDescription
    token_env: str
    timeout: float = 10.0
    max_response_bytes: int = MAX_WIRE_BYTES

    def __post_init__(self) -> None:
        canonical, _, _ = loopback_endpoint(self.endpoint)
        object.__setattr__(self, "endpoint", canonical)
        if not isinstance(self.description, ProcessorDescription):
            raise RemoteAnnotationError("worker requires an explicit pinned processor description")
        if not isinstance(self.token_env, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", self.token_env) is None:
            raise RemoteAnnotationError("worker credential source must be an environment variable name")
        if type(self.timeout) not in (int, float) or not 0 < self.timeout <= 300:
            raise RemoteAnnotationError("worker timeout must be a finite number in (0, 300]")
        if type(self.max_response_bytes) is not int or not 1 <= self.max_response_bytes <= MAX_WIRE_BYTES:
            raise RemoteAnnotationError("invalid worker response byte limit")
        object.__setattr__(self, "timeout", float(self.timeout))

    @property
    def identity(self) -> dict[str, Any]:
        return {
            "processor": self.description.to_dict(),
            "endpoint_sha256": hashlib.sha256(self.endpoint.encode("ascii")).hexdigest(),
        }

    def verify(self) -> None:
        """Confirm schemas/configuration before any processor POST invocation."""
        try:
            current = ProcessorDescription.from_dict(
                _exchange(self.endpoint, self.token_env, self.timeout, self.max_response_bytes)
            )
            if current != self.description:
                raise RemoteAnnotationError("worker processor identity changed")
        except (InputError, ValueError):
            raise RemoteAnnotationError("worker processor identity could not be verified") from None

    def execute(self, request: AnnotationRequest) -> AnnotationResponse:
        if not isinstance(request, AnnotationRequest) or request.processor != self.description:
            raise RemoteAnnotationError("request processor does not match the pinned worker")
        self.verify()
        try:
            result = AnnotationResponse.from_dict(
                _exchange(self.endpoint, self.token_env, self.timeout, self.max_response_bytes, request.to_dict())
            )
            result.apply(request)
            return result
        except (InputError, AnnotationPipelineError):
            raise RemoteAnnotationError("worker result could not be validated") from None

    def as_processor(self, operation_id: str, step_id: str) -> AnnotationProcessor:
        identifier(operation_id, "operation ID")
        identifier(step_id, "step ID")
        return self.description.as_processor(
            lambda document: (
                self.execute(AnnotationRequest(operation_id, step_id, self.description, document)).annotations
            )
        )
