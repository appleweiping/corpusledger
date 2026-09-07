"""Small language-neutral NDJSON processor gateway.

The gateway is intentionally transport-agnostic: a caller can feed it stdin,
an HTTP body, a subprocess pipe, or a gRPC adapter without changing processor
semantics. Every input line produces exactly one JSON response line, including a
structured error, which makes it safe to drive from clients in other languages.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

Processor = Callable[[Mapping[str, Any]], Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class StreamReport:
    """Digest and outcome counts for one gateway stream."""

    input_digest: str
    output_digest: str
    records: int
    successes: int
    failures: int


class NdjsonGateway:
    """Registry and line protocol for independent record processors."""

    def __init__(self, *, max_line_bytes: int = 1_048_576) -> None:
        if isinstance(max_line_bytes, bool) or not isinstance(max_line_bytes, int):
            raise TypeError("max_line_bytes must be an integer")
        if max_line_bytes < 1:
            raise ValueError("max_line_bytes must be positive")
        self.max_line_bytes = max_line_bytes
        self._processors: dict[str, Processor] = {}

    @property
    def processors(self) -> tuple[str, ...]:
        """Return registered processor names in stable order."""

        return tuple(sorted(self._processors))

    def register(self, name: str, processor: Processor, *, replace: bool = False) -> None:
        """Register a processor; duplicate names require explicit replacement."""

        if not isinstance(name, str) or not name.strip() or any(char.isspace() for char in name):
            raise ValueError("processor name must be a non-empty token")
        if not callable(processor):
            raise TypeError("processor must be callable")
        if name in self._processors and not replace:
            raise ValueError(f"processor {name!r} is already registered")
        self._processors[name] = processor

    def dispatch(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Process one request object under the stable protocol."""

        if not isinstance(request, Mapping):
            raise ValueError("request must be an object")
        name = request.get("processor")
        payload = request.get("payload")
        if not isinstance(name, str) or name not in self._processors:
            raise ValueError("processor must name a registered processor")
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be an object")
        result = self._processors[name](dict(payload))
        if not isinstance(result, Mapping):
            raise TypeError("processor must return an object")
        return {"ok": True, "processor": name, "result": dict(result)}

    def process_lines(self, lines: Iterable[bytes | str]) -> tuple[tuple[str, ...], StreamReport]:
        """Process NDJSON lines and return responses plus reproducible digests.

        A malformed line never shifts response alignment: it receives one
        ``ok=false`` response. Input and output digests hash the exact UTF-8
        lines including their normalized trailing newline.
        """

        input_hash = hashlib.sha256()
        output_hash = hashlib.sha256()
        responses: list[str] = []
        successes = failures = 0
        for raw in lines:
            error: str | None
            if isinstance(raw, bytes):
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    text = ""
                    error = "line is not valid UTF-8"
                else:
                    error = None
            elif isinstance(raw, str):
                text = raw
                error = None
            else:
                text = ""
                error = "line must be text or bytes"
            normalized = text.rstrip("\r\n") + "\n"
            encoded = normalized.encode("utf-8")
            input_hash.update(encoded)
            if error is None and len(encoded) > self.max_line_bytes:
                error = f"line exceeds {self.max_line_bytes} bytes"
            if error is None:
                try:
                    request = json.loads(text)
                    response = self.dispatch(request)
                    successes += 1
                except (UnicodeError, json.JSONDecodeError, TypeError, ValueError, OSError) as exc:
                    response = {"ok": False, "error": str(exc)}
                    failures += 1
            else:
                response = {"ok": False, "error": error}
                failures += 1
            rendered = json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            responses.append(rendered)
            output_hash.update(rendered.encode("utf-8"))
        return tuple(responses), StreamReport(
            input_hash.hexdigest(),
            output_hash.hexdigest(),
            len(responses),
            successes,
            failures,
        )
