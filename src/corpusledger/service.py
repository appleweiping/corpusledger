"""Small local HTTP/JSON service for CorpusLedger operations.

The service deliberately binds to loopback by default and reuses the same
strict library functions as the CLI. It is an integration boundary for local
pipelines, not an Internet-facing multi-tenant server.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .canonical import CanonicalPolicy
from .catalog import SnapshotCatalog
from .diff import compare
from .manifest import Manifest, build_manifest
from .readers import iter_corpus
from .schema import to_json_schema, validate_json_schema
from .store import verify_bundle


class CorpusService:
    """Dispatch typed JSON requests to manifest, diff, and bundle operations."""

    def dispatch(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Execute one request and return a JSON-compatible response."""
        if not isinstance(request, Mapping):
            raise ValueError("request must be an object")
        operation = request.get("operation")
        if operation == "manifest":
            source = _required_path(request, "input")
            policy_value = request.get("policy", {})
            if not isinstance(policy_value, Mapping):
                raise ValueError("policy must be an object")
            return {
                "operation": operation,
                "manifest": build_manifest(source, policy=CanonicalPolicy.from_dict(dict(policy_value))).to_dict(),
            }
        if operation == "diff":
            before = Manifest.load(_required_path(request, "before"))
            after = Manifest.load(_required_path(request, "after"))
            return {"operation": operation, "diff": compare(before, after).to_dict()}
        if operation == "verify_bundle":
            bundle = _required_path(request, "bundle")
            expected = request.get("expected_archive_digest")
            if expected is not None and not isinstance(expected, str):
                raise ValueError("expected_archive_digest must be a string or omitted")
            report = verify_bundle(bundle, expected_archive_digest=expected)
            return {
                "operation": operation,
                "verified": True,
                "archive_digest": report.archive_digest,
                "manifest_digest": report.manifest_digest,
                "files": list(report.files),
                "bytes": report.bytes,
            }
        if operation == "schema":
            manifest = Manifest.load(_required_path(request, "manifest"))
            title = request.get("title")
            schema_id = request.get("id")
            if title is not None and not isinstance(title, str):
                raise ValueError("title must be a string or omitted")
            if schema_id is not None and not isinstance(schema_id, str):
                raise ValueError("id must be a string or omitted")
            return {
                "operation": operation,
                "schema": to_json_schema(manifest.schema, title=title, schema_id=schema_id),
            }
        if operation == "schema_validate":
            source = _required_path(request, "input")
            schema_value = request.get("schema")
            if isinstance(schema_value, str):
                try:
                    schema_value = json.loads(Path(schema_value).read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise ValueError(f"cannot read schema: {exc}") from exc
            if not isinstance(schema_value, (Mapping, bool)):
                raise ValueError("schema must be an object, boolean, or path string")
            max_errors = request.get("max_errors", 100)
            if isinstance(max_errors, bool) or not isinstance(max_errors, int):
                raise ValueError("max_errors must be an integer")
            checked = 0

            def values() -> Iterator[dict[str, Any]]:
                nonlocal checked
                for record in iter_corpus(source):
                    checked += 1
                    yield record.data

            issues = validate_json_schema(values(), schema_value, max_errors=max_errors)
            return {
                "operation": operation,
                "valid": not issues,
                "records_checked": checked,
                "errors": [item.to_dict() for item in issues],
            }
        if operation == "catalog":
            return _catalog_request(request)
        raise ValueError("operation must be one of: manifest, diff, verify_bundle, schema, schema_validate, catalog")


def create_server(
    service: CorpusService | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    """Create a threaded JSON server; call ``serve_forever`` to run it."""
    target = service or CorpusService()
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise ValueError("port must be an integer between 0 and 65535")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.path != "/v1/dispatch":
                self._write(HTTPStatus.NOT_FOUND, {"error": "unknown endpoint"})
                return
            try:
                size = int(self.headers.get("Content-Length", "-1"))
                if size < 0 or size > 4 * 1024 * 1024:
                    raise ValueError("Content-Length must be between 0 and 4194304")
                raw = self.rfile.read(size)
                request = json.loads(raw.decode("utf-8"))
                response = target.dispatch(request)
            except (UnicodeError, json.JSONDecodeError, TypeError, ValueError, OSError) as error:
                self._write(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            self._write(HTTPStatus.OK, response)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _write(self, status: HTTPStatus, payload: Mapping[str, Any]) -> None:
            encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server


def _required_path(request: Mapping[str, Any], name: str) -> Path:
    value = request.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path string")
    return Path(value)


def _catalog_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Dispatch catalog listing, lineage, diff, and registration requests."""

    database = _required_path(request, "database")
    action = request.get("action", "list")
    if not isinstance(action, str):
        raise ValueError("catalog action must be a string")
    with SnapshotCatalog(database) as catalog:
        if action == "list":
            name = request.get("name")
            if name is not None and not isinstance(name, str):
                raise ValueError("catalog name must be a string or omitted")
            refs = catalog.list(name)
            return {
                "operation": "catalog",
                "action": action,
                "snapshots": [_snapshot_ref_payload(ref) for ref in refs],
            }
        if action == "lineage":
            corpus_hash = request.get("corpus_hash")
            if not isinstance(corpus_hash, str) or not corpus_hash:
                raise ValueError("corpus_hash must be a non-empty string")
            return {
                "operation": "catalog",
                "action": action,
                "lineage": [_snapshot_ref_payload(ref) for ref in catalog.lineage(corpus_hash)],
            }
        if action == "diff":
            name = request.get("name")
            before = request.get("before")
            after = request.get("after")
            if not isinstance(name, str) or not name.strip():
                raise ValueError("catalog name must be a non-empty string")
            if isinstance(before, bool) or not isinstance(before, int):
                raise ValueError("before must be an integer")
            if isinstance(after, bool) or not isinstance(after, int):
                raise ValueError("after must be an integer")
            difference = catalog.diff(name, before, after)
            return {
                "operation": "catalog",
                "action": action,
                "name": name,
                "before": before,
                "after": after,
                "diff": difference.to_dict(),
            }
        if action == "register":
            name = request.get("name")
            manifest_path = request.get("manifest")
            parent = request.get("parent")
            tags = request.get("tags", [])
            if not isinstance(name, str) or not name.strip():
                raise ValueError("catalog name must be a non-empty string")
            if not isinstance(manifest_path, str) or not manifest_path.strip():
                raise ValueError("manifest must be a non-empty path string")
            if parent is not None and not isinstance(parent, str):
                raise ValueError("parent must be a string or omitted")
            if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
                raise ValueError("tags must be an array of strings")
            ref = catalog.register(
                name,
                Manifest.load(manifest_path),
                parent=parent,
                tags=tuple(tags),
            )
            return {
                "operation": "catalog",
                "action": action,
                "snapshot": _snapshot_ref_payload(ref),
            }
    raise ValueError("catalog action must be one of: list, lineage, diff, register")


def _snapshot_ref_payload(ref: Any) -> dict[str, Any]:
    return {
        "name": ref.name,
        "version": ref.version,
        "corpus_hash": ref.corpus_hash,
        "parent": ref.parent,
        "tags": list(ref.tags),
    }
