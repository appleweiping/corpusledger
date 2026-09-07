from __future__ import annotations

import json
import threading
import urllib.request
from urllib.error import HTTPError

import pytest

from corpusledger import CorpusService, build_manifest, bundle_snapshot, create_server


def test_service_dispatch_manifest_and_http(tmp_path) -> None:
    source = tmp_path / "records.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    result = CorpusService().dispatch({"operation": "manifest", "input": str(source)})
    assert result["manifest"]["records"][0]["record_id"] == "a"

    server = create_server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/v1/dispatch",
            data=json.dumps({"operation": "manifest", "input": str(source)}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read())
        assert payload["manifest"]["corpus_hash"] == result["manifest"]["corpus_hash"]
    finally:
        server.shutdown()
        server.server_close()


def test_service_rejects_unknown_operations() -> None:
    try:
        CorpusService().dispatch({"operation": "delete_everything"})
    except ValueError as error:
        assert "operation must be one of" in str(error)
    else:  # pragma: no cover
        raise AssertionError("unknown operation was accepted")


def test_service_diff_and_bundle_verification(tmp_path) -> None:
    before_source = tmp_path / "before.jsonl"
    after_source = tmp_path / "after.jsonl"
    before_source.write_text('{"id":"a","text":"before"}\n', encoding="utf-8")
    after_source.write_text('{"id":"a","text":"after"}\n', encoding="utf-8")
    before = build_manifest(before_source)
    after = build_manifest(after_source)
    before_path = tmp_path / "before.json"
    after_path = tmp_path / "after.json"
    before.save(before_path)
    after.save(after_path)
    diff = CorpusService().dispatch({"operation": "diff", "before": str(before_path), "after": str(after_path)})
    assert diff["diff"]["has_changes"] is True
    bundle_path = tmp_path / "snapshot.zip"
    bundle_snapshot(after, after_source, bundle_path)
    verified = CorpusService().dispatch({"operation": "verify_bundle", "bundle": str(bundle_path)})
    assert verified["verified"] is True
    assert verified["files"] == ["manifest.json", "source/after.jsonl"]


def test_service_rejects_invalid_requests_and_endpoint(tmp_path) -> None:
    with pytest.raises(ValueError, match="request must be an object"):
        CorpusService().dispatch([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="expected_archive_digest"):
        CorpusService().dispatch({"operation": "verify_bundle", "bundle": "x", "expected_archive_digest": 4})
    with pytest.raises(ValueError, match="port"):
        create_server(port=65536)
    server = create_server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/wrong",
            data=b"{}",
            method="POST",
        )
        with pytest.raises(HTTPError) as error:
            urllib.request.urlopen(request, timeout=5)
        assert error.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
