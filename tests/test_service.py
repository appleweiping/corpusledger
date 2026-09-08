from __future__ import annotations

import json
import threading
import urllib.request
from urllib.error import HTTPError

import pytest

from corpusledger import CorpusService, build_manifest, bundle_snapshot, create_server
from corpusledger.cli import _parser


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


def test_service_manifest_accepts_selective_sort_policy(tmp_path) -> None:
    source = tmp_path / "records.jsonl"
    source.write_text('{"id":"a","labels":["b","a"]}\n', encoding="utf-8")
    result = CorpusService().dispatch(
        {"operation": "manifest", "input": str(source), "policy": {"sort_paths": ["labels"]}}
    )
    assert result["manifest"]["hash_metadata"]["policy"]["sort_paths"] == ["labels"]


def test_service_external_sort_operation(tmp_path) -> None:
    source = tmp_path / "records.jsonl"
    output = tmp_path / "sorted.jsonl"
    source.write_text('{"id":"b"}\n{"id":"a"}\n', encoding="utf-8")
    result = CorpusService().dispatch(
        {"operation": "sort_jsonl", "input": str(source), "output": str(output), "chunk_size": 1}
    )
    assert result["report"]["records"] == 2
    assert output.read_text(encoding="utf-8").splitlines()[0].startswith('{"id":"a"')


def test_service_rejects_invalid_extended_requests(tmp_path) -> None:
    source = tmp_path / "records.jsonl"
    source.write_text('{"id":"a"}\n', encoding="utf-8")
    service = CorpusService()
    with pytest.raises(ValueError):
        service.dispatch([])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        service.dispatch(
            {"operation": "sort_jsonl", "input": str(source), "output": str(tmp_path / "x"), "chunk_size": True}
        )
    with pytest.raises(ValueError):
        service.dispatch({"operation": "manifest", "input": str(source), "policy": []})
    manifest_path = tmp_path / "manifest.json"
    build_manifest(source).save(manifest_path)
    with pytest.raises(ValueError):
        service.dispatch({"operation": "schema", "manifest": str(manifest_path), "title": 1})


def test_service_rejects_unknown_operations() -> None:
    try:
        CorpusService().dispatch({"operation": "delete_everything"})
    except ValueError as error:
        assert "operation must be one of" in str(error)
    else:  # pragma: no cover
        raise AssertionError("unknown operation was accepted")


def test_cli_exposes_service_command() -> None:
    args = _parser().parse_args(["serve", "--host", "127.0.0.1", "--port", "0"])
    assert args.command == "serve"
    assert args.host == "127.0.0.1"
    assert args.port == 0


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


def test_service_exports_manifest_schema(tmp_path) -> None:
    source = tmp_path / "records.jsonl"
    source.write_text('{"id":"a","meta":{"lang":"en"}}\n', encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    build_manifest(source).save(manifest_path)
    result = CorpusService().dispatch(
        {
            "operation": "schema",
            "manifest": str(manifest_path),
            "title": "records",
        }
    )
    assert result["schema"]["title"] == "records"
    assert result["schema"]["properties"]["meta"]["properties"]["lang"]["type"] == "string"


def test_cli_schema_validation_and_service_shape(tmp_path, capsys) -> None:
    source = tmp_path / "records.jsonl"
    source.write_text('{"id":"a","score":1}\n{"id":"b","score":"bad"}\n', encoding="utf-8")
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(
        json.dumps({"type": "object", "required": ["score"], "properties": {"score": {"type": "number"}}}),
        encoding="utf-8",
    )
    from corpusledger.cli import run

    assert run(["schema-validate", str(source), str(schema_path)]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["records_checked"] == 2
    assert payload["errors"][0]["path"] == "/score"
    service_payload = CorpusService().dispatch(
        {
            "operation": "schema_validate",
            "input": str(source),
            "schema": str(schema_path),
        }
    )
    assert service_payload["valid"] is False


def test_service_catalog_operations_are_typed_and_lineage_aware(tmp_path) -> None:
    first_source = tmp_path / "first.jsonl"
    second_source = tmp_path / "second.jsonl"
    first_source.write_text('{"id":"a","text":"one"}\n', encoding="utf-8")
    second_source.write_text('{"id":"a","text":"two"}\n', encoding="utf-8")
    first = build_manifest(first_source)
    second = build_manifest(second_source)
    first_path = tmp_path / "first.manifest.json"
    second_path = tmp_path / "second.manifest.json"
    first.save(first_path)
    second.save(second_path)
    database = tmp_path / "catalog.sqlite"
    service = CorpusService()
    registered = service.dispatch(
        {
            "operation": "catalog",
            "action": "register",
            "database": str(database),
            "name": "release",
            "manifest": str(first_path),
            "tags": ["baseline"],
        }
    )
    first_hash = registered["snapshot"]["corpus_hash"]
    second_registered = service.dispatch(
        {
            "operation": "catalog",
            "action": "register",
            "database": str(database),
            "name": "release",
            "manifest": str(second_path),
            "parent": first_hash,
        }
    )
    assert second_registered["snapshot"]["version"] == 2
    listing = service.dispatch({"operation": "catalog", "database": str(database), "action": "list", "name": "release"})
    assert [item["version"] for item in listing["snapshots"]] == [1, 2]
    lineage = service.dispatch(
        {
            "operation": "catalog",
            "database": str(database),
            "action": "lineage",
            "corpus_hash": second_registered["snapshot"]["corpus_hash"],
        }
    )
    assert [item["version"] for item in lineage["lineage"]] == [2, 1]
    difference = service.dispatch(
        {
            "operation": "catalog",
            "database": str(database),
            "action": "diff",
            "name": "release",
            "before": 1,
            "after": 2,
        }
    )
    assert difference["diff"]["has_changes"] is True


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


def test_service_catalog_validates_request_shape(tmp_path) -> None:
    database = tmp_path / "catalog.sqlite"
    service = CorpusService()
    invalid_requests = (
        {"operation": "catalog", "database": str(database), "action": 1},
        {"operation": "catalog", "database": str(database), "action": "list", "name": 1},
        {"operation": "catalog", "database": str(database), "action": "lineage", "corpus_hash": ""},
        {"operation": "catalog", "database": str(database), "action": "diff", "name": "x", "before": True, "after": 2},
        {
            "operation": "catalog",
            "database": str(database),
            "action": "register",
            "name": "x",
            "manifest": "x",
            "tags": "bad",
        },
        {"operation": "catalog", "database": str(database), "action": "unknown"},
    )
    for request in invalid_requests:
        with pytest.raises((ValueError, KeyError)):
            service.dispatch(request)
