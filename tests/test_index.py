from __future__ import annotations

import json
from pathlib import Path

import pytest

from corpusledger import ManifestIndex, build_manifest
from corpusledger.cli import run


def _manifest(tmp_path: Path, value: int = 1):
    source = tmp_path / "records.jsonl"
    source.write_text(
        json.dumps({"id": "alpha", "text": "hello", "value": value})
        + "\n"
        + json.dumps({"id": "beta", "text": "bye", "value": 2})
        + "\n",
        encoding="utf-8",
    )
    return build_manifest(source)


def test_manifest_index_builds_queries_and_verifies(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    index_path = tmp_path / "records.index.db"
    with ManifestIndex.build(manifest, index_path) as index:
        assert index.manifest_digest
        assert index.corpus_hash == manifest.corpus_hash
        assert index.record_count == 2
        assert index.get("alpha").field_paths == ("/id", "/text", "/value")
        assert [row.record_id for row in index.query(id_prefix="a")] == ["alpha"]
        assert [row.record_id for row in index.query(source="records.jsonl")] == ["alpha", "beta"]
        assert [row.record_id for row in index.query(field_path="/text")] == ["alpha", "beta"]
        assert index.stats()["field_paths"] == 6
        index.verify(manifest)
        with pytest.raises(KeyError):
            index.get("missing")
    with pytest.raises(ValueError, match="closed"):
        index.stats()  # type: ignore[union-attr]


def test_manifest_index_build_stream_accepts_one_pass_entries(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    reference_path = tmp_path / "reference.db"
    with ManifestIndex.build(manifest, reference_path) as reference:
        digest = reference.manifest_digest
        corpus_hash = reference.corpus_hash
    calls = 0

    def entries():
        nonlocal calls
        for entry in manifest.records:
            calls += 1
            yield entry

    stream_path = tmp_path / "stream.db"
    with ManifestIndex.build_stream(
        entries(),
        stream_path,
        manifest_digest=digest,
        corpus_hash=corpus_hash,
        record_count=2,
    ) as index:
        assert calls == 2
        assert index.record_count == 2
        index.verify(manifest)
        assert [row.record_id for row in index.query(id_prefix="b")] == ["beta"]


def test_manifest_index_build_stream_rejects_identity_and_count_errors(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    path = tmp_path / "stream.db"
    with pytest.raises(ValueError, match="manifest_digest"):
        ManifestIndex.build_stream(
            iter(manifest.records),
            path,
            manifest_digest="bad",
            corpus_hash=manifest.corpus_hash,
            record_count=2,
        )
    with pytest.raises(ValueError, match="yielded 2 entries; expected 1"):
        ManifestIndex.build_stream(
            iter(manifest.records),
            path,
            manifest_digest="0" * 64,
            corpus_hash=manifest.corpus_hash,
            record_count=1,
        )
    assert not path.exists()


def test_manifest_index_detects_manifest_mismatch_and_bad_filters(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, 1)
    changed = _manifest(tmp_path, 3)
    path = tmp_path / "index.db"
    index = ManifestIndex.build(manifest, path)
    try:
        with pytest.raises(ValueError, match="digest"):
            index.verify(changed)
        with pytest.raises(ValueError, match="limit"):
            index.query(limit=0)
        with pytest.raises(ValueError, match="id_prefix"):
            index.query(id_prefix="")
        with pytest.raises(ValueError, match="field_path"):
            index.query(field_path="")
        with pytest.raises(ValueError, match="field_hash"):
            index.query(field_hash="")
        assert [row.record_id for row in index.query(field_hash=manifest.records[0].field_hashes["/text"])] == ["alpha"]
    finally:
        index.close()


def test_manifest_index_accepts_empty_manifest_and_rejects_untrusted_files(tmp_path: Path) -> None:
    source = tmp_path / "empty.jsonl"
    source.write_text("", encoding="utf-8")
    manifest = build_manifest(source)
    path = tmp_path / "empty.db"
    with ManifestIndex.build(manifest, path) as index:
        assert index.record_count == 0
        assert index.query() == ()
    malformed = tmp_path / "malformed.db"
    malformed.write_text("not sqlite", encoding="utf-8")
    with pytest.raises(ValueError, match=r"not a supported|file is not a database"):
        ManifestIndex(malformed)


def test_manifest_index_cli_build_verify_and_query(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "records.manifest.json"
    manifest.save(manifest_path)
    index_path = tmp_path / "records.index.db"
    assert run(["index", str(manifest_path), str(index_path)]) == 0
    assert run(["verify-index", str(manifest_path), str(index_path)]) == 0
    output = tmp_path / "query.json"
    assert run(["query-index", str(index_path), "--field", "/text", "--output", str(output)]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert [row["record_id"] for row in payload["records"]] == ["alpha", "beta"]
    assert "manifest_digest" in capsys.readouterr().out


def test_duplicate_field_groups_are_authenticated_and_cli_visible(tmp_path: Path) -> None:
    source = tmp_path / "duplicates.jsonl"
    source.write_text(
        "\n".join(json.dumps({"id": identifier, "text": "same"}) for identifier in ("alpha", "beta", "gamma")) + "\n",
        encoding="utf-8",
    )
    manifest = build_manifest(source)
    path = tmp_path / "index.db"
    with ManifestIndex.build(manifest, path) as index:
        groups = index.duplicate_fields(field_path="/text")
        assert len(groups) == 1
        assert groups[0].record_ids == ("alpha", "beta", "gamma")
        assert groups[0].to_dict()["field_path"] == "/text"
        digest = groups[0].field_hash
        assert [item.record_id for item in index.query(field_hash=digest)] == [
            "alpha",
            "beta",
            "gamma",
        ]
    output = tmp_path / "duplicates.json"
    assert run(["duplicate-fields", str(path), "--field", "/text", "--output", str(output)]) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["groups"][0]["field_hash"] == digest
