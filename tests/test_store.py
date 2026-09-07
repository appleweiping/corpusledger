from pathlib import Path

import pytest

from corpusledger import ObjectStore, build_manifest, bundle_snapshot


def source(tmp_path: Path) -> Path:
    path = tmp_path / "corpus.jsonl"
    path.write_text('{"id":"a","text":"hello"}\n{"id":"b","text":"world"}\n', encoding="utf-8")
    return path


def test_object_store_deduplicates_and_verifies(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    digest = store.put_bytes(b"hello")
    assert store.put_bytes(b"hello") == digest
    assert store.read_bytes(digest) == b"hello"
    assert store.contains(digest) and store.digests() == (digest,)
    with pytest.raises(ValueError, match="digest"):
        store.read_bytes("bad")


def test_bundle_is_reproducible_and_manifest_is_stored(tmp_path: Path) -> None:
    corpus = source(tmp_path)
    manifest = build_manifest(corpus)
    store = ObjectStore(tmp_path / "objects")
    first = bundle_snapshot(manifest, corpus, tmp_path / "one.zip", store=store)
    second = bundle_snapshot(manifest, corpus, tmp_path / "two.zip", store=store)
    assert first.archive_digest == second.archive_digest
    assert first.manifest_digest in store.digests()
    assert first.files == ("manifest.json", "source/corpus.jsonl")
    assert first.bytes > 0


def test_bundle_rejects_missing_source_file(tmp_path: Path) -> None:
    corpus = source(tmp_path)
    manifest = build_manifest(corpus)
    corpus.unlink()
    with pytest.raises(FileNotFoundError):
        bundle_snapshot(manifest, corpus, tmp_path / "bundle.zip")
