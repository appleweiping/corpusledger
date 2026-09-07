from pathlib import Path

import pytest

from corpusledger import ObjectStore, build_manifest, bundle_snapshot, extract_bundle, verify_bundle


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


def test_bundle_verification_and_safe_extraction(tmp_path: Path) -> None:
    corpus = source(tmp_path)
    manifest = build_manifest(corpus)
    bundle = tmp_path / "bundle.zip"
    report = bundle_snapshot(manifest, corpus, bundle)
    verified = verify_bundle(bundle, expected_archive_digest=report.archive_digest)
    assert verified.archive_digest == report.archive_digest
    assert verified.files == report.files
    extracted = tmp_path / "out"
    extracted_report = extract_bundle(bundle, extracted)
    assert extracted_report == verified
    assert (extracted / "manifest.json").is_file()
    assert (extracted / "source" / "corpus.jsonl").read_text(encoding="utf-8") == corpus.read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        extract_bundle(bundle, extracted)


def test_bundle_verification_rejects_wrong_digest(tmp_path: Path) -> None:
    corpus = source(tmp_path)
    bundle = tmp_path / "bundle.zip"
    report = bundle_snapshot(build_manifest(corpus), corpus, bundle)
    with pytest.raises(ValueError, match="digest"):
        verify_bundle(bundle, expected_archive_digest="0" * 64)
    assert report.bytes == bundle.stat().st_size


def test_object_store_garbage_collection_defaults_to_a_plan(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    keep = store.put_bytes(b"keep")
    remove = store.put_bytes(b"remove")
    plan = store.collect_unreferenced((keep,))
    assert plan.kept == (keep,) and plan.removed == (remove,) and store.contains(remove)
    collected = store.collect_unreferenced((keep,), dry_run=False)
    assert collected == plan and not store.contains(remove) and store.contains(keep)
    with pytest.raises(KeyError, match="missing"):
        store.collect_unreferenced(("0" * 64,))
