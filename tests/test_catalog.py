from pathlib import Path

import pytest

from corpusledger import SnapshotCatalog, build_manifest


def make_manifest(path: Path, value: int):
    path.write_text(f'{{"id":"a","value":{value}}}\n', encoding="utf-8")
    return build_manifest(path)


def test_catalog_versions_lineage_and_diff(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    first, second = make_manifest(source, 1), make_manifest(source, 1)
    source.write_text('{"id":"a","value":2}\n', encoding="utf-8")
    second = build_manifest(source)
    with SnapshotCatalog(tmp_path / "catalog.db") as catalog:
        one = catalog.register("dataset", first, tags=("raw",))
        two = catalog.register("dataset", second, parent=first.corpus_hash, tags=("latest", "raw"))
        assert one.version == 1 and two.version == 2
        assert catalog.list("dataset") == (one, two)
        assert catalog.manifest("dataset", 1).corpus_hash == first.corpus_hash
        assert catalog.lineage(second.corpus_hash) == (two, one)
        assert catalog.diff("dataset", 1, 2).changed_records["a"]["fields"] == ["/value"]
        with pytest.raises(KeyError, match="parent"):
            catalog.register("broken", first, parent="missing")


def test_catalog_rejects_bad_names_tags_and_unknown_versions(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    manifest = make_manifest(source, 1)
    with SnapshotCatalog(":memory:") as catalog:
        with pytest.raises(ValueError, match="name"):
            catalog.register("bad name", manifest)
        with pytest.raises(ValueError, match="unique"):
            catalog.register("ok", manifest, tags=("x", "x"))
        with pytest.raises(KeyError):
            catalog.manifest("missing")
        with pytest.raises(KeyError):
            catalog.lineage("missing")


def test_catalog_close_is_idempotent(tmp_path: Path) -> None:
    catalog = SnapshotCatalog(tmp_path / "catalog.db")
    catalog.close()
    catalog.close()
    with pytest.raises(ValueError, match="closed"):
        catalog.list()
