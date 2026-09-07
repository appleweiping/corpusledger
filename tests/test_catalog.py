import json
from pathlib import Path

import pytest

from corpusledger import SnapshotCatalog, build_manifest
from corpusledger.cli import run


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


def test_catalog_cli_register_list_lineage_and_diff(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    manifest_one_path = tmp_path / "one.json"
    manifest_two_path = tmp_path / "two.json"
    source.write_text('{"id":"a","value":1}\n', encoding="utf-8")
    one = build_manifest(source)
    one.save(manifest_one_path)
    source.write_text('{"id":"a","value":2}\n', encoding="utf-8")
    two = build_manifest(source)
    two.save(manifest_two_path)
    database = tmp_path / "catalog.db"
    assert run(["catalog", "register", str(database), "dataset", str(manifest_one_path), "--tag", "raw"]) == 0
    assert (
        run(
            [
                "catalog",
                "register",
                str(database),
                "dataset",
                str(manifest_two_path),
                "--parent",
                one.corpus_hash,
                "--tag",
                "latest",
            ]
        )
        == 0
    )
    listing = tmp_path / "listing.json"
    assert run(["catalog", "list", str(database), "--name", "dataset"]) == 0
    assert run(["catalog", "lineage", str(database), two.corpus_hash, "--output", str(listing)]) == 0
    assert len(json.loads(listing.read_text(encoding="utf-8"))) == 2
    diff = tmp_path / "diff.json"
    assert run(["catalog", "diff", str(database), "dataset", "1", "2", "--output", str(diff)]) == 0
    changed = json.loads(diff.read_text(encoding="utf-8"))["changed_records"]["a"]
    assert changed["fields"] == ["/value"]
    assert changed["from"] == one.records[0].hash
    assert changed["to"] == two.records[0].hash
