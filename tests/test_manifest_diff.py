import json
from pathlib import Path

import pytest

from corpusledger.canonical import CanonicalPolicy
from corpusledger.diff import compare
from corpusledger.errors import ManifestError
from corpusledger.manifest import Manifest, build_manifest
from corpusledger.privacy import PrivacyConfig
from corpusledger.reporting import render_json, render_markdown


def corpus(path: Path, values: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(value, ensure_ascii=False) for value in values) + "\n", encoding="utf-8")


def test_manifest_roundtrip_and_determinism(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "一", "text": "你好"}, {"id": "2", "text": "world"}])
    first = build_manifest(source)
    second = build_manifest(source)
    assert first.corpus_hash == second.corpus_hash
    target = tmp_path / "manifest.json"
    first.save(target)
    initial_bytes = target.read_bytes()
    Manifest.load(target).save(target)
    assert target.read_bytes() == initial_bytes


def test_unicode_equivalent_keys_and_values_normalize_all_derived_sections(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "café": "résumé"}])
    composed = build_manifest(source)
    corpus(source, [{"id": "1", "cafe\u0301": "re\u0301sume\u0301"}])
    decomposed = build_manifest(source)
    assert composed.corpus_hash == decomposed.corpus_hash
    assert composed.records == decomposed.records
    assert composed.schema == decomposed.schema
    assert compare(composed, decomposed).has_changes is False


def test_diff_records_fields_schema_and_privacy(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "text": "old", "score": 1}, {"id": "gone", "text": "x"}])
    before = build_manifest(source)
    corpus(source, [{"id": "1", "text": "new", "score": "1"}, {"id": "added", "token": "Z8x4Qm2Vn9P0rT7sK3jH5dF1"}])
    after = build_manifest(source)
    result = compare(before, after)
    assert result.added_records == ("added",)
    assert result.removed_records == ("gone",)
    assert result.changed_records["1"]["fields"] == ["/score", "/text"]
    assert "/score" in result.schema["changed_fields"]
    assert result.privacy_findings_added
    assert '"has_changes": true' in render_json(result)
    assert "# CorpusLedger diff" in render_markdown(result)


def test_order_only_is_separate(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    values = [{"id": "1", "text": "a"}, {"id": "2", "text": "b"}]
    corpus(source, values)
    before = build_manifest(source)
    corpus(source, list(reversed(values)))
    after = build_manifest(source)
    result = compare(before, after)
    assert before.corpus_hash == after.corpus_hash
    assert result.order_only is True
    assert not result.changed_records


def test_empty_object_change_has_a_field_path(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "meta": {}}])
    before = build_manifest(source)
    corpus(source, [{"id": "1", "meta": {"language": "en"}}])
    after = build_manifest(source)
    assert compare(before, after).changed_records["1"]["fields"] == ["/meta", "/meta/language"]


def test_json_pointer_paths_do_not_collide_with_dotted_keys(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "a": {"b": 1}, "a.b": 2}])
    before = build_manifest(source)
    corpus(source, [{"id": "1", "a": {"b": 9}, "a.b": 2}])
    after = build_manifest(source)
    assert compare(before, after).changed_records["1"]["fields"] == ["/a/b"]


def test_json_pointer_escapes_slash_and_tilde_keys(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "a/b": 1, "til~de": 2}])
    before = build_manifest(source)
    corpus(source, [{"id": "1", "a/b": 3, "til~de": 4}])
    after = build_manifest(source)
    assert compare(before, after).changed_records["1"]["fields"] == ["/a~1b", "/til~0de"]


def test_incompatible_manifests_and_invalid_file(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1"}])
    normal = build_manifest(source)
    sorted_lists = build_manifest(source, policy=CanonicalPolicy(list_strategy="sort"))
    with pytest.raises(ManifestError, match="different hash"):
        compare(normal, sorted_lists)
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{}", encoding="utf-8")
    with pytest.raises(ManifestError, match="format"):
        Manifest.load(invalid)


def test_privacy_configuration_is_recorded_and_checked(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "private_note": "short"}])
    quiet = build_manifest(source, privacy=PrivacyConfig(frozenset(), 100, 9.0))
    strict = build_manifest(source, privacy=PrivacyConfig(frozenset({"private_note"}), 100, 9.0))
    assert quiet.privacy_metadata != strict.privacy_metadata
    with pytest.raises(ManifestError, match="different privacy"):
        compare(quiet, strict)


def test_manifest_load_rejects_unknown_nested_fields_and_duplicate_entries(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1"}])
    manifest = build_manifest(source)
    raw = manifest.to_dict()
    raw["records"][0]["future"] = True
    path = tmp_path / "invalid.manifest.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ManifestError, match="unknown fields"):
        Manifest.load(path)
    raw = manifest.to_dict()
    raw["records"].append(dict(raw["records"][0]))
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ManifestError, match="duplicate manifest record"):
        Manifest.load(path)


def test_manifest_load_wraps_strict_structure_errors(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1", "token": "Z8x4Qm2Vn9P0rT7sK3jH5dF1"}])
    baseline = build_manifest(source).to_dict()
    mutations = [
        lambda value: value.pop("schema"),
        lambda value: value.__setitem__("source", ""),
        lambda value: value.__setitem__("schema", []),
        lambda value: value.__setitem__("corpus_hash", "bad"),
        lambda value: value["hash_metadata"].pop("policy"),
        lambda value: value["hash_metadata"].__setitem__("algorithm", "md5"),
        lambda value: value["hash_metadata"].__setitem__("canonical_version", "999"),
        lambda value: value["privacy_metadata"].__setitem__("extra", True),
        lambda value: value["privacy_metadata"].__setitem__("version", "999"),
        lambda value: value.__setitem__("records", {}),
        lambda value: value["records"][0].__setitem__("record_id", "e\u0301"),
        lambda value: value["records"][0].__setitem__("position", 0),
        lambda value: value.__setitem__("privacy_findings", {}),
        lambda value: value["privacy_findings"][0].__setitem__("record_id", "unknown"),
        lambda value: value["privacy_findings"][0].__setitem__("path", "not-a-pointer"),
    ]
    path = tmp_path / "invalid.manifest.json"
    for mutate in mutations:
        value = json.loads(json.dumps(baseline))
        mutate(value)
        path.write_text(json.dumps(value), encoding="utf-8")
        with pytest.raises(ManifestError):
            Manifest.load(path)
    with pytest.raises(ManifestError, match="cannot load"):
        Manifest.load(tmp_path / "missing.json")
    path.write_text('{"format":"corpusledger/1","format":"corpusledger/1"}', encoding="utf-8")
    with pytest.raises(ManifestError, match="duplicate object key"):
        Manifest.load(path)
    with pytest.raises(ManifestError, match="cannot save"):
        build_manifest(source).save(tmp_path)


def test_reader_metadata_and_numeric_manifest_boundaries_are_strict(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "1"}])
    baseline = build_manifest(source).to_dict()
    path = tmp_path / "invalid.manifest.json"

    baseline["reader_metadata"] = {"name": "plugin", "version": "1\n"}
    path.write_text(json.dumps(baseline), encoding="utf-8")
    with pytest.raises(ManifestError, match="control characters"):
        Manifest.load(path)

    path.write_text('{"format":"corpusledger/1","future":1e400}', encoding="utf-8")
    with pytest.raises(ManifestError, match="finite binary64"):
        Manifest.load(path)


def test_markdown_escapes_untrusted_manifest_identifiers(tmp_path: Path) -> None:
    source = tmp_path / "data.jsonl"
    corpus(source, [{"id": "safe"}])
    before = build_manifest(source)
    hostile = "bad`\n## injected"
    corpus(source, [{"id": "safe"}, {"id": hostile}])
    markdown = render_markdown(compare(before, build_manifest(source)))
    assert "`bad`\n## injected`" not in markdown
    assert "bad&#96;<br>## injected" in markdown
