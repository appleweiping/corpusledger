from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from corpusledger.canonical import CanonicalPolicy, canonicalize
from corpusledger.diff import compare
from corpusledger.errors import InputError, ManifestError
from corpusledger.fingerprint import (
    HashAlgorithm,
    corpus_fingerprint,
    file_fingerprints,
    metadata,
    order_fingerprint,
    record_fingerprint,
)
from corpusledger.manifest import MANIFEST_FORMAT, Manifest, RecordEntry, _flatten, build_manifest
from corpusledger.privacy import PRIVACY_VERSION, PrivacyConfig, scan_records
from corpusledger.readers import (
    Record,
    discover_inputs,
    iter_jsonl,
    load_reader_adapter,
    read_corpus,
    validate_reader_adapter,
)
from corpusledger.schema import infer_schema

_V01_GOLDEN_HASHES = {
    ("sha256", "preserve"): (
        "3815f82d0c93f7b37ae471509db38c4919f914a5a8c7a935670e3d3a477af56d",
        "02d8bc3008a9bb0dcc4b86d7fd3428ced792355c733c19756bec5a56dc61b2c5",
        "29d856ab5903e37aefa6a7607438f0980bd3e230dcf1bf9a66e3642ac8c45f38",
    ),
    ("sha256", "sort"): (
        "38f249bc8a0171744b4ed222aded57a492576aa38b6d45ad1246476bbf8f3a8c",
        "0473ef2dc0d324ab659d3580c1134e9d812035905c4781fdd6d529b0c6860e13",
        "27bd07c61516224662d5a01b1950e3afafe99d307fe9749584017e7f220b8054",
    ),
    ("blake2b", "preserve"): (
        "6bba38c8ebc2732a4498e66b493341c42e98977ef4ef8e8cb2da32622b8fab04",
        "22955598607b5b15d60afeb9fe8c254117f657f29bff10eb346da459a5ad4d20",
        "3d93462ee51f73c90d07d72160e5c45a0a2eefd3b4319b7e8be3f8a3672db8a1",
    ),
    ("blake2b", "sort"): (
        "fb5f3c1119af8ef05d163cd2fe7759b356ffcd1738d0ca6eb5df28c27af6770e",
        "d803f13f94cb4546f8f9d50368dfbb44ea46aa3db56fecfa2570a3ebf90f3a13",
        "2c5b4f17d2b1ebfb16ff25173bad9be78218e6a22e845e50caf3a76f395cae43",
    ),
}


def test_jsonl_iterator_is_lazy(tmp_path: Path) -> None:
    source = tmp_path / "lazy.jsonl"
    source.write_text('{"id":"first"}\n{broken}\n', encoding="utf-8")
    records = iter_jsonl(source)
    assert next(records).record_id == "first"
    with pytest.raises(InputError, match="line 2"):
        next(records)


def _legacy_manifest(
    source: Path,
    policy: CanonicalPolicy,
    algorithm: HashAlgorithm,
) -> Manifest:
    """Reconstruct the v0.1 materialized algorithm as a compatibility oracle."""

    records = read_corpus(source, policy=policy)
    normalized = {record.record_id: canonicalize(record.data, policy) for record in records}
    hashes = {record.record_id: record_fingerprint(record, policy, algorithm) for record in records}
    entries = tuple(
        RecordEntry(
            record_id=record.record_id,
            hash=hashes[record.record_id],
            source=source.name,
            position=record.position,
            field_hashes={
                path: record_fingerprint(
                    Record(record.record_id, {"value": value}, record.source, record.position),
                    policy,
                    algorithm,
                )
                for path, value in _flatten(normalized[record.record_id]).items()
            },
        )
        for record in records
    )
    file_hashes = file_fingerprints(records, policy, algorithm)
    hash_metadata = asdict(metadata(policy, algorithm))
    privacy = PrivacyConfig()
    return Manifest(
        format=MANIFEST_FORMAT,
        source=str(source.resolve()),
        id_field="id",
        hash_metadata=hash_metadata,
        privacy_metadata={"version": PRIVACY_VERSION, "config": privacy.to_dict()},
        corpus_hash=corpus_fingerprint(hashes, policy, algorithm),
        order_hash=order_fingerprint((record.record_id for record in records), policy, algorithm),
        files={source.name: next(iter(file_hashes.values()))},
        records=entries,
        schema=infer_schema(normalized[record.record_id] for record in records),
        privacy_findings=tuple(
            scan_records(
                ((record.record_id, normalized[record.record_id]) for record in records),
                privacy,
            )
        ),
    )


@pytest.mark.parametrize("algorithm", ["sha256", "blake2b"])
@pytest.mark.parametrize("list_strategy", ["preserve", "sort"])
def test_streaming_snapshot_is_byte_semantic_compatible_with_v01(
    tmp_path: Path,
    algorithm: HashAlgorithm,
    list_strategy: str,
) -> None:
    source = tmp_path / "compat.jsonl"
    source.write_text(
        "\n".join(
            (
                '{"id":"b","text":"cafe\\u0301","labels":["z","a"]}',
                '{"id":"a","text":"world","nested":{"value":1}}',
            )
        )
        + "\n",
        encoding="utf-8",
    )
    policy = CanonicalPolicy(list_strategy=list_strategy)  # type: ignore[arg-type]
    current = build_manifest(source, policy=policy, algorithm=algorithm)
    assert current == _legacy_manifest(source, policy, algorithm)
    assert (current.corpus_hash, current.order_hash, current.files[source.name]) == _V01_GOLDEN_HASHES[
        (algorithm, list_strategy)
    ]


class PipeReader:
    name = "pipe"
    version = "2026.1"
    extensions = frozenset({".pipe"})

    def iter_records(
        self,
        path: Path,
        *,
        id_field: str,
        policy: CanonicalPolicy,
    ) -> Iterator[Record]:
        del policy
        with path.open("r", encoding="utf-8") as stream:
            for position, line in enumerate(stream, start=1):
                identifier, text = line.rstrip("\n").split("|", 1)
                data = {id_field: identifier, "text": text}
                yield Record(identifier, data, path.as_posix(), position)


def test_injected_reader_is_recorded_roundtripped_and_compared(tmp_path: Path) -> None:
    source = tmp_path / "records.pipe"
    source.write_text("a|hello\nb|world\n", encoding="utf-8")
    manifest = build_manifest(source, reader=PipeReader())
    assert manifest.reader_metadata == {"name": "pipe", "version": "2026.1"}
    target = tmp_path / "manifest.json"
    manifest.save(target)
    assert Manifest.load(target) == manifest

    builtin = tmp_path / "records.jsonl"
    builtin.write_text('{"id":"a","text":"hello"}\n{"id":"b","text":"world"}\n', encoding="utf-8")
    with pytest.raises(ManifestError, match="reader adapters"):
        compare(build_manifest(builtin), manifest)


def test_adapter_records_are_validated(tmp_path: Path) -> None:
    class BadReader(PipeReader):
        name = "bad"

        def iter_records(
            self,
            path: Path,
            *,
            id_field: str,
            policy: CanonicalPolicy,
        ) -> Iterator[Record]:
            del id_field, policy
            yield Record("different", {"id": "actual"}, path.as_posix(), 1)

    source = tmp_path / "records.pipe"
    source.write_text("ignored", encoding="utf-8")
    with pytest.raises(InputError, match="inconsistent"):
        build_manifest(source, reader=BadReader())


def test_entry_point_reader_is_loaded_only_when_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = False

    class EntryPoint:
        def load(self) -> type[PipeReader]:
            nonlocal loaded
            loaded = True
            return PipeReader

    class EntryPoints:
        def select(self, **kwargs: Any) -> tuple[EntryPoint, ...]:
            assert kwargs == {"group": "corpusledger.readers", "name": "pipe"}
            return (EntryPoint(),)

    monkeypatch.setattr("corpusledger.readers.entry_points", EntryPoints)
    assert not loaded
    assert load_reader_adapter("pipe").version == "2026.1"
    assert loaded


@pytest.mark.parametrize(
    "candidate,match",
    [
        (SimpleNamespace(version="1", extensions=frozenset({".x"}), iter_records=lambda: None), "name"),
        (SimpleNamespace(name="x", extensions=frozenset({".x"}), iter_records=lambda: None), "version"),
        (SimpleNamespace(name="x", version="1", extensions=set({".x"}), iter_records=lambda: None), "frozenset"),
        (
            SimpleNamespace(name="x", version="1", extensions=frozenset({"X"}), iter_records=lambda: None),
            "lowercase ASCII",
        ),
        (
            SimpleNamespace(name=" x", version="1", extensions=frozenset({".x"}), iter_records=lambda: None),
            "whitespace",
        ),
        (SimpleNamespace(name="x", version="1\n", extensions=frozenset({".x"}), iter_records=lambda: None), "control"),
        (
            SimpleNamespace(name="x", version="1", extensions=frozenset({".tar.gz"}), iter_records=lambda: None),
            "file suffixes",
        ),
        (SimpleNamespace(name="x", version="1", extensions=frozenset({".x"})), "iter_records"),
    ],
)
def test_reader_adapter_contract_boundaries(candidate: object, match: str) -> None:
    with pytest.raises(InputError, match=match):
        validate_reader_adapter(candidate)


def test_entry_point_failure_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    class EntryPoints:
        values: tuple[object, ...] = ()

        def select(self, **kwargs: Any) -> tuple[object, ...]:
            del kwargs
            return self.values

    points = EntryPoints()
    monkeypatch.setattr("corpusledger.readers.entry_points", lambda: points)
    with pytest.raises(InputError, match="must not be empty"):
        load_reader_adapter("")
    with pytest.raises(InputError, match="not installed"):
        load_reader_adapter("missing")
    points.values = (object(), object())
    with pytest.raises(InputError, match="ambiguous"):
        load_reader_adapter("duplicate")

    class BrokenEntryPoint:
        def load(self) -> object:
            raise RuntimeError("sensitive plugin detail")

    points.values = (BrokenEntryPoint(),)
    with pytest.raises(InputError, match="RuntimeError") as captured:
        load_reader_adapter("broken")
    assert "sensitive plugin detail" not in str(captured.value)

    class WrongEntryPoint:
        def load(self) -> PipeReader:
            return PipeReader()

    points.values = (WrongEntryPoint(),)
    with pytest.raises(InputError, match="returned adapter named"):
        load_reader_adapter("other")

    def broken_discovery() -> object:
        raise RuntimeError("sensitive discovery detail")

    monkeypatch.setattr("corpusledger.readers.entry_points", broken_discovery)
    with pytest.raises(InputError, match=r"cannot discover.*RuntimeError") as captured:
        load_reader_adapter("pipe")
    assert "sensitive discovery detail" not in str(captured.value)


def test_discovery_and_adapter_output_boundaries(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(InputError, match="does not exist"):
        discover_inputs(missing)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(InputError, match="no supported files"):
        discover_inputs(empty)

    class ExplodingReader(PipeReader):
        name = "exploding"

        def iter_records(
            self,
            path: Path,
            *,
            id_field: str,
            policy: CanonicalPolicy,
        ) -> Iterator[Record]:
            del path, id_field, policy
            raise RuntimeError("record body must not be echoed")
            yield

    source = tmp_path / "source.pipe"
    source.write_text("ignored", encoding="utf-8")
    with pytest.raises(InputError, match="RuntimeError") as captured:
        build_manifest(source, reader=ExplodingReader())
    assert "record body" not in str(captured.value)

    class NotARecordReader(PipeReader):
        name = "not-a-record"

        def iter_records(  # type: ignore[override]
            self,
            path: Path,
            *,
            id_field: str,
            policy: CanonicalPolicy,
        ) -> Iterator[object]:
            del path, id_field, policy
            yield object()

    with pytest.raises(InputError, match="expected Record"):
        build_manifest(source, reader=NotARecordReader())
