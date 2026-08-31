"""Manifest model, deterministic persistence, and snapshot construction."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_json, canonicalize
from .errors import CanonicalizationError, InputError, ManifestError
from .fingerprint import (
    CanonicalSequenceHasher,
    HashAlgorithm,
    metadata,
    record_fingerprint,
)
from .paths import join_pointer
from .privacy import PRIVACY_VERSION, PrivacyConfig, scan_record
from .readers import (
    ReaderAdapter,
    Record,
    iter_corpus,
    validate_reader_adapter,
    validate_reader_identity,
)
from .schema import SchemaAccumulator
from .strictjson import (
    StrictJsonError,
    bounded_int,
    finite_float,
    object_without_duplicates,
    reject_constant,
)

MANIFEST_FORMAT = "corpusledger/1"
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _flatten(value: Any, path: str = "") -> dict[str, Any]:
    output: dict[str, Any] = {}
    if isinstance(value, dict):
        if not value and path:
            output[path] = value
        for key in sorted(value):
            child = join_pointer(path, str(key))
            output.update(_flatten(value[key], child))
    else:
        output[path] = value
    return output


@dataclass(frozen=True)
class RecordEntry:
    """Manifest data for one record."""

    record_id: str
    hash: str
    source: str
    position: int
    field_hashes: dict[str, str]


@dataclass(frozen=True)
class Manifest:
    """Portable, deterministic description of a corpus snapshot."""

    format: str
    source: str
    id_field: str
    hash_metadata: dict[str, Any]
    privacy_metadata: dict[str, Any]
    corpus_hash: str
    order_hash: str
    files: dict[str, str]
    records: tuple[RecordEntry, ...]
    schema: dict[str, Any]
    privacy_findings: tuple[dict[str, Any], ...]
    reader_metadata: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable representation."""
        result = asdict(self)
        if self.reader_metadata is None:
            result.pop("reader_metadata")
        result["records"] = [asdict(record) for record in self.records]
        result["privacy_findings"] = list(self.privacy_findings)
        return result

    def save(self, path: str | Path) -> None:
        """Write stable, compact UTF-8 JSON."""
        destination = Path(path)
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            storage_policy = CanonicalPolicy(unicode_form="none")
            destination.write_text(canonical_json(self.to_dict(), storage_policy) + "\n", encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise ManifestError(f"cannot save manifest {path}: {exc}") from exc

    @classmethod
    def load(cls, path: str | Path) -> Manifest:
        """Load and minimally validate a versioned manifest."""
        try:
            raw = json.loads(
                Path(path).read_text(encoding="utf-8"),
                object_pairs_hook=object_without_duplicates,
                parse_constant=reject_constant,
                parse_float=finite_float,
                parse_int=bounded_int,
            )
        except (OSError, UnicodeError, json.JSONDecodeError, StrictJsonError) as exc:
            raise ManifestError(f"cannot load manifest {path}: {exc}") from exc
        if not isinstance(raw, dict) or raw.get("format") != MANIFEST_FORMAT:
            raise ManifestError(f"unsupported or missing manifest format in {path}")
        required = {
            "source",
            "id_field",
            "hash_metadata",
            "privacy_metadata",
            "corpus_hash",
            "order_hash",
            "files",
            "records",
            "schema",
        }
        missing = required - raw.keys()
        if missing:
            raise ManifestError(f"manifest missing keys: {', '.join(sorted(missing))}")
        try:
            source = _required_text(raw, "source")
            id_field = _required_text(raw, "id_field")
            hash_metadata = _validate_hash_metadata(raw["hash_metadata"])
            policy = CanonicalPolicy(**hash_metadata["policy"])
            privacy_metadata = _validate_privacy_metadata(raw["privacy_metadata"])
            reader_metadata = _validate_reader_metadata(raw.get("reader_metadata"))
            corpus_hash = _validate_digest(raw["corpus_hash"], "corpus_hash")
            order_hash = _validate_digest(raw["order_hash"], "order_hash")
            files = _validate_hash_mapping(raw["files"], "files")
            entries = _validate_record_entries(raw["records"], policy)
            schema = _required_mapping(raw["schema"], "schema")
            findings = _validate_findings(raw.get("privacy_findings", []), {entry.record_id for entry in entries})
            return cls(
                format=raw["format"],
                source=source,
                id_field=id_field,
                hash_metadata=hash_metadata,
                privacy_metadata=privacy_metadata,
                corpus_hash=corpus_hash,
                order_hash=order_hash,
                files=files,
                records=entries,
                schema=schema,
                privacy_findings=findings,
                reader_metadata=reader_metadata,
            )
        except (InputError, KeyError, TypeError, ValueError) as exc:
            raise ManifestError(f"invalid manifest structure in {path}: {exc}") from exc


def _required_text(value: Mapping[str, Any], key: str) -> str:
    item = value[key]
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _required_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be an object with string keys")
    return cast(dict[str, Any], value)


def _validate_digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _HEX_DIGEST.fullmatch(value):
        raise ValueError(f"{name} must be a 64-character lowercase hexadecimal digest")
    return value


def _validate_hash_mapping(value: Any, name: str) -> dict[str, str]:
    mapping = _required_mapping(value, name)
    return {key: _validate_digest(item, f"{name}.{key}") for key, item in mapping.items()}


def _validate_hash_metadata(value: Any) -> dict[str, Any]:
    metadata_value = _required_mapping(value, "hash_metadata")
    if set(metadata_value) != {"algorithm", "canonical_version", "policy"}:
        raise ValueError("hash_metadata has missing or unknown fields")
    algorithm = metadata_value["algorithm"]
    if algorithm not in {"sha256", "blake2b"}:
        raise ValueError("hash_metadata.algorithm is unsupported")
    if metadata_value["canonical_version"] != CANONICAL_VERSION:
        raise ValueError("hash_metadata.canonical_version is unsupported")
    policy_value = _required_mapping(metadata_value["policy"], "hash_metadata.policy")
    policy = CanonicalPolicy(**policy_value)
    return {
        "algorithm": algorithm,
        "canonical_version": CANONICAL_VERSION,
        "policy": policy.to_dict(),
    }


def _validate_privacy_metadata(value: Any) -> dict[str, Any]:
    metadata_value = _required_mapping(value, "privacy_metadata")
    if set(metadata_value) != {"version", "config"}:
        raise ValueError("privacy_metadata has missing or unknown fields")
    if metadata_value["version"] != PRIVACY_VERSION:
        raise ValueError("privacy_metadata.version is unsupported")
    config_value = _required_mapping(metadata_value["config"], "privacy_metadata.config")
    config = PrivacyConfig.from_dict(config_value)
    if config_value != config.to_dict():
        raise ValueError("privacy_metadata.config is not canonical")
    return {"version": PRIVACY_VERSION, "config": config.to_dict()}


def _validate_reader_metadata(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    metadata_value = _required_mapping(value, "reader_metadata")
    if set(metadata_value) != {"name", "version"}:
        raise ValueError("reader_metadata has missing or unknown fields")
    name, version = validate_reader_identity(metadata_value.get("name"), metadata_value.get("version"))
    return {"name": name, "version": version}


def _validate_record_entries(value: Any, policy: CanonicalPolicy) -> tuple[RecordEntry, ...]:
    if not isinstance(value, list):
        raise ValueError("records must be an array")
    required = {"record_id", "hash", "source", "position", "field_hashes"}
    entries: list[RecordEntry] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        mapping = _required_mapping(item, f"records[{index}]")
        if set(mapping) != required:
            raise ValueError(f"records[{index}] has missing or unknown fields")
        record_id = _required_text(mapping, "record_id")
        normalized_id = canonicalize(record_id, policy)
        if normalized_id != record_id:
            raise ValueError(f"records[{index}].record_id is not Unicode-normalized")
        if record_id in seen:
            raise ValueError(f"duplicate manifest record ID {record_id!r}")
        seen.add(record_id)
        source = _required_text(mapping, "source")
        position = mapping["position"]
        if isinstance(position, bool) or not isinstance(position, int) or position < 1:
            raise ValueError(f"records[{index}].position must be a positive integer")
        entries.append(
            RecordEntry(
                record_id=record_id,
                hash=_validate_digest(mapping["hash"], f"records[{index}].hash"),
                source=source,
                position=position,
                field_hashes=_validate_hash_mapping(mapping["field_hashes"], f"records[{index}].field_hashes"),
            )
        )
    return tuple(entries)


def _validate_findings(value: Any, record_ids: set[str]) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise ValueError("privacy_findings must be an array")
    findings: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        finding = _required_mapping(item, f"privacy_findings[{index}]")
        for key in ("kind", "path", "record_id"):
            _required_text(finding, key)
        if finding["record_id"] not in record_ids:
            raise ValueError(f"privacy_findings[{index}] references an unknown record ID")
        if not str(finding["path"]).startswith("/"):
            raise ValueError(f"privacy_findings[{index}].path must be a JSON Pointer")
        findings.append(finding)
    return tuple(findings)


def build_manifest(
    source: str | Path,
    *,
    id_field: str = "id",
    algorithm: HashAlgorithm = "sha256",
    policy: CanonicalPolicy | None = None,
    privacy: PrivacyConfig | None = None,
    exclude_paths: Iterable[str | Path] = (),
    reader: ReaderAdapter | None = None,
) -> Manifest:
    """Stream a corpus into a complete format-version-1 manifest.

    Default JSONL input is never materialized as raw records. Memory retains the
    information required by the manifest itself, the global duplicate-ID index,
    schema aggregates, and privacy findings. JSON arrays are decoded as a whole;
    ``list_strategy='sort'`` also retains canonical sequence members to preserve
    the exact version-1 hash semantics.
    """

    policy = policy or CanonicalPolicy()
    privacy = privacy or PrivacyConfig()
    root = Path(source).resolve()
    active_reader = validate_reader_adapter(reader) if reader is not None else None
    source_base = root if root.is_dir() else root.parent
    relative_sources: dict[str, str] = {}

    def relative_source(source_path: str) -> str:
        cached = relative_sources.get(source_path)
        if cached is not None:
            return cached
        path = Path(source_path)
        try:
            value = path.relative_to(source_base).as_posix()
        except ValueError:
            value = path.as_posix()
        relative_sources[source_path] = value
        return value

    entries: list[RecordEntry] = []
    schema = SchemaAccumulator()
    findings: list[dict[str, Any]] = []
    order_hasher = CanonicalSequenceHasher(policy, algorithm)
    relative_files: dict[str, str] = {}
    current_source: str | None = None
    file_hasher: CanonicalSequenceHasher | None = None

    def finish_file() -> None:
        nonlocal file_hasher
        if current_source is not None and file_hasher is not None:
            relative_files[relative_source(current_source)] = file_hasher.finish()
            file_hasher = None

    for record in iter_corpus(
        root,
        id_field,
        policy=policy,
        exclude_paths=exclude_paths,
        reader=active_reader,
    ):
        if record.source != current_source:
            finish_file()
            current_source = record.source
            file_hasher = CanonicalSequenceHasher(policy, algorithm)
        try:
            normalized = canonicalize(record.data, policy)
        except CanonicalizationError as exc:
            raise ManifestError(
                f"cannot canonicalize {record.source} record {record.position} ({record.record_id!r}): {exc}"
            ) from exc
        if not isinstance(normalized, dict):
            raise ManifestError(f"record {record.record_id!r} did not normalize to a JSON object")
        normalized_record = Record(record.record_id, normalized, record.source, record.position)
        record_hash = record_fingerprint(normalized_record, policy, algorithm)
        entries.append(
            RecordEntry(
                record_id=record.record_id,
                hash=record_hash,
                source=relative_source(record.source),
                position=record.position,
                field_hashes={
                    path: record_fingerprint(
                        Record(record.record_id, {"value": value}, record.source, record.position),
                        policy,
                        algorithm,
                    )
                    for path, value in _flatten(normalized).items()
                },
            )
        )
        schema.observe(normalized)
        findings.extend(scan_record(record.record_id, normalized, privacy))
        order_hasher.add(record.record_id)
        assert file_hasher is not None
        file_hasher.add({"id": record.record_id, "record": canonical_json(normalized, policy)})
    finish_file()

    corpus_hasher = CanonicalSequenceHasher(policy, algorithm)
    for entry in sorted(entries, key=lambda item: item.record_id):
        corpus_hasher.add({"id": entry.record_id, "hash": entry.hash})
    hash_meta = asdict(metadata(policy, algorithm))
    hash_meta["canonical_version"] = CANONICAL_VERSION
    return Manifest(
        format=MANIFEST_FORMAT,
        source=str(root),
        id_field=id_field,
        hash_metadata=hash_meta,
        privacy_metadata={"version": PRIVACY_VERSION, "config": privacy.to_dict()},
        corpus_hash=corpus_hasher.finish(),
        order_hash=order_hasher.finish(),
        files=relative_files,
        records=tuple(entries),
        schema=schema.to_dict(),
        privacy_findings=tuple(
            sorted(
                findings,
                key=lambda item: (str(item["record_id"]), str(item["path"]), str(item["kind"])),
            )
        ),
        reader_metadata=(
            {"name": active_reader.name, "version": active_reader.version} if active_reader is not None else None
        ),
    )
