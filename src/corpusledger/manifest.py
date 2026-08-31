"""Manifest model, deterministic persistence, and snapshot construction."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_json, canonicalize
from .errors import ManifestError
from .fingerprint import (
    HashAlgorithm,
    corpus_fingerprint,
    file_fingerprints,
    metadata,
    order_fingerprint,
    record_fingerprint,
)
from .paths import join_pointer
from .privacy import PRIVACY_VERSION, PrivacyConfig, scan_records
from .readers import Record, read_corpus
from .schema import infer_schema
from .strictjson import StrictJsonError, object_without_duplicates, reject_constant

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

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable representation."""
        result = asdict(self)
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
            )
        except (KeyError, TypeError, ValueError) as exc:
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
) -> Manifest:
    """Read a corpus and build its complete manifest."""
    policy = policy or CanonicalPolicy()
    privacy = privacy or PrivacyConfig()
    root = Path(source).resolve()
    records = read_corpus(root, id_field, policy=policy, exclude_paths=exclude_paths)
    canonical_records: dict[str, dict[str, Any]] = {}
    for record in records:
        normalized = canonicalize(record.data, policy)
        if not isinstance(normalized, dict):
            raise ManifestError(f"record {record.record_id!r} did not normalize to a JSON object")
        canonical_records[record.record_id] = normalized
    hashes = {record.record_id: record_fingerprint(record, policy, algorithm) for record in records}
    entries = tuple(
        RecordEntry(
            record_id=record.record_id,
            hash=hashes[record.record_id],
            source=_relative_source(record, root),
            position=record.position,
            field_hashes={
                path: record_fingerprint(
                    Record(record.record_id, {"value": value}, record.source, record.position),
                    policy,
                    algorithm,
                )
                for path, value in _flatten(canonical_records[record.record_id]).items()
            },
        )
        for record in records
    )
    file_hashes = file_fingerprints(records, policy, algorithm)
    relative_files = {_relative_path(Path(path), root): value for path, value in file_hashes.items()}
    hash_meta = asdict(metadata(policy, algorithm))
    hash_meta["canonical_version"] = CANONICAL_VERSION
    return Manifest(
        format=MANIFEST_FORMAT,
        source=str(root),
        id_field=id_field,
        hash_metadata=hash_meta,
        privacy_metadata={"version": PRIVACY_VERSION, "config": privacy.to_dict()},
        corpus_hash=corpus_fingerprint(hashes, policy, algorithm),
        order_hash=order_fingerprint((record.record_id for record in records), policy, algorithm),
        files=relative_files,
        records=entries,
        schema=infer_schema(canonical_records[record.record_id] for record in records),
        privacy_findings=tuple(
            scan_records(
                ((record.record_id, canonical_records[record.record_id]) for record in records),
                privacy,
            )
        ),
    )


def _relative_path(path: Path, root: Path) -> str:
    base = root if root.is_dir() else root.parent
    try:
        return path.resolve().relative_to(base).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _relative_source(record: Record, root: Path) -> str:
    return _relative_path(Path(record.source), root)
