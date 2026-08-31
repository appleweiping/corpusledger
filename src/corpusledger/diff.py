"""Auditable comparisons between two corpus manifests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .canonical import CanonicalPolicy, canonical_json
from .errors import ManifestError
from .manifest import Manifest, RecordEntry
from .schema import schema_drift


@dataclass(frozen=True)
class CorpusDiff:
    """Structured record, field, schema, and order changes."""

    added_records: tuple[str, ...]
    removed_records: tuple[str, ...]
    changed_records: dict[str, dict[str, Any]]
    schema: dict[str, Any]
    order_changed: bool
    order_only: bool
    privacy_findings_added: tuple[dict[str, Any], ...]

    @property
    def has_changes(self) -> bool:
        """Whether any content, order, schema, or new risk changed."""
        return bool(
            self.added_records
            or self.removed_records
            or self.changed_records
            or self.order_changed
            or self.privacy_findings_added
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-compatible result."""
        return {
            "added_records": list(self.added_records),
            "changed_records": self.changed_records,
            "has_changes": self.has_changes,
            "order_changed": self.order_changed,
            "order_only": self.order_only,
            "privacy_findings_added": list(self.privacy_findings_added),
            "removed_records": list(self.removed_records),
            "schema": self.schema,
        }


def _entry_map(manifest: Manifest) -> dict[str, RecordEntry]:
    return {record.record_id: record for record in manifest.records}


def compare(before: Manifest, after: Manifest) -> CorpusDiff:
    """Compare compatible manifests and classify all observable changes."""
    if before.hash_metadata != after.hash_metadata:
        raise ManifestError("cannot diff manifests produced with different hash or canonicalization settings")
    if before.privacy_metadata != after.privacy_metadata:
        raise ManifestError("cannot diff manifests produced with different privacy settings")
    old, new = _entry_map(before), _entry_map(after)
    old_ids, new_ids = set(old), set(new)
    changed: dict[str, dict[str, Any]] = {}
    for record_id in sorted(old_ids & new_ids):
        if old[record_id].hash == new[record_id].hash:
            continue
        old_fields, new_fields = old[record_id].field_hashes, new[record_id].field_hashes
        field_names = set(old_fields) | set(new_fields)
        changed[record_id] = {
            "fields": [name for name in sorted(field_names) if old_fields.get(name) != new_fields.get(name)],
            "from": old[record_id].hash,
            "to": new[record_id].hash,
        }
    order_changed = before.order_hash != after.order_hash
    content_changed = bool(old_ids ^ new_ids or changed)
    storage_policy = CanonicalPolicy(unicode_form="none")
    old_risks = {canonical_json(item, storage_policy) for item in before.privacy_findings}
    new_risks = tuple(item for item in after.privacy_findings if canonical_json(item, storage_policy) not in old_risks)
    return CorpusDiff(
        added_records=tuple(sorted(new_ids - old_ids)),
        removed_records=tuple(sorted(old_ids - new_ids)),
        changed_records=changed,
        schema=schema_drift(before.schema, after.schema),
        order_changed=order_changed,
        order_only=order_changed and not content_changed,
        privacy_findings_added=new_risks,
    )
