"""Compare two snapshots record by record and field by field."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .errors import SnapshotError
from .schema import schema_drift
from .snapshot import Entry, Snapshot


@dataclass(frozen=True)
class Change:
    """A record present in both versions whose content differs."""

    before: Entry
    after: Entry
    fields: tuple[str, ...]


@dataclass
class DatasetDiff:
    """Everything that differs between two versions of a dataset."""

    before: Snapshot
    after: Snapshot
    unchanged: int
    changed: list[Change]
    rekeyed: list[Change]
    removed: list[Entry]
    added: list[Entry]
    schema: dict[str, Any]
    order_changed: bool
    field_counts: Counter[str] = field(default_factory=Counter)

    @property
    def has_changes(self) -> bool:
        return bool(self.changed or self.rekeyed or self.removed or self.added)

    def to_dict(self) -> dict[str, Any]:
        def change(item: Change) -> dict[str, Any]:
            result: dict[str, Any] = {"id": item.after.id, "label": item.after.label}
            if item.before.id != item.after.id:
                result["before_id"] = item.before.id
                result["before_label"] = item.before.label
            result["fields"] = list(item.fields)
            return result

        return {
            "before": {"source": self.before.source, "records": len(self.before.entries)},
            "after": {"source": self.after.source, "records": len(self.after.entries)},
            "match": self.before.match.to_dict(),
            "ignore": list(self.before.ignore),
            "has_changes": self.has_changes,
            "counts": {
                "unchanged": self.unchanged,
                "changed": len(self.changed),
                "key_edited": len(self.rekeyed),
                "removed": len(self.removed),
                "added": len(self.added),
                "duplicates_before": self.before.duplicates,
                "duplicates_after": self.after.duplicates,
            },
            "fields_changed": dict(self.field_counts.most_common()),
            "schema": self.schema,
            "order_changed": self.order_changed,
            "changed": [change(item) for item in self.changed],
            "key_edited": [change(item) for item in self.rekeyed],
            "removed": [{"id": entry.id, "label": entry.label} for entry in self.removed],
            "added": [{"id": entry.id, "label": entry.label} for entry in self.added],
        }


def _changed_fields(before: Entry, after: Entry) -> tuple[str, ...]:
    names = before.fields.keys() | after.fields.keys()
    return tuple(
        sorted(name for name in names if before.fields.get(name) != after.fields.get(name))
    )


def compare(before: Snapshot, after: Snapshot) -> DatasetDiff:
    """Pair records by ID and classify every difference.

    With key matching, a removed and an added record whose non-key fields are
    identical are reported as one record whose key fields were edited.
    """
    if before.match != after.match:
        raise SnapshotError(
            f"snapshots match records differently ({before.match.describe()} vs "
            f"{after.match.describe()})"
        )
    if before.ignore != after.ignore:
        raise SnapshotError("snapshots ignore different fields")
    old = {entry.id: entry for entry in before.entries}
    new = {entry.id: entry for entry in after.entries}
    unchanged = 0
    changed: list[Change] = []
    counts: Counter[str] = Counter()
    for entry in after.entries:
        previous = old.get(entry.id)
        if previous is None:
            continue
        if previous.hash == entry.hash:
            unchanged += 1
            continue
        fields = _changed_fields(previous, entry)
        counts.update(fields)
        changed.append(Change(previous, entry, fields))
    removed = [entry for entry in before.entries if entry.id not in new]
    added = [entry for entry in after.entries if entry.id not in old]

    rekeyed: list[Change] = []
    if before.match.by == "key":
        by_rest_removed = Counter(entry.rest for entry in removed)
        by_rest_added = Counter(entry.rest for entry in added)
        unique = {
            rest
            for rest, count in by_rest_removed.items()
            if count == 1 and by_rest_added[rest] == 1
        }
        pending = {entry.rest: entry for entry in removed if entry.rest in unique}
        for entry in added:
            if entry.rest in pending:
                previous = pending.pop(entry.rest)
                fields = _changed_fields(previous, entry)
                counts.update(fields)
                rekeyed.append(Change(previous, entry, fields))
        paired_old = {item.before.id for item in rekeyed}
        paired_new = {item.after.id for item in rekeyed}
        removed = [entry for entry in removed if entry.id not in paired_old]
        added = [entry for entry in added if entry.id not in paired_new]

    common_old = [entry.id for entry in before.entries if entry.id in new]
    common_new = [entry.id for entry in after.entries if entry.id in old]
    return DatasetDiff(
        before=before,
        after=after,
        unchanged=unchanged,
        changed=changed,
        rekeyed=rekeyed,
        removed=removed,
        added=added,
        schema=schema_drift(before.schema, after.schema),
        order_changed=common_old != common_new,
        field_counts=counts,
    )
