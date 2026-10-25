"""Render a :class:`~corpusledger.diff.DatasetDiff` as a terminal summary or JSON."""

from __future__ import annotations

import json
from typing import Any

from .diff import Change, DatasetDiff
from .snapshot import Entry, Snapshot, flatten

_WIDTH = 66


def _value(snapshot: Snapshot, record_id: str, path: str) -> Any:
    if snapshot.rows is None or record_id not in snapshot.rows:
        return None
    return flatten(snapshot.rows[record_id]).get(path)


def _one_line(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return " ".join(text.split())


def excerpt(before: Any, after: Any, width: int = _WIDTH) -> tuple[str, str]:
    """Two one-line excerpts that start a little before the first difference."""
    old, new = _one_line(before), _one_line(after)
    start = 0
    while start < min(len(old), len(new)) and old[start] == new[start]:
        start += 1
    start = max(0, start - 12)

    def cut(text: str) -> str:
        piece = text[start : start + width]
        prefix = "..." if start > 0 else ""
        suffix = "..." if start + width < len(text) else ""
        return json.dumps(prefix + piece + suffix, ensure_ascii=False)

    return cut(old), cut(new)


def _short(value: Any, width: int = _WIDTH) -> str:
    text = " ".join(json.dumps(value, ensure_ascii=False).split())
    return text if len(text) <= width else text[: width - 3] + "..."


def _change_lines(diff: DatasetDiff, item: Change, kind: str) -> list[str]:
    lines = [f"  {kind:<11}{item.after.label}"]
    if diff.before.rows is None or diff.after.rows is None:
        lines.append(f"  {'':<11}fields: {', '.join(item.fields)}")
        return lines
    for path in item.fields[:3]:
        old = _value(diff.before, item.before.id, path)
        new = _value(diff.after, item.after.id, path)
        if old is None and path not in flatten(diff.before.rows[item.before.id]):
            lines.append(f"  {'':<11}{path}  added: {_short(new)}")
        elif new is None and path not in flatten(diff.after.rows[item.after.id]):
            lines.append(f"  {'':<11}{path}  removed: {_short(old)}")
        else:
            left, right = excerpt(old, new)
            lines.append(f"  {'':<11}{path}")
            lines.append(f"  {'':<13}- {left}")
            lines.append(f"  {'':<13}+ {right}")
    return lines


def _entry_lines(entries: list[Entry], kind: str, limit: int) -> list[str]:
    return [f"  {kind:<11}{entry.label}" for entry in entries[:limit]]


def render_text(diff: DatasetDiff, examples: int = 3) -> str:
    before, after = diff.before, diff.after
    match = before.match.describe()
    ignored = f", ignoring {', '.join(before.ignore)}" if before.ignore else ""
    width = max(len(f"{len(before.entries):,}"), len(f"{len(after.entries):,}"), 5)

    def row(name: str, value: int, note: str = "") -> str:
        return f"  {name:<12}{value:>{width},}{('   ' + note) if note else ''}"

    lines = [
        f"{before.source} -> {after.source}",
        f"records matched by {match}{ignored}",
        "",
        row("before", len(before.entries)),
        row("after", len(after.entries)),
        "",
        row("unchanged", diff.unchanged),
        row("changed", len(diff.changed)),
    ]
    if before.match.by == "key":
        lines.append(row("key edited", len(diff.rekeyed), "key fields edited, rest identical"))
    lines.append(row("removed", len(diff.removed)))
    lines.append(row("added", len(diff.added)))
    for name, snapshot in (("before", before), ("after", after)):
        if snapshot.duplicates:
            lines.append(
                f"  note: {snapshot.duplicates:,} repeated IDs in {name} ({snapshot.source}), "
                "numbered #2, #3, ..."
            )
    if diff.field_counts:
        lines += ["", "fields changed (records)"]
        name_width = max(len(name) for name in diff.field_counts)
        for name, count in diff.field_counts.most_common(10):
            lines.append(f"  {name:<{name_width}}  {count:>{width},}")
    schema = diff.schema
    if schema["added"] or schema["removed"] or schema["retyped"]:
        lines += ["", "schema"]
        lines += [f"  + {path}" for path in schema["added"]]
        lines += [f"  - {path}" for path in schema["removed"]]
        lines += [
            f"  ~ {path}  {'|'.join(types['before'])} -> {'|'.join(types['after'])}"
            for path, types in schema["retyped"].items()
        ]
    if diff.order_changed:
        lines += ["", "row order of the common records changed"]
    if examples > 0 and diff.has_changes:
        lines += ["", "examples"]
        for item in diff.changed[:examples]:
            lines += _change_lines(diff, item, "changed")
        for item in diff.rekeyed[:examples]:
            lines += _change_lines(diff, item, "key edited")
        lines += _entry_lines(diff.removed, "removed", examples)
        lines += _entry_lines(diff.added, "added", examples)
    if not diff.has_changes:
        lines += ["", "no differences"]
    return "\n".join(lines) + "\n"


def render_json(diff: DatasetDiff) -> str:
    return json.dumps(diff.to_dict(), ensure_ascii=False, indent=2) + "\n"
