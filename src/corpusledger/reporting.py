"""JSON and Markdown rendering for diff results."""

from __future__ import annotations

import json
from collections.abc import Callable

from .diff import CorpusDiff


def render_json(diff: CorpusDiff) -> str:
    """Render pretty deterministic JSON."""
    return json.dumps(diff.to_dict(), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _markdown_code(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("`", "&#96;")
        .replace("\r\n", "<br>")
        .replace("\r", "<br>")
        .replace("\n", "<br>")
    )


def _items(values: list[str]) -> str:
    return "\n".join(f"- `{_markdown_code(value)}`" for value in values) if values else "_None_"


def render_markdown(diff: CorpusDiff) -> str:
    """Render a compact audit report without exposing record values."""
    drift = diff.schema
    changed_fields = sorted(drift.get("changed_fields", {}))
    lines = [
        "# CorpusLedger diff",
        "",
        f"**Changes:** {'yes' if diff.has_changes else 'no'}  ",
        f"**Order changed:** {'yes' if diff.order_changed else 'no'}  ",
        f"**Order-only change:** {'yes' if diff.order_only else 'no'}",
        "",
        "## Added records",
        "",
        _items(list(diff.added_records)),
        "",
        "## Removed records",
        "",
        _items(list(diff.removed_records)),
        "",
        "## Changed records",
        "",
        _items(list(diff.changed_records)),
        "",
        "## Schema drift",
        "",
        f"- Added fields: {', '.join(drift.get('added_fields', [])) or 'none'}",
        f"- Removed fields: {', '.join(drift.get('removed_fields', [])) or 'none'}",
        f"- Changed summaries: {', '.join(changed_fields) or 'none'}",
        "",
        "## New privacy-risk hints",
        "",
    ]
    if diff.privacy_findings_added:
        lines.extend(
            f"- `{_markdown_code(str(item['record_id']))}` "
            f"`{_markdown_code(str(item['path']))}`: {_markdown_code(str(item['kind']))}"
            for item in diff.privacy_findings_added
        )
    else:
        lines.append("_None_")
    return "\n".join(lines) + "\n"


def render(diff: CorpusDiff, format_name: str) -> str:
    """Render using ``json`` or ``markdown``."""
    renderers: dict[str, Callable[[CorpusDiff], str]] = {
        "json": render_json,
        "markdown": render_markdown,
        "md": render_markdown,
    }
    try:
        return renderers[format_name](diff)
    except KeyError as exc:
        raise ValueError(f"unsupported report format: {format_name}") from exc
