"""Unambiguous JSON Pointer paths shared by manifests, schemas, and findings."""

from __future__ import annotations


def escape_pointer_segment(segment: str) -> str:
    """Escape one RFC 6901 JSON Pointer segment."""
    return segment.replace("~", "~0").replace("/", "~1")


def join_pointer(path: str, segment: str) -> str:
    """Append one object key or array index to a JSON Pointer."""
    return f"{path}/{escape_pointer_segment(segment)}"
