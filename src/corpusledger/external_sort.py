"""Bounded-memory external sorting for canonical JSONL corpora."""

from __future__ import annotations

import hashlib
import heapq
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import CanonicalPolicy, canonical_json
from .readers import iter_corpus


@dataclass(frozen=True, slots=True)
class ExternalSortReport:
    """Summary of an atomically materialized external sort."""

    records: int
    chunks: int
    output_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "algorithm": "external-jsonl-merge-v1",
            "records": self.records,
            "chunks": self.chunks,
            "output_digest": self.output_digest,
        }


def external_sort_jsonl(
    source: str | Path,
    destination: str | Path,
    *,
    chunk_size: int = 10_000,
    id_field: str = "id",
    policy: CanonicalPolicy | None = None,
) -> ExternalSortReport:
    """Sort JSONL records by canonical content with bounded in-memory chunks."""

    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")
    source_path = Path(source).resolve()
    destination_path = Path(destination).resolve()
    if source_path == destination_path:
        raise ValueError("sort destination must differ from source")
    if source_path.suffix.lower() != ".jsonl":
        raise ValueError("external sorting requires a JSONL source")
    active = policy or CanonicalPolicy()
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_chunks: list[Path] = []
    try:
        chunk: list[str] = []
        for record in iter_corpus(source_path, id_field=id_field, policy=active):
            chunk.append(canonical_json(record.data, active))
            if len(chunk) >= chunk_size:
                temporary_chunks.append(_write_chunk(chunk, destination_path.parent))
                chunk.clear()
        if chunk:
            temporary_chunks.append(_write_chunk(chunk, destination_path.parent))
        return _merge_chunks(temporary_chunks, destination_path)
    finally:
        for path in temporary_chunks:
            with suppress(OSError):
                path.unlink()


def _write_chunk(rows: list[str], directory: Path) -> Path:
    rows.sort()
    descriptor, name = tempfile.mkstemp(prefix=".corpusledger-sort-", suffix=".chunk", dir=directory)
    path = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            for rendered in rows:
                stream.write(rendered + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        with suppress(OSError):
            path.unlink()
        raise
    return path


def _merge_chunks(chunks: list[Path], destination: Path) -> ExternalSortReport:
    streams = [path.open("r", encoding="utf-8", newline="\n") for path in chunks]
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
        os.close(descriptor)
        temporary = Path(name)
        digest = hashlib.sha256()
        records = 0
        with temporary.open("w", encoding="utf-8", newline="\n") as output:
            heap: list[tuple[str, int]] = []
            for index, stream in enumerate(streams):
                line = stream.readline()
                if line:
                    heapq.heappush(heap, (line.rstrip("\n"), index))
            while heap:
                rendered, index = heapq.heappop(heap)
                line = rendered + "\n"
                output.write(line)
                digest.update(line.encode("utf-8"))
                records += 1
                next_line = streams[index].readline()
                if next_line:
                    heapq.heappush(heap, (next_line.rstrip("\n"), index))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        temporary = None
        return ExternalSortReport(records, len(chunks), digest.hexdigest())
    finally:
        for stream in streams:
            stream.close()
        if temporary is not None:
            with suppress(OSError):
                temporary.unlink()


__all__ = ["ExternalSortReport", "external_sort_jsonl"]
