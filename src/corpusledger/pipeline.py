"""Streaming, resumable record transformations with provenance checkpoints.

The pipeline deliberately accepts Python callables rather than importing a
configuration language. Callers should give steps stable names and keep them
pure; the checkpoint records names and source/output digests but cannot prove
the implementation of an arbitrary callable has not changed.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import CanonicalPolicy, canonical_json
from .errors import InputError
from .readers import discover_inputs, iter_corpus

Transform = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class PipelineStep:
    """One named pure transformation over a JSON object."""

    name: str
    transform: Transform

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not self.name.strip()
            or any(character.isspace() for character in self.name)
        ):
            raise ValueError("pipeline step name must be a non-empty token")
        if not callable(self.transform):
            raise TypeError("pipeline transform must be callable")


@dataclass(frozen=True, slots=True)
class PipelineReport:
    """Immutable result and provenance of one pipeline execution."""

    source: str
    output: str
    input_digest: str
    output_digest: str
    records: int
    steps: tuple[str, ...]
    resumed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "source": self.source,
            "output": self.output,
            "input_digest": self.input_digest,
            "output_digest": self.output_digest,
            "records": self.records,
            "steps": list(self.steps),
            "resumed": self.resumed,
        }


def _path_digest(path: Path, policy: CanonicalPolicy, exclude_paths: Iterable[Path] = ()) -> str:
    digest = hashlib.sha256()
    excluded = {item.resolve() for item in exclude_paths}
    for file in discover_inputs(path, exclude_paths=excluded):
        digest.update(file.relative_to(path if path.is_dir() else file.parent).as_posix().encode())
        digest.update(b"\0")
        with file.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    # Include policy because normalization affects IDs and downstream semantics.
    digest.update(
        canonical_json(
            policy.__dict__
            if hasattr(policy, "__dict__")
            else {
                "unicode_form": policy.unicode_form,
                "list_strategy": policy.list_strategy,
            },
            CanonicalPolicy(unicode_form="none"),
        ).encode()
    )
    return digest.hexdigest()


def _atomic_write(path: Path, content: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            for line in content:
                stream.write(line)
        os.replace(temporary, path)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary)
        raise


def run_pipeline(
    source: str | Path,
    output: str | Path,
    steps: Iterable[PipelineStep],
    *,
    id_field: str = "id",
    state: str | Path | None = None,
    resume: bool = False,
    policy: CanonicalPolicy | None = None,
) -> PipelineReport:
    """Transform a corpus to JSONL atomically and write a completion checkpoint.

    The source is read through CorpusLedger's strict readers. Every step receives
    a fresh shallow record mapping and must return a JSON object with the same
    normalized ID. Records are streamed with O(unique IDs + one record) memory;
    the reader's global uniqueness index is retained by design. A failed step
    leaves the prior output and checkpoint untouched. ``resume`` only reuses a
    completed checkpoint when source digest, output digest, ID field and ordered
    step names all match; it does not trust a stale or hand-edited checkpoint.
    """
    source_path, output_path = Path(source).resolve(), Path(output).resolve()
    if source_path == output_path or (
        output_path.exists() and source_path.exists() and output_path.samefile(source_path)
    ):
        raise InputError("pipeline output must not overwrite its input")
    if not isinstance(id_field, str) or not id_field:
        raise InputError("id_field must be a non-empty string")
    active_policy = policy or CanonicalPolicy()
    planned = tuple(steps)
    names = tuple(step.name for step in planned)
    state_path = (
        Path(state).resolve() if state is not None else output_path.with_suffix(output_path.suffix + ".state.json")
    )
    source_digest = _path_digest(source_path, active_policy, (output_path, state_path))
    if resume and output_path.is_file() and state_path.is_file():
        try:
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            if (
                saved.get("schema_version") == 1
                and saved.get("status") == "complete"
                and saved.get("source_digest") == source_digest
                and saved.get("output") == str(output_path)
                and tuple(saved.get("steps", ())) == names
                and saved.get("id_field") == id_field
                and hashlib.sha256(output_path.read_bytes()).hexdigest() == saved.get("output_digest")
            ):
                return PipelineReport(
                    str(source_path),
                    str(output_path),
                    source_digest,
                    saved["output_digest"],
                    int(saved["records"]),
                    names,
                    resumed=True,
                )
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError, KeyError):
            pass

    output_digest = hashlib.sha256()
    records = 0

    def lines() -> Iterable[str]:
        nonlocal records
        for record in iter_corpus(source_path, id_field, policy=active_policy, exclude_paths=(output_path, state_path)):
            data = dict(record.data)
            for step in planned:
                data = step.transform(dict(data))
                if not isinstance(data, dict):
                    raise InputError(f"pipeline step {step.name!r} did not return an object")
            if id_field not in data or str(data[id_field]) != record.record_id:
                raise InputError(f"pipeline must preserve normalized {id_field!r} for {record.record_id!r}")
            rendered = canonical_json(data, active_policy) + "\n"
            encoded = rendered.encode("utf-8")
            output_digest.update(encoded)
            records += 1
            yield rendered

    _atomic_write(output_path, lines())
    digest_text = output_digest.hexdigest()
    report = PipelineReport(str(source_path), str(output_path), source_digest, digest_text, records, names)
    checkpoint = {
        "schema_version": 1,
        "status": "complete",
        "source": str(source_path),
        "source_digest": source_digest,
        "output": str(output_path),
        "output_digest": digest_text,
        "records": records,
        "id_field": id_field,
        "steps": list(names),
    }
    _atomic_write(state_path, [json.dumps(checkpoint, sort_keys=True, separators=(",", ":")) + "\n"])
    return report


def select_fields(fields: Iterable[str]) -> PipelineStep:
    """Keep only named fields; the ID field must be included by the caller."""
    selected = tuple(fields)
    if not selected or any(not isinstance(field, str) or not field for field in selected):
        raise ValueError("fields must be a non-empty iterable of names")
    if len(set(selected)) != len(selected):
        raise ValueError("fields must not contain duplicates")
    return PipelineStep(
        "select:" + ",".join(selected), lambda data: {field: data[field] for field in selected if field in data}
    )


def drop_fields(fields: Iterable[str]) -> PipelineStep:
    """Drop named fields from each object."""
    dropped = frozenset(fields)
    if not dropped or any(not isinstance(field, str) or not field for field in dropped):
        raise ValueError("fields must be a non-empty iterable of names")
    return PipelineStep(
        "drop:" + ",".join(sorted(dropped)),
        lambda data: {key: value for key, value in data.items() if key not in dropped},
    )


def rename_field(old: str, new: str) -> PipelineStep:
    """Rename one field, rejecting collisions instead of overwriting data."""
    if not isinstance(old, str) or not old or not isinstance(new, str) or not new or old == new:
        raise ValueError("old and new fields must be distinct non-empty strings")

    def transform(data: dict[str, Any]) -> dict[str, Any]:
        if old not in data:
            return data
        if new in data:
            raise InputError(f"cannot rename {old!r}: destination {new!r} already exists")
        result = dict(data)
        result[new] = result.pop(old)
        return result

    return PipelineStep(f"rename:{old}->{new}", transform)
