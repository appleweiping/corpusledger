"""Verify persistent annotation event histories on locally supplied real text.

The related casefold view and whole-document spans are derived engineering
fixtures, not translations or gold annotations. No source text or IDs are emitted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Mapping
from contextlib import closing
from itertools import pairwise
from pathlib import Path
from typing import Any

import benchmark_annotations as source_helpers

import corpusledger
from corpusledger import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation
from corpusledger.annotation_pipeline import AnnotationPipeline, AnnotationProcessor
from corpusledger.annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationRevisionInfo,
    AnnotationStore,
)

WHOLE_SCHEMA = AnnotationType("whole_span", {"length": AnnotationField("integer")})


def _whole_span(document: AnnotationDocument) -> tuple[SpanAnnotation, ...]:
    return (SpanAnnotation("whole", "whole_span", 0, len(document.text), {"length": len(document.text)}),)


def _event(position: int, text: str) -> AnnotationEvent:
    return AnnotationEvent(
        f"event:{position:06d}",
        (AnnotationDocument("original", text), AnnotationDocument("casefold", text.casefold())),
        {"related_view": "Python str.casefold, derived from original; not a translation"},
    )


def _failure_oracles(store: AnnotationStore, event: AnnotationEvent) -> dict[str, Any]:
    before = store.get(event.event_id)
    history_before = tuple(row.to_dict() for row in store.history(event.event_id))
    try:
        store.put(before.event, expected_revision=before.revision - 1)
    except AnnotationConflictError as error:
        if error.actual_revision != before.revision:
            raise AssertionError("stale CAS did not report the current revision") from error
    else:
        raise AssertionError("stale CAS unexpectedly appended a revision")

    calls: list[str] = []
    first_schema, second_schema = AnnotationType("probe_first"), AnnotationType("probe_second")

    def first(document: AnnotationDocument) -> tuple[SpanAnnotation, ...]:
        calls.append("first")
        return (SpanAnnotation("probe", first_schema.name, 0, len(document.text)),)

    def second(document: AnnotationDocument) -> tuple[SpanAnnotation, ...]:
        calls.append("second")
        if document.get("probe").type_name != first_schema.name:
            raise AssertionError("second processor did not receive the first processor output")
        raise RuntimeError("intentional second-processor failure")

    failing = AnnotationPipeline(
        (
            AnnotationProcessor("first", "1", first, produces=(first_schema,)),
            AnnotationProcessor("second", "1", second, requires=(first_schema,), produces=(second_schema,)),
        )
    )
    try:
        store.process(event.event_id, {"original": failing}, expected_revision=before.revision)
    except RuntimeError as error:
        if str(error) != "intentional second-processor failure":
            raise
    else:
        raise AssertionError("failing second processor unexpectedly published a revision")
    if calls != ["first", "second"]:
        raise AssertionError("failure oracle did not exercise both callbacks")
    after = store.get(event.event_id)
    if (
        after.to_dict() != before.to_dict()
        or tuple(row.to_dict() for row in store.history(event.event_id)) != history_before
    ):
        raise AssertionError("a failed operation changed the event or its history")
    if after.event.get_document("casefold") != before.event.get_document("casefold"):
        raise AssertionError("a failed operation changed the related sibling document")
    return {
        "stale_revision_rejected": True,
        "second_processor_failed_after_first_output": True,
        "callbacks_executed": len(calls),
        "event_and_history_unchanged": True,
        "sibling_document_unchanged": True,
        "callback_effects_rolled_back": False,
    }


def _verify_reopened(
    database: Path,
    records: list[tuple[str, str]],
    revisions: Mapping[str, tuple[str, ...]],
    updated: int,
) -> dict[str, Any]:
    expected_documents: set[str] = set()
    expected_ids = [f"event:{position:06d}" for position in range(len(records))]
    history_records = history_pages = 0
    revision_digest = hashlib.sha256()
    with AnnotationStore(database, create=False) as store:
        seen_ids: list[str] = []
        cursor = None
        list_pages = 0
        while True:
            page = store.list(after_event_id=cursor, limit=37)
            if not page:
                break
            list_pages += 1
            seen_ids.extend(row.event_id for row in page)
            cursor = page[-1].event_id
        if seen_ids != expected_ids:
            raise AssertionError("event pagination lost, reordered, or repeated events")
        for position, (_, text) in enumerate(records):
            expected = _event(position, text)
            initial = store.get(expected.event_id, revision=1)
            latest = store.get(expected.event_id)
            if initial.event != expected:
                raise AssertionError("historical source documents or metadata changed")
            for document in expected.documents:
                expected_documents.add(document.digest)
            original = latest.event.get_document("original")
            sibling = latest.event.get_document("casefold")
            if original.text != text or original.text_sha256 != hashlib.sha256(text.encode("utf-8")).hexdigest():
                raise AssertionError("persistent original text no longer matches its source hash")
            if sibling != expected.get_document("casefold"):
                raise AssertionError("an original-document update changed the casefold sibling")
            if position < updated:
                wanted = AnnotationDocument(
                    "original",
                    text,
                    (WHOLE_SCHEMA,),
                    (SpanAnnotation("whole", "whole_span", 0, len(text), {"length": len(text)}),),
                )
                if original != wanted or original.span_text("whole") != text:
                    raise AssertionError("persistent processor output differs from independent full-span arithmetic")
                expected_documents.add(wanted.digest)
                provenance = latest.provenance["annotation_pipelines"]["original"]
                if (
                    provenance["input_digest"] != initial.event.get_document("original").digest
                    or provenance["output_digest"] != wanted.digest
                    or len(provenance["steps"]) != 1
                    or provenance["steps"][0]["name"] != "whole-span"
                    or provenance["steps"][0]["version"] != "1"
                ):
                    raise AssertionError("persisted pipeline provenance differs from the committed transformation")
            elif latest.to_dict() != initial.to_dict():
                raise AssertionError("an unselected event acquired an unexpected revision")
            history: list[AnnotationRevisionInfo] = []
            after_revision = 0
            while True:
                page = store.history(expected.event_id, after_revision=after_revision, limit=1)
                if not page:
                    break
                history_pages += 1
                history.extend(page)
                after_revision = page[-1].revision
            history_records += len(history)
            if tuple(row.digest for row in history) != revisions[expected.event_id]:
                raise AssertionError("reopened history differs from the originally committed revision digests")
            if tuple(row.revision for row in history) != tuple(range(1, len(history) + 1)):
                raise AssertionError("revision pagination has a gap")
            for previous, current in pairwise(history):
                if current.parent_digest != previous.digest:
                    raise AssertionError("revision parent digest does not match its predecessor")
            for row in history:
                revision_digest.update(bytes.fromhex(row.digest))
        verification = store.verify()
        if (verification.events, verification.revisions, verification.documents) != (
            len(records),
            len(records) + updated,
            len(expected_documents),
        ):
            raise AssertionError("store verification counts differ from the independent expected inventory")
    # Check the physical content-addressed table, not only the public verifier's
    # referenced-document count, so orphan or duplicate writes cannot hide.
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        stored_documents = [row[0] for row in connection.execute("SELECT digest FROM documents")]
    if len(stored_documents) != len(expected_documents) or set(stored_documents) != expected_documents:
        raise AssertionError("physical document storage differs from exact content-digest deduplication")
    return {
        "verification": verification.to_dict(),
        "event_list_pages": list_pages,
        "history_pages": history_pages,
        "history_records": history_records,
        "source_text_hashes_checked": len(records),
        "sibling_documents_checked": len(records),
        "processor_provenance_records_checked": updated,
        "physical_document_rows": len(stored_documents),
        "expected_unique_document_digests": len(expected_documents),
        "document_references_across_history": 2 * (len(records) + updated),
        "deduplicated_document_references": 2 * (len(records) + updated) - len(expected_documents),
        "revision_inventory_sha256": revision_digest.hexdigest(),
        "physical_deduplication_verified": True,
        "no_orphan_document_rows": True,
    }


def run(archive: Path, *, limit: int = 200) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("record limit must be an integer in [1, 1000]")
    runtime_before = source_helpers.runtime_sources()
    helper_path = Path(source_helpers.__file__)
    helper_digest = source_helpers.digest(helper_path)
    benchmark_digest = source_helpers.digest(Path(__file__))
    started = time.perf_counter()
    records, source = source_helpers.real_texts(archive, limit)
    source_seconds = time.perf_counter() - started
    pipeline = AnnotationPipeline((AnnotationProcessor("whole-span", "1", _whole_span, produces=(WHOLE_SCHEMA,)),))
    updated = len(records) // 2
    revisions: dict[str, tuple[str, ...]] = {}
    stages: dict[str, float] = {"source_hashing_and_loading_seconds": source_seconds}
    with tempfile.TemporaryDirectory(prefix="corpusledger-annotation-store-") as directory:
        database = Path(directory) / "events.db"
        tracemalloc.start()
        try:
            started = time.perf_counter()
            with AnnotationStore(database) as store:
                for position, (_, text) in enumerate(records):
                    event = _event(position, text)
                    revision = store.put(event)
                    revisions[event.event_id] = (revision.digest,)
                stages["create_events_seconds"] = time.perf_counter() - started
                started = time.perf_counter()
                for position in range(updated):
                    event_id = f"event:{position:06d}"
                    revision = store.process(event_id, {"original": pipeline}, expected_revision=1)
                    revisions[event_id] += (revision.digest,)
                stages["process_and_commit_seconds"] = time.perf_counter() - started
                started = time.perf_counter()
                failures = _failure_oracles(store, _event(0, records[0][1]))
                stages["failure_oracles_seconds"] = time.perf_counter() - started
            started = time.perf_counter()
            verified = _verify_reopened(database, records, revisions, updated)
            stages["reopen_and_verify_seconds"] = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
            database_bytes = database.stat().st_size
        finally:
            tracemalloc.stop()
    if (
        source_helpers.runtime_sources() != runtime_before
        or source_helpers.digest(helper_path) != helper_digest
        or source_helpers.digest(Path(__file__)) != benchmark_digest
        or source_helpers.digest(archive) != source_helpers.SOURCE_SHA256
    ):
        raise RuntimeError("benchmark/runtime source files changed during the run; rerun on a stable tree")
    return {
        "format": "corpusledger.annotation-store-benchmark.v1",
        "kind": "persistent-event-engineering-checks-on-external-real-text",
        "source": source,
        "events_created": len(records),
        "events_updated": updated,
        "documents_per_event": 2,
        "source_text_codepoints": sum(len(text) for _, text in records),
        "related_view": "Python str.casefold-derived text; not translation or gold annotation",
        "annotation_rule": "one [0, len(text)) span with integer code-point length; not a linguistic annotation",
        "checks": verified,
        "failure_oracles": failures,
        "stages": stages,
        "peak_python_bytes": peak,
        "sqlite_database_bytes_after_close": database_bytes,
        "temporary_database_removed": True,
        "python": sys.version,
        "platform": platform.platform(),
        "sqlite_version": sqlite3.sqlite_version,
        "corpusledger_version": corpusledger.__version__,
        "runtime_sources": runtime_before,
        "benchmark_sha256": benchmark_digest,
        "source_helper_sha256": helper_digest,
        "timing_scope": "separate local stages; reopen verification includes independent correctness oracles",
        "memory_scope": (
            "traced Python allocations during creation, processing, failure oracles and reopening/verification; "
            "excludes source records and pipeline/schema objects loaded before tracing; "
            "also excludes SQLite native caches and process RSS"
        ),
        "claim": "Bounded persistence and failure-atomicity checks, not gold NLP accuracy or remote-service parity.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    protected = (args.archive.resolve(), Path(__file__).resolve(), Path(source_helpers.__file__).resolve())
    if any(output == path or (output.exists() and output.samefile(path)) for path in protected):
        parser.error("output must not overwrite or alias the archive or benchmark sources")
    payload = run(args.archive, limit=args.limit)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=output.parent)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
        os.replace(temporary, output)
    finally:
        if temporary is not None and Path(temporary).exists():
            Path(temporary).unlink()
    print(json.dumps(payload, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
