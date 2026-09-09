"""Bounded real-text Python service -> Go -> Java annotation benchmark.

No downloads or corpus extraction. Existing outputs are never overwritten.
The report contains aggregate counters/hashes, never source text or record IDs.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import platform
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import tracemalloc
import unicodedata
from contextlib import ExitStack, closing
from itertools import groupby
from pathlib import Path
from unittest.mock import patch

import benchmark_annotations as source_helpers

import corpusledger
from corpusledger.annotation_client import AnnotationClient
from corpusledger.annotation_store import AnnotationEvent

ROOT = Path(__file__).resolve().parents[1]


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def checksum(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def update_document_inventory(inventory, ordinal, document_digests):
    # Frame each ordered event explicitly, including both selected and sibling
    # documents. No process endpoint, execution duration or revision enters it.
    inventory.update(canonical({"ordinal": ordinal, "documents": list(document_digests)}) + b"\n")


def require(condition, code):
    if not condition:
        raise AssertionError(code)  # Fixed codes, never raw records, feature values or identifiers.


def white_space(character):
    # Independent UCD category oracle: all separator categories plus the six
    # Unicode White_Space control characters. Python str.isspace also includes
    # four C0 information separators, which this protocol does not split on.
    return unicodedata.category(character) in {"Zs", "Zl", "Zp"} or character in "\t\n\v\f\r\x85"


def verify_oracle():
    separators = [point for point in range(sys.maxunicode + 1) if white_space(chr(point))]
    require(len(separators) == 25, "unsupported_unicode_white_space_inventory")
    require(all(not white_space(char) for char in "\x1c\x1d\x1e\x1f\u200b\ufeff"), "extra_separator_in_oracle")
    require(token_spans("A😀 e\u0301\r\n終\u00a0Z") == [(0, 2), (3, 5), (7, 8), (9, 10)], "unicode_oracle_regression")
    return {
        "unicode_database_version": unicodedata.unidata_version,
        "white_space_codepoints": len(separators),
        "white_space_inventory_sha256": checksum(separators),
        "algorithm": "UCD Zs/Zl/Zp categories plus HT/LF/VT/FF/CR/NEL; group maximal nonseparator codepoints",
        "reference": "https://www.unicode.org/Public/15.0.0/ucd/PropList.txt",
        "synthetic_boundary_self_check": True,
    }


def token_spans(text):
    spans = []
    for separator, positions in groupby(enumerate(text), key=lambda item: white_space(item[1])):
        block = list(positions)
        if not separator:
            spans.append((block[0][0], block[-1][0] + 1))
    return spans


def field(kind, target=None):
    return {"kind": kind, "required": True, "nullable": False, "target_type": target}


def schemas():
    return (
        {"name": "token", "fields": {"text": field("string"), "position": field("integer")}},
        {"name": "token_group", "fields": {"members": field("references", "token"), "count": field("integer")}},
    )


def document(identifier, text, types=(), annotations=()):
    return {
        "format": "corpusledger.annotations.v1",
        "id": identifier,
        "text": text,
        "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "offset_unit": "unicode_codepoint",
        "types": sorted(types, key=lambda item: item["name"]),
        "annotations": sorted(annotations, key=lambda item: (item["start"], item["end"], item["id"])),
    }


def expected_documents(text, sibling_text):
    spans = token_spans(text)
    token_type, group_type = schemas()
    tokens = [
        {
            "id": f"demo.go.token.{position}",
            "type": "token",
            "start": start,
            "end": end,
            "features": {"text": text[start:end], "position": position},
        }
        for position, (start, end) in enumerate(spans)
    ]
    group = {
        "id": "demo.java.group.0",
        "type": "token_group",
        "start": spans[0][0] if spans else 0,
        "end": spans[-1][1] if spans else 0,
        "features": {"members": [item["id"] for item in tokens], "count": len(tokens)},
    }
    return (
        document("selected", text),
        document("untouched", sibling_text),
        document("selected", text, (token_type,), tokens),
        document("selected", text, (token_type, group_type), (*tokens, group)),
        len(tokens),
    )


def event(position, selected, sibling):
    return {
        "format": "corpusledger.annotation-event.v1",
        "id": f"real.event.{position:03d}",
        "documents": [selected, sibling],
        "metadata": {"adjacent_archive_records": True, "semantic_relationship_asserted": False},
    }


def revision_digest(raw_event, revision, parent, provenance):
    return checksum(
        {
            "format": "corpusledger.annotation-store.v1",
            "event_id": raw_event["id"],
            "metadata": raw_event["metadata"],
            "documents": [{"id": item["id"], "digest": checksum(item)} for item in raw_event["documents"]],
            "revision": revision,
            "parent_digest": parent,
            "provenance": provenance,
        }
    )


def worker_steps(go, java):
    token_type, group_type = schemas()
    result = []
    for position, (worker, name, inputs, outputs) in enumerate(
        ((go, "demo.go.tokens", [], [token_type]), (java, "demo.java.group", [token_type], [group_type]))
    ):
        actual = worker.description.to_dict()
        expected = {
            "format": "corpusledger.processor.v1",
            "name": name,
            "version": "1",
            "config_sha256": actual["config_sha256"],
            "requires": inputs,
            "produces": outputs,
        }
        require(canonical(actual) == canonical(expected), "worker_contract_differs_from_oracle")
        result.append(
            {
                "step_id": f"s{position:03d}",
                "document_id": "selected",
                "pipeline_id": "demo.chain",
                "pipeline_version": "1",
                "worker": {
                    "processor": expected,
                    "endpoint_sha256": hashlib.sha256(worker.address.geturl().encode("ascii")).hexdigest(),
                },
            }
        )
    return result


def check_result(client, source_event, expected, operation_id, steps, initial_digest):
    initial, sibling, tokens, grouped, count = expected
    final = client.get(source_event["id"])
    original = client.get(source_event["id"], revision=1)
    state = client.status(operation_id).to_dict()
    final_event = event(int(source_event["id"].rsplit(".", 1)[1]), grouped, sibling)
    require(canonical(original.event.to_dict()) == canonical(source_event), "historical_source_changed")
    require(original.digest == initial_digest and original.revision == 1, "source_revision_changed")
    require(canonical(final.event.to_dict()) == canonical(final_event), "token_span_reference_text_sibling_mismatch")
    require(final.revision == 2 and final.parent_digest == initial_digest, "final_revision_ancestry_mismatch")
    request = {
        "event_id": source_event["id"],
        "expected_revision": 1,
        "expected_digest": initial_digest,
        "steps": steps,
    }
    require(canonical(state["request"]) == canonical(request), "selective_full_pipeline_binding_mismatch")
    require(state["request_digest"] == checksum(request), "request_checksum_mismatch")
    require(
        state["status"] == "committed" and canonical(state["attempts"]) == b"[1,1]", "unexpected_operation_execution"
    )
    require(state["reservation"] is None and state["error"] is None, "operation_not_fully_committed")
    expected_digests = ((checksum(initial), checksum(tokens)), (checksum(tokens), checksum(grouped)))
    require(len(state["completed"]) == 2, "unexpected_completed_step_count")
    for index, saved in enumerate(state["completed"]):
        require(set(saved) == {"step_id", "attempt", "input_digest", "output_digest", "duration_ms"}, "step_fields")
        require(
            saved["step_id"] == f"s{index:03d}" and type(saved["attempt"]) is int and saved["attempt"] == 1,
            "step_order",
        )
        require((saved["input_digest"], saved["output_digest"]) == expected_digests[index], "step_document_digest")
        require(type(saved["duration_ms"]) is int and 0 <= saved["duration_ms"] < 2**63, "step_duration_type")
    provenance = {
        "annotation_execution": {
            "operation_id": operation_id,
            "request_digest": checksum(request),
            "request": request,
            "completed": state["completed"],
        }
    }
    require(canonical(final.to_dict()["provenance"]) == canonical(provenance), "provenance_binding_mismatch")
    require(final.digest == revision_digest(final_event, 2, initial_digest, provenance), "final_revision_checksum")
    require(
        canonical(state["result"]) == canonical({"revision": 2, "digest": final.digest}),
        "operation_result_not_atomic_revision",
    )
    history = client.history(source_event["id"])
    require([item.revision for item in history] == [1, 2], "unexpected_extra_event_revision")
    require([item.digest for item in history] == [initial_digest, final.digest], "revision_history_checksum")
    journal = client.operation_history(operation_id)
    require([item.version for item in journal] == list(range(1, 7)), "operation_history_versions")
    require(
        [item.status for item in journal] == ["ready", "reserved", "ready", "reserved", "ready", "committed"],
        "operation_history_states",
    )
    require(canonical(journal[-1].to_dict()) == canonical(state), "operation_history_head_mismatch")
    observed_document_digests = tuple(checksum(raw) for raw in final.event.to_dict()["documents"])
    return state, count, final.digest, observed_document_digests


def _helpers():
    path = str(ROOT / "interop")
    sys.path.insert(0, path)
    try:
        return importlib.import_module("verify_execution"), importlib.import_module("verify_workers")
    finally:
        sys.path.remove(path)


def _snapshot(paths):
    return {label: source_helpers.digest(path) for label, path in sorted(paths.items())}


def _tools(command):
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    return (result.stdout + result.stderr).strip()


def run(archive: Path, build_dir: Path, *, events: int = 50):
    if type(events) is not int or not 1 <= events <= 200:
        raise ValueError("events must be an integer in [1, 200]")
    harness, worker_harness = _helpers()
    manifest = build_dir / "build.json"
    artifacts = json.loads(manifest.read_text(encoding="utf-8"))
    java, go_tool = shutil.which("java"), shutil.which("go")
    if java is None or go_tool is None:
        raise ValueError("already installed Go and JDK are required")
    paths = {
        "benchmark": Path(__file__),
        "source_helper": Path(source_helpers.__file__),
        "service_harness": Path(harness.__file__),
        "worker_harness": Path(worker_harness.__file__),
        "build_manifest": manifest,
        "python_executable": Path(sys.executable),
        "java_executable": Path(java),
        "go_executable": Path(go_tool),
    }
    for name in ("go_worker", "java_worker", "jackson_core"):
        paths[name] = Path(artifacts[name])
        require(
            source_helpers.digest(paths[name]) == artifacts[name + "_sha256"], "artifact_manifest_checksum_mismatch"
        )
    for path in sorted((ROOT / "interop").rglob("*")):
        if path.is_file() and path.suffix in {".go", ".java", ".mod", ".json", ".py"}:
            paths["repository/" + path.relative_to(ROOT).as_posix()] = path
    before = _snapshot(paths)
    runtime = source_helpers.runtime_sources()
    oracle = verify_oracle()
    stages = {}
    started = time.perf_counter()
    records, source = source_helpers.real_texts(archive, events * 2)
    stages["source_hashing_and_loading_seconds"] = time.perf_counter() - started
    tool_versions = {
        "java": _tools([java, "-version"]),
        "installed_go": _tools([go_tool, "version"]),
        "go_worker_compiler": _tools([go_tool, "version", "-m", str(paths["go_worker"])]).splitlines()[0].split()[-1],
    }
    commands = [
        [str(paths["go_worker"])],
        [
            java,
            "--add-modules",
            "jdk.httpserver",
            "-cp",
            os.pathsep.join((str(paths["java_worker"]), str(paths["jackson_core"]))),
            "ReferenceWorker",
        ],
    ]
    physical_expected = {}
    snapshots = []
    token_count = 0
    result_inventory = hashlib.sha256()
    expected_document_inventory = hashlib.sha256()
    observed_document_inventory = hashlib.sha256()
    with tempfile.TemporaryDirectory(prefix="corpusledger-real-service-") as directory, ExitStack() as children:
        database = Path(directory) / "events.sqlite"
        tracemalloc.start()
        try:
            started = time.perf_counter()
            go, java_worker = (children.enter_context(worker_harness._worker_context(command)) for command in commands)
            credentials = {
                harness.GO_TOKEN_ENV: go.token,
                harness.JAVA_TOKEN_ENV: java_worker.token,
                harness.SERVICE_TOKEN_ENV: secrets.token_urlsafe(32),
            }
            config = {
                "go": {"endpoint": go.address.geturl(), "description": go.description.to_dict()},
                "java": {"endpoint": java_worker.address.geturl(), "description": java_worker.description.to_dict()},
            }
            steps = worker_steps(go, java_worker)
            service = harness.ServiceProcess(database, config, credentials)
            children.callback(service.close)
            stages["worker_and_service_startup_seconds"] = time.perf_counter() - started
            with patch.dict(os.environ, {harness.SERVICE_TOKEN_ENV: credentials[harness.SERVICE_TOKEN_ENV]}):
                client = AnnotationClient(service.endpoint, harness.SERVICE_TOKEN_ENV)
                started = time.perf_counter()
                for position in range(events):
                    expected = expected_documents(records[position * 2][1], records[position * 2 + 1][1])
                    source_event = event(position, expected[0], expected[1])
                    initial = client.create(AnnotationEvent.from_dict(source_event))
                    initial_digest = revision_digest(source_event, 1, None, {})
                    require(initial.digest == initial_digest, "created_revision_checksum")
                    operation_id = f"real.operation.{position:03d}"
                    begun = client.begin(
                        operation_id,
                        source_event["id"],
                        {"selected": "demo.chain"},
                        expected_revision=1,
                        expected_digest=initial_digest,
                    )
                    require(begun.status == "ready" and begun.completed_steps == 0, "begin_executed_steps")
                    client.resume(operation_id, {"selected": "demo.chain"})
                    state, count, final_digest, observed_documents = check_result(
                        client, source_event, expected, operation_id, steps, initial_digest
                    )
                    token_count += count
                    result_inventory.update(bytes.fromhex(final_digest))
                    update_document_inventory(
                        expected_document_inventory, position, (checksum(expected[3]), checksum(expected[1]))
                    )
                    update_document_inventory(observed_document_inventory, position, observed_documents)
                    snapshots.append((source_event, expected, operation_id, initial_digest, canonical(state)))
                    for raw_document in expected[:4]:
                        physical_expected[checksum(raw_document)] = canonical(raw_document)
                stages["create_execute_and_independent_oracles_seconds"] = time.perf_counter() - started
                service.close()
                started = time.perf_counter()
                reopened = harness.ServiceProcess(database, config, credentials)
                children.callback(reopened.close)
                client = AnnotationClient(reopened.endpoint, harness.SERVICE_TOKEN_ENV)
                go.close()
                java_worker.close()
                for source_event, expected, operation_id, initial_digest, prior in snapshots:
                    actual, _, _, observed_documents = check_result(
                        client, source_event, expected, operation_id, steps, initial_digest
                    )
                    require(
                        observed_documents == (checksum(expected[3]), checksum(expected[1])),
                        "reopened_final_document_inventory_mismatch",
                    )
                    require(canonical(actual) == prior, "reopened_operation_changed")
                    repeat = client.begin(
                        operation_id,
                        source_event["id"],
                        {"selected": "demo.chain"},
                        expected_revision=1,
                        expected_digest=initial_digest,
                    )
                    require(canonical(repeat.to_dict()) == prior, "idempotent_begin_changed_operation")
                    require(
                        canonical(client.resume(operation_id, {"selected": "demo.chain"}).to_dict()) == prior,
                        "idempotent_resume_changed_operation",
                    )
                    require(len(client.history(source_event["id"])) == 2, "idempotence_created_revision")
                require(len(client.list(limit=events)) == events, "event_inventory_mismatch")
                require(len(client.operations(limit=events)) == events, "operation_inventory_mismatch")
                reopened.close()
                stages["service_restart_requery_and_idempotence_seconds"] = time.perf_counter() - started
            started = time.perf_counter()
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as sql:
                physical = {
                    digest: body.encode("utf-8") for digest, body in sql.execute("SELECT digest,body FROM documents")
                }
                require(physical == physical_expected, "physical_documents_or_orphans_mismatch")
                require(
                    sql.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == events * 2, "physical_revision_count"
                )
                require(
                    sql.execute("SELECT COUNT(*) FROM annotation_operations").fetchone()[0] == events,
                    "physical_operation_count",
                )
                require(
                    sql.execute("SELECT COUNT(*) FROM annotation_operation_events").fetchone()[0] == events * 6,
                    "physical_operation_event_count",
                )
                require(sql.execute("PRAGMA integrity_check").fetchone() == ("ok",), "sqlite_integrity_failed")
                secret_bytes = tuple(value.encode() for value in credentials.values())
                for (body,) in sql.execute("SELECT body FROM annotation_operation_events"):
                    require(not any(secret in body.encode() for secret in secret_bytes), "journal_contains_credential")
            database_bytes = database.stat().st_size
            stages["physical_sqlite_verification_seconds"] = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    require(
        expected_document_inventory.digest() == observed_document_inventory.digest(),
        "final_document_inventory_mismatch",
    )
    require(
        _snapshot(paths) == before
        and source_helpers.runtime_sources() == runtime
        and source_helpers.digest(archive) == source_helpers.SOURCE_SHA256,
        "sources_changed_during_run",
    )
    return {
        "format": "corpusledger.annotation-service-benchmark.v1",
        "kind": "independent-cross-language-service-engineering-checks-on-external-real-text",
        "passed": True,
        "source": source,
        "events": events,
        "source_records": events * 2,
        "documents_per_event": 2,
        "selected_documents": events,
        "untouched_siblings": events,
        "source_codepoints": sum(len(text) for _, text in records),
        "tokens_checked": token_count,
        "groups_checked": events,
        "committed_operations": events,
        "event_revisions": events * 2,
        "operation_transitions": events * 6,
        "physical_documents": len(physical_expected),
        "service_processes": 2,
        "worker_processes": 2,
        "reopened_events_checked": events,
        "idempotent_begin_calls": events,
        "idempotent_resume_calls": events,
        "workers_stopped_before_idempotence": True,
        "oracle": oracle,
        "checks": {
            name: True
            for name in (
                "token_boundaries_and_codepoint_spans",
                "exact_token_text_and_position",
                "typed_group_references",
                "exact_source_and_sibling_preservation",
                "source_and_full_pipeline_hash_bindings",
                "independent_revision_and_provenance_hashes",
                "final_document_inventory_matches",
                "operation_result_matches_committed_revision",
                "service_reopen_and_all_event_requeries",
                "idempotence_without_extra_revisions_or_attempts",
                "physical_document_inventory_without_orphans",
                "sqlite_integrity",
                "no_credentials_in_journal",
                "all_pinned_sources_unchanged_before_after",
            )
        },
        "stages": stages,
        "peak_client_python_bytes": peak,
        "sqlite_database_bytes_after_close": database_bytes,
        "revision_inventory_sha256": result_inventory.hexdigest(),
        "expected_final_document_inventory_sha256": expected_document_inventory.hexdigest(),
        "observed_final_document_inventory_sha256": observed_document_inventory.hexdigest(),
        "pipeline_binding_sha256": checksum(steps),
        "worker_config_sha256": [step["worker"]["processor"]["config_sha256"] for step in steps],
        "python": sys.version,
        "platform": platform.platform(),
        "sqlite_version": sqlite3.sqlite_version,
        "corpusledger_version": corpusledger.__version__,
        "tool_versions": tool_versions,
        "runtime_sources": runtime,
        "artifact_and_helper_sha256": before,
        "temporary_database_removed": True,
        "timing_scope": (
            "single sequential cold-start trial; stages include HTTP, validation and independent oracles, "
            "not isolated throughput"
        ),
        "memory_scope": (
            "tracemalloc in Python client only from process startup through SQLite checks; "
            "excludes preloaded corpus, all child processes, native allocations and RSS"
        ),
        "source_relationship": (
            "adjacent archive records paired for selective-processing tests; no semantic relationship asserted"
        ),
        "claim": (
            "Real-text transport/persistence correctness against deterministic whitespace rules, "
            "not gold NLP accuracy, production scaling or whole-platform parity."
        ),
    }


def output_target(path):
    if os.path.lexists(path):
        raise ValueError("output must be new; existing files and aliases are never overwritten")
    target = path.parent.resolve() / path.name
    if not target.parent.is_dir():
        raise ValueError("output parent must already be a directory")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    published = False
    try:
        output = output_target(args.output)
        payload = run(args.archive.resolve(), args.build_dir.resolve(), events=args.events)
        rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
        # Publishing a fully written same-filesystem temporary file with link()
        # atomically refuses a destination created by a concurrent writer.
        with tempfile.TemporaryDirectory(prefix=".annotation-service-report-", dir=output.parent) as directory:
            temporary = Path(directory) / "report.json"
            temporary.write_text(rendered, encoding="utf-8", newline="\n")
            os.link(temporary, output)
            published = True
        print(json.dumps(payload, sort_keys=True, allow_nan=False))
    except Exception:
        # argparse/tracebacks must not accidentally redistribute fixture text.
        message = (
            "annotation_service_benchmark_report_published_but_delivery_failed"
            if published
            else "annotation_service_benchmark_failed_no_report_published"
        )
        print(message, file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
