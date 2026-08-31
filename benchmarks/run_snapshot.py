"""Reproducible end-to-end JSONL snapshot benchmark."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from generate_jsonl import generate_jsonl

from corpusledger import __version__, build_manifest


def _run_once(source: Path, output: Path, *, trace_allocations: bool) -> tuple[float, int | None, int, str]:
    gc.collect()
    if trace_allocations:
        tracemalloc.start()
    started = time.perf_counter()
    manifest = build_manifest(source)
    manifest.save(output)
    elapsed = time.perf_counter() - started
    peak: int | None = None
    if trace_allocations:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    return elapsed, peak, output.stat().st_size, manifest.corpus_hash


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--memory-records",
        type=int,
        default=10_000,
        help="separate smaller allocation profile; tracemalloc strongly perturbs throughput",
    )
    parser.add_argument("--result", type=Path)
    args = parser.parse_args()
    if args.records < 1 or args.payload_bytes < 0 or args.repeats < 1 or args.memory_records < 1:
        parser.error("records, repeats, and memory-records must be positive; payload-bytes must be non-negative")

    with tempfile.TemporaryDirectory(prefix="corpusledger-benchmark-") as directory:
        root = Path(directory)
        source = root / "corpus.jsonl"
        output = root / "manifest.json"
        input_bytes = generate_jsonl(source, args.records, args.payload_bytes)
        runs = [_run_once(source, output, trace_allocations=False) for _ in range(args.repeats)]
        memory_source = root / "memory-corpus.jsonl"
        memory_output = root / "memory-manifest.json"
        memory_input_bytes = generate_jsonl(memory_source, args.memory_records, args.payload_bytes)
        memory_run = _run_once(memory_source, memory_output, trace_allocations=True)

    elapsed = [run[0] for run in runs]
    hashes = {run[3] for run in runs}
    if len(hashes) != 1:
        raise RuntimeError("benchmark snapshots were not deterministic")
    median_seconds = statistics.median(elapsed)
    result = {
        "benchmark": "jsonl-snapshot-v1",
        "corpusledger_version": __version__,
        "input": {
            "bytes": input_bytes,
            "payload_bytes_per_record": args.payload_bytes,
            "records": args.records,
        },
        "manifest_bytes": runs[-1][2],
        "allocation_profile": {
            "input_bytes": memory_input_bytes,
            "manifest_bytes": memory_run[2],
            "peak_tracemalloc_mib": round((memory_run[1] or 0) / 1024 / 1024, 2),
            "records": args.memory_records,
            "seconds_with_tracing": round(memory_run[0], 4),
        },
        "platform": {
            "machine": platform.machine(),
            "operating_system": platform.platform(),
            "python": sys.version.split()[0],
        },
        "results": {
            "median_records_per_second": round(args.records / median_seconds, 2),
            "median_seconds": round(median_seconds, 4),
            "repeat_seconds": [round(value, 4) for value in elapsed],
            "repeats": args.repeats,
        },
        "scope": (
            "Throughput is end-to-end strict JSONL parse, canonicalization, manifest construction, "
            "privacy/schema aggregation, and manifest write without tracing. The separate allocation "
            "profile uses a smaller corpus because tracemalloc strongly perturbs runtime; its peak is "
            "Python allocations, not process RSS."
        ),
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.result is not None:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
