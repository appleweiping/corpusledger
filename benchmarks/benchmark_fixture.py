"""Benchmark manifest, bundle, and verification work on checked-in JSONL."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from corpusledger import build_manifest, bundle_snapshot, verify_bundle


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    source = root / "examples" / "after.jsonl"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=root / "benchmarks/results/fixture.json")
    args = parser.parse_args()
    payload = source.read_bytes()
    tracemalloc.start()
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="corpusledger-fixture-") as directory:
        bundle = Path(directory) / "fixture.zip"
        manifest = build_manifest(source)
        report = bundle_snapshot(manifest, source, bundle)
        verified = verify_bundle(bundle, expected_archive_digest=report.archive_digest)
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    result = {
        "kind": "fixture-real",
        "source": str(source.relative_to(root)).replace("\\", "/"),
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "source_bytes": len(payload),
        "records": len(manifest.records),
        "bundle_bytes": verified.bytes,
        "manifest_digest": verified.manifest_digest,
        "elapsed_seconds": elapsed,
        "peak_python_bytes": peak,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
