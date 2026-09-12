"""Build and verify real local workers, retaining a source-bound CI evidence file."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ATTACHMENT_ORACLE = "examples/annotation_attachment_polyglot.py"
ATTACHMENT_SOURCES = {
    ATTACHMENT_ORACLE,
    "interop/verify_workers.py",
    "interop/go/attachment_example/main.go",
    "interop/go/go.mod",
    "interop/java/AttachmentExample.java",
    "interop/java/dependencies.json",
}
ATTACHMENT_CHECKS = (
    "passed",
    "all_256_octets",
    "nul_included",
    "history_retained_after_detach",
    "idempotent_original_pins",
    "wire_16mib_header_rejected",
    "blob_4mib_accepted",
    "blob_4mib_plus_one_rejected_without_revision",
    "snapshot_export_import_verified",
    "snapshot_12mib_rejected_without_revision",
    "sources_unchanged_during_run",
    "imported_runtime_matches_sources",
)


def invoke(script: str, *arguments: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(ROOT / script), *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=600,
    )
    # Build tools can print non-JSON progress before the final manifest. The
    # verifier scripts deliberately end with exactly one JSON evidence line.
    value = json.loads(completed.stdout.rstrip().splitlines()[-1])
    if not isinstance(value, dict):
        raise ValueError("worker verifier did not produce an evidence object")
    return value


def sources() -> dict[str, str]:
    paths = [
        *ROOT.glob("src/corpusledger/*.py"),
        *ROOT.glob("interop/**/*.py"),
        *ROOT.glob("interop/**/*.go"),
        *ROOT.glob("interop/**/*.java"),
        *ROOT.glob("interop/**/*.json"),
        ROOT / "interop/go/go.mod",
        ROOT / ATTACHMENT_ORACLE,
        ROOT / ".github/workflows/ci.yml",
        Path(__file__).resolve(),
    ]
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def attachment_evidence(value: dict[str, Any], inventory: dict[str, str]) -> None:
    """Require the actual native oracle's checks and source binding, not a skip."""
    if value.get("format") != "corpusledger.attachment-polyglot-oracle.v1" or any(
        value.get(name) is not True for name in ATTACHMENT_CHECKS
    ):
        raise ValueError("attachment verifier did not confirm every required contract")
    expected = {
        name: digest
        for name, digest in inventory.items()
        if name.startswith("src/corpusledger/") or name in ATTACHMENT_SOURCES
    }
    if not expected.keys() >= ATTACHMENT_SOURCES or value.get("source_sha256") != expected:
        raise ValueError("attachment verifier did not bind the expected source inventory")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    if args.artifacts is None:
        temporary = os.environ.get("RUNNER_TEMP")
        if not temporary:
            parser.error("--artifacts is required outside a CI runner")
        artifacts = Path(temporary) / "corpusledger-worker-evidence"
    else:
        artifacts = args.artifacts
    artifacts = artifacts.resolve()
    if artifacts == ROOT or ROOT in artifacts.parents or artifacts in ROOT.parents:
        parser.error("artifacts must be a fresh dedicated directory outside the repository")
    artifacts.mkdir(parents=True, exist_ok=False)
    before = sources()
    build_dir = artifacts / "build"
    build = invoke(
        "interop/build_workers.py",
        "--output",
        str(build_dir),
        "--dependency-cache",
        str(artifacts / "dependencies"),
        "--fetch",
    )
    worker = invoke("interop/verify_workers.py", "--build-dir", str(build_dir))
    service = invoke("interop/verify_execution.py", "--build-dir", str(build_dir))
    if worker.get("passed") is not True or service.get("passed") is not True:
        raise ValueError("cross-language verifier did not confirm success")
    attachments = invoke(ATTACHMENT_ORACLE, "--build-dir", str(build_dir))
    attachment_evidence(attachments, before)
    if before != sources():
        raise ValueError("source files changed during cross-language verification")
    report = {
        "format": "corpusledger.worker-ci-evidence.v1",
        "python": sys.version,
        "platform": sys.platform,
        "build": build,
        "worker_contract": worker,
        "event_service_contract": service,
        "attachment_contract": attachments,
        "source_sha256": before,
    }
    (artifacts / "evidence.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"passed": True, "evidence": str(artifacts / "evidence.json")}, sort_keys=True))


if __name__ == "__main__":
    main()
