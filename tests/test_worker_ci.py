"""CI orchestration regressions; mocks here are not native-process evidence."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


@pytest.fixture
def worker_ci() -> Any:
    path = Path(__file__).resolve().parents[1] / "scripts/worker_ci.py"
    spec = importlib.util.spec_from_file_location("corpusledger_worker_ci_review", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inventory(module: Any) -> dict[str, str]:
    return {
        name: "a" * 64 for name in {*module.ATTACHMENT_SOURCES, "src/corpusledger/__init__.py", "scripts/worker_ci.py"}
    }


def success(module: Any, sources: dict[str, str]) -> dict[str, Any]:
    return {
        "format": "corpusledger.attachment-polyglot-oracle.v1",
        **dict.fromkeys(module.ATTACHMENT_CHECKS, True),
        "source_sha256": {key: value for key, value in sources.items() if key != "scripts/worker_ci.py"},
    }


def test_source_inventory_covers_the_actual_attachment_entrypoint_and_native_sources(worker_ci: Any) -> None:
    result = worker_ci.sources()
    assert result.keys() >= worker_ci.ATTACHMENT_SOURCES
    assert ".github/workflows/ci.yml" in result
    assert "scripts/worker_ci.py" in result
    assert "src/corpusledger/annotation_attachments.py" in result
    assert all(len(value) == 64 for value in result.values())


@pytest.mark.parametrize("bad", [False, None, 1, "true"])
def test_each_required_native_check_must_be_explicitly_true(worker_ci: Any, bad: Any) -> None:
    sources = inventory(worker_ci)
    for name in worker_ci.ATTACHMENT_CHECKS:
        report = success(worker_ci, sources)
        report[name] = bad
        with pytest.raises(ValueError, match="required contract"):
            worker_ci.attachment_evidence(report, sources)


def test_missing_native_check_or_changed_source_cannot_be_promoted_to_success(worker_ci: Any) -> None:
    sources = inventory(worker_ci)
    report = success(worker_ci, sources)
    worker_ci.attachment_evidence(report, sources)
    del report["snapshot_export_import_verified"]
    with pytest.raises(ValueError, match="required contract"):
        worker_ci.attachment_evidence(report, sources)
    report = success(worker_ci, sources)
    report["source_sha256"][worker_ci.ATTACHMENT_ORACLE] = "b" * 64
    with pytest.raises(ValueError, match="source inventory"):
        worker_ci.attachment_evidence(report, sources)
    report = success(worker_ci, sources)
    del report["source_sha256"]["interop/java/AttachmentExample.java"]
    with pytest.raises(ValueError, match="source inventory"):
        worker_ci.attachment_evidence(report, sources)


@pytest.mark.parametrize("failure", [None, "worker", "service", "attachments", "source-change", "subprocess"])
def test_ci_runs_attachment_oracle_and_only_publishes_after_all_contracts_pass(
    worker_ci: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str | None,
) -> None:
    sources = inventory(worker_ci)
    snapshots = iter((sources, {**sources, "changed.py": "b" * 64} if failure == "source-change" else sources))
    monkeypatch.setattr(worker_ci, "sources", lambda: next(snapshots))
    artifacts = tmp_path / "evidence"
    monkeypatch.setattr(sys, "argv", ["worker_ci.py", "--artifacts", str(artifacts)])
    calls = []

    def invoke(script: str, *args: str) -> dict[str, Any]:
        calls.append((script, args))
        if script == "interop/build_workers.py":
            return {"build": "synthetic-orchestration-fixture"}
        if script == "interop/verify_workers.py":
            return {"passed": failure != "worker"}
        if script == "interop/verify_execution.py":
            return {"passed": failure != "service"}
        assert script == worker_ci.ATTACHMENT_ORACLE
        assert args == ("--build-dir", str(artifacts / "build"))
        if failure == "subprocess":
            raise subprocess.CalledProcessError(2, ["synthetic-fixture"])
        result = success(worker_ci, sources)
        if failure == "attachments":
            result["passed"] = False
        return result

    monkeypatch.setattr(worker_ci, "invoke", invoke)
    if failure is not None:
        with pytest.raises((ValueError, subprocess.CalledProcessError)):
            worker_ci.main()
        assert not (artifacts / "evidence.json").exists()
        assert capsys.readouterr().out == ""
        return
    worker_ci.main()
    assert [script for script, _ in calls] == [
        "interop/build_workers.py",
        "interop/verify_workers.py",
        "interop/verify_execution.py",
        worker_ci.ATTACHMENT_ORACLE,
    ]
    report = json.loads((artifacts / "evidence.json").read_text(encoding="utf-8"))
    assert report["attachment_contract"] == success(worker_ci, sources)
    assert report["source_sha256"] == sources
    assert json.loads(capsys.readouterr().out)["passed"] is True


def test_invoke_uses_current_python_and_checks_process_exit(worker_ci: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = []

    def run(command: list[str], **options: Any) -> Any:
        captured.append((command, options))
        return SimpleNamespace(stdout='tool progress\n{"passed": true}\n')

    monkeypatch.setattr(worker_ci.subprocess, "run", run)
    assert worker_ci.invoke(worker_ci.ATTACHMENT_ORACLE, "--build-dir", "fixture") == {"passed": True}
    command, options = captured[0]
    assert command == [sys.executable, str(worker_ci.ROOT / worker_ci.ATTACHMENT_ORACLE), "--build-dir", "fixture"]
    assert options["check"] is True and options["timeout"] == 600
    assert options["cwd"] == worker_ci.ROOT
