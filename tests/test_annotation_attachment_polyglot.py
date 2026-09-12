"""Independent fixture oracles and an explicit, opt-in real native-process run."""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="module")
def example():
    path = Path(__file__).resolve().parents[1] / "examples/annotation_attachment_polyglot.py"
    specification = importlib.util.spec_from_file_location("_attachment_polyglot_example_tests", path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_fixed_binary_oracle_contains_every_octet_at_the_raw_limit(example):
    payload = example.binary_fixture()
    assert len(payload) == 4_194_304
    assert payload[:256] == bytes(range(256))
    assert payload.count(b"\x00") == 16_384
    assert payload.count(b"\xff") == 16_384
    assert hashlib.sha256(payload).hexdigest() == "2b07811057df887086f06a67edc6ebf911de8b6741156e7a2eb1416a4b8b1b2e"


def test_unicode_oracle_uses_hand_counted_codepoint_offsets(example):
    # Emoji is one codepoint; combining accent is separate; CR and LF remain two.
    assert list(example.TEXT) == ["A", "😀", " ", "e", "\u0301", "\r", "\n", "\t", "終", "\u00a0", "Z"]
    assert example.TOKEN_SPANS == ((0, 2, "A😀"), (3, 5, "e\u0301"), (8, 9, "終"), (10, 11, "Z"))
    assert [(start, end, example.TEXT[start:end]) for start, end, _ in example.TOKEN_SPANS] == [
        (0, 2, "A😀"),
        (3, 5, "e\u0301"),
        (8, 9, "終"),
        (10, 11, "Z"),
    ]


def test_source_inventory_pins_runtime_and_all_example_sources(example):
    inventory = example.source_inventory()
    expected = {
        "src/corpusledger/annotation_service.py",
        "src/corpusledger/annotation_client.py",
        "src/corpusledger/annotation_attachments.py",
        "src/corpusledger/annotation_attachment_snapshot.py",
        "examples/annotation_attachment_polyglot.py",
        "interop/verify_workers.py",
        "interop/go/attachment_example/main.go",
        "interop/java/AttachmentExample.java",
        "interop/java/dependencies.json",
    }
    assert expected <= inventory.keys()
    assert all(len(value) == 64 and set(value) <= set("0123456789abcdef") for value in inventory.values())
    assert not any(Path(name).is_absolute() for name in inventory)
    assert example.runtime_matches_sources(inventory)
    changed = {**inventory, "src/corpusledger/annotation_service.py": "0" * 64}
    assert not example.runtime_matches_sources(changed)


def test_build_cannot_write_to_checkout(example):
    with pytest.raises(ValueError, match="outside"):
        example.compile_examples(example.ROOT / "new-unsafe-build", {})


def test_build_cannot_replace_an_existing_directory(example, tmp_path, monkeypatch):
    sentinel = tmp_path / "existing"
    sentinel.write_bytes(b"original")
    monkeypatch.setattr(example.shutil, "which", lambda name: name)
    monkeypatch.setattr(example.subprocess, "run", lambda *args, **kwargs: pytest.fail("must not compile"))
    with pytest.raises(FileExistsError):
        example.compile_examples(tmp_path, {})
    assert sentinel.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [sentinel]


def test_build_descriptor_is_bounded_and_closed(example, tmp_path):
    manifest = tmp_path / "build.json"
    manifest.write_bytes(b"x" * 65_537)
    with pytest.raises(ValueError, match="too large"):
        example.verified_workers(tmp_path)
    manifest.write_text('{"go_worker":"not sufficient"}', encoding="utf-8")
    with pytest.raises(ValueError, match="unexpected"):
        example.verified_workers(tmp_path)


@pytest.mark.parametrize(
    ("stdout", "stderr", "returncode"),
    [
        (b'{"verified":false}', b"", 0),
        (b'{"verified":true}', b"unexpected diagnostics", 0),
        (b'{"verified":true}', b"", 2),
        (b"x" * 65_537, b"", 0),
        (b'{"verified":true,"secret":"PRIVATE_CREDENTIAL"}', b"", 0),
    ],
    ids=["unverified", "diagnostics", "failed-process", "over-limit", "credential-disclosure"],
)
def test_native_wrapper_does_not_promote_invalid_process_results(example, monkeypatch, stdout, stderr, returncode):
    monkeypatch.setattr(
        example.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode),
    )
    with pytest.raises(RuntimeError) as failure:
        example.native_call(["unused"], {}, {"TEST_TOKEN": "PRIVATE_CREDENTIAL"})
    assert "PRIVATE_CREDENTIAL" not in str(failure.value)


def test_failure_never_publishes_a_success_report(example, monkeypatch, capsys, tmp_path):
    def fail(_):
        raise RuntimeError("PRIVATE_DETAILS_DO_NOT_PRINT")

    monkeypatch.setattr(example, "verify", fail)
    assert example.main(["--build-dir", str(tmp_path)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "attachment_polyglot_oracle_failed\n"
    assert list(tmp_path.iterdir()) == []


def test_optimized_python_cannot_skip_oracle_assertions(example, tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            str(example.ROOT / "examples/annotation_attachment_polyglot.py"),
            "--build-dir",
            str(tmp_path),
        ],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == b""
    assert result.stderr.strip() == b"attachment_polyglot_oracle_failed"
    assert list(tmp_path.iterdir()) == []


def test_real_go_upload_python_reopen_java_read_and_attachment_dag(example):
    build = os.environ.get("CORPUSLEDGER_ATTACHMENT_WORKER_BUILD")
    if not build:
        pytest.skip("set CORPUSLEDGER_ATTACHMENT_WORKER_BUILD to an existing verified worker build")
    report = example.verify(Path(build))
    assert report["passed"] is True
    assert report["kind"] == "original-synthetic-real-process-contract-check"
    assert report["payload_bytes"] == 4_194_304
    assert report["payload_sha256"] == "2b07811057df887086f06a67edc6ebf911de8b6741156e7a2eb1416a4b8b1b2e"
    assert report["all_256_octets"] is True and report["nul_included"] is True
    assert report["event_revisions"] == [1, 2, 3, 4]
    assert report["snapshot_import_revisions"] == [1]
    assert report["snapshot_limit_event_revisions"] == [1, 2]
    assert (report["service_processes"], report["native_upload_calls"], report["native_read_calls"]) == (3, 3, 3)
    assert (report["text_worker_steps"], report["tokens"], report["reference_groups"]) == (2, 4, 1)
    for name in (
        "history_retained_after_detach",
        "idempotent_original_pins",
        "wire_16mib_header_rejected",
        "blob_4mib_accepted",
        "blob_4mib_plus_one_rejected_without_revision",
        "snapshot_export_import_verified",
        "snapshot_12mib_rejected_without_revision",
        "sources_unchanged_during_run",
        "imported_runtime_matches_sources",
    ):
        assert report[name] is True, name
    assert report["blob_oversize_status"] == report["snapshot_oversize_status"] == 413
    assert report["snapshot_rejection"] == "too_large"
    assert "AttachmentExample.class" in report["java_class_sha256"]
    assert "AttachmentExample$LimitedBody.class" in report["java_class_sha256"]
    assert len(report["java_class_sha256"]) >= 3
    # Report is aggregate evidence, not a covert dump of fixture bytes or auth.
    encoded = json.dumps(report)
    assert example.TEXT not in encoded and "Bearer " not in encoded
    assert "payload_file" not in encoded and "padding" not in encoded
