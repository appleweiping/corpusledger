"""Fast publication/inventory regressions; no corpus, worker or network required."""

import builtins
import hashlib
import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def benchmark(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "benchmarks"))
    return importlib.import_module("benchmark_annotation_service")


@pytest.fixture
def invocation(benchmark, monkeypatch, tmp_path):
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["benchmark", str(tmp_path / "archive.zip"), "--build-dir", str(tmp_path), "--output", str(output)],
    )
    monkeypatch.setattr(benchmark, "run", lambda *args, **kwargs: {"passed": True, "events": 1})
    return output


def test_success_publishes_complete_report(benchmark, invocation, capsys):
    benchmark.main()
    assert json.loads(invocation.read_text(encoding="utf-8")) == {"passed": True, "events": 1}
    assert json.loads(capsys.readouterr().out) == {"passed": True, "events": 1}
    assert list(invocation.parent.iterdir()) == [invocation]


def test_stdout_failure_acknowledges_published_report(benchmark, invocation, monkeypatch, capsys):
    real_print = builtins.print

    def fail_stdout(*args, **kwargs):
        if kwargs.get("file") is sys.stderr:
            return real_print(*args, **kwargs)
        raise BrokenPipeError("PRIVATE_DELIVERY_DETAIL")

    monkeypatch.setattr(builtins, "print", fail_stdout)
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2
    assert json.loads(invocation.read_text(encoding="utf-8")) == {"passed": True, "events": 1}
    assert capsys.readouterr().err == "annotation_service_benchmark_report_published_but_delivery_failed\n"
    assert list(invocation.parent.iterdir()) == [invocation]


@pytest.mark.parametrize("failure_at", ["run", "link"])
def test_prepublication_failure_does_not_claim_success(benchmark, invocation, monkeypatch, capsys, failure_at):
    def fail(*args, **kwargs):
        raise OSError("PRIVATE_FAILURE_DETAIL")

    if failure_at == "run":
        monkeypatch.setattr(benchmark, "run", fail)
    else:
        monkeypatch.setattr(benchmark.os, "link", fail)
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2
    assert not invocation.exists()
    assert capsys.readouterr().err == "annotation_service_benchmark_failed_no_report_published\n"
    assert list(invocation.parent.iterdir()) == []


def test_existing_report_is_not_replaced_or_benchmark_run(benchmark, invocation, monkeypatch, capsys):
    invocation.write_bytes(b"previous report")

    def unexpected_run(*args, **kwargs):
        pytest.fail("an existing output must be rejected before running the benchmark")

    monkeypatch.setattr(benchmark, "run", unexpected_run)
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2
    assert invocation.read_bytes() == b"previous report"
    assert capsys.readouterr().err == "annotation_service_benchmark_failed_no_report_published\n"


def test_concurrent_publisher_is_not_overwritten(benchmark, invocation, monkeypatch, capsys):
    real_link = benchmark.os.link

    def concurrent_link(source, destination):
        destination.write_bytes(b"concurrent report")
        return real_link(source, destination)

    monkeypatch.setattr(benchmark.os, "link", concurrent_link)
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2
    assert invocation.read_bytes() == b"concurrent report"
    assert capsys.readouterr().err == "annotation_service_benchmark_failed_no_report_published\n"
    assert list(invocation.parent.iterdir()) == [invocation]


def test_document_inventory_has_independent_canonical_framing(benchmark):
    inventory = hashlib.sha256()
    benchmark.update_document_inventory(inventory, 0, ("selected", "sibling"))
    benchmark.update_document_inventory(inventory, 1, ("selected2", "sibling2"))
    independent = (
        b'{"documents":["selected","sibling"],"ordinal":0}\n{"documents":["selected2","sibling2"],"ordinal":1}\n'
    )
    assert inventory.hexdigest() == hashlib.sha256(independent).hexdigest()


@pytest.mark.parametrize(
    ("ordinal", "documents"),
    [(1, ("selected", "sibling")), (0, ("sibling", "selected")), (0, ("selected",))],
)
def test_inventory_distinguishes_ordinals_order_and_missing_sibling(benchmark, ordinal, documents):
    original = hashlib.sha256()
    changed = hashlib.sha256()
    benchmark.update_document_inventory(original, 0, ("selected", "sibling"))
    benchmark.update_document_inventory(changed, ordinal, documents)
    assert original.digest() != changed.digest()
