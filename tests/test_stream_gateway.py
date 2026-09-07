from __future__ import annotations

import json

import pytest

from corpusledger import NdjsonGateway


def test_ndjson_gateway_keeps_response_alignment_and_digests() -> None:
    gateway = NdjsonGateway(max_line_bytes=512)
    gateway.register("uppercase", lambda payload: {"text": str(payload["text"]).upper()})
    responses, report = gateway.process_lines(
        [
            json.dumps({"processor": "uppercase", "payload": {"text": "hello"}}),
            "not json\n",
            json.dumps({"processor": "missing", "payload": {}}).encode(),
        ]
    )
    assert json.loads(responses[0]) == {
        "ok": True,
        "processor": "uppercase",
        "result": {"text": "HELLO"},
    }
    assert json.loads(responses[1])["ok"] is False
    assert json.loads(responses[2])["ok"] is False
    assert report.records == 3 and report.successes == 1 and report.failures == 2
    assert len(report.input_digest) == 64 and len(report.output_digest) == 64


def test_ndjson_gateway_rejects_oversized_and_invalid_processor_results() -> None:
    gateway = NdjsonGateway(max_line_bytes=8)
    gateway.register("bad", lambda _payload: ["not an object"])  # type: ignore[return-value]
    responses, report = gateway.process_lines([b'{"x": 1}\n', b"123456789\n"])
    assert all(json.loads(item)["ok"] is False for item in responses)
    assert report.failures == 2
    with pytest.raises(ValueError, match="already registered"):
        gateway.register("bad", lambda payload: payload)
    with pytest.raises(ValueError, match="positive"):
        NdjsonGateway(max_line_bytes=0)


def test_ndjson_gateway_validation_and_utf8_errors() -> None:
    gateway = NdjsonGateway()
    assert gateway.processors == ()
    with pytest.raises(ValueError, match="non-empty token"):
        gateway.register("bad name", lambda payload: payload)
    with pytest.raises(TypeError, match="callable"):
        gateway.register("bad", None)  # type: ignore[arg-type]
    gateway.register("echo", lambda payload: payload)
    responses, report = gateway.process_lines([b"\xff\n", 42])  # type: ignore[list-item]
    assert all(json.loads(item)["ok"] is False for item in responses)
    assert report.failures == 2
