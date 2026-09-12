"""Real local HTTP attachment publication, recovery and verification boundaries."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest

from corpusledger.annotation_client import AnnotationClient, AnnotationClientError
from corpusledger.annotation_execution import RemoteAnnotationPipeline
from corpusledger.annotation_protocol import AnnotationResponse, ProcessorDescription, encode_wire
from corpusledger.annotation_remote import RemoteAnnotationProcessor
from corpusledger.annotation_service import (
    AnnotationServer,
    _decode_attachment_data,
    _encode_attachment_data,
    create_annotation_server,
    main,
)
from corpusledger.annotation_store import AnnotationEvent, AnnotationRevision, AnnotationStore
from corpusledger.annotations import AnnotationDocument, AnnotationType, SpanAnnotation
from corpusledger.attachment_types import AttachmentLimits
from corpusledger.errors import InputError

TOKEN_ENV = "CORPUSLEDGER_ATTACHMENT_TEST_TOKEN"
TOKEN = "local-authored-attachment-test-token-76543"


@pytest.fixture(autouse=True)
def credential(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, TOKEN)


def source() -> AnnotationEvent:
    return AnnotationEvent(
        "event",
        (AnnotationDocument("original", "Exact 😀\r\ntext"), AnnotationDocument("sibling", "KEEP")),
        {"typed": [True, 1, 1.0]},
    )


@contextmanager
def running(path: Path, *, enabled: bool = True, pipelines: Any = None, **options: Any) -> Iterator[AnnotationServer]:
    server = create_annotation_server(
        path,
        {} if pipelines is None else pipelines,
        token_env=TOKEN_ENV,
        enable_execution_journal=enabled,
        enable_attachments=enabled,
        **options,
    )
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


@pytest.mark.parametrize("data", [b"", bytes(range(256)), b"\x00\xff\x00not text\r\n"])
def test_canonical_base64_preserves_opaque_bytes(data: bytes) -> None:
    encoded = _encode_attachment_data(data)
    assert encoded == base64.b64encode(data).decode("ascii")
    assert _decode_attachment_data(encoded, maximum=len(data)) == data


@pytest.mark.parametrize("data", [None, True, [], "AA==\n", "AA===", "AB==", "_w==", "💣", "data:text/plain,a"])
def test_noncanonical_base64_is_rejected_without_echoing_content(data: Any) -> None:
    with pytest.raises(InputError):
        _decode_attachment_data(data)


def test_decoded_byte_budget_checks_padding_not_only_encoded_length() -> None:
    assert len(base64.b64encode(b"a")) == len(base64.b64encode(b"abc"))
    with pytest.raises(InputError):
        _decode_attachment_data("YWJj", maximum=1)


def test_attachment_migration_is_a_separate_local_opt_in(tmp_path: Path) -> None:
    path = tmp_path / "events.sqlite"
    with running(path, enabled=False) as server:
        assert AnnotationClient(server.endpoint, TOKEN_ENV).create(source()).revision == 1
        with AnnotationStore(path, create=False) as store:
            assert not store.execution_enabled and not store.attachments_enabled
    with pytest.raises(InputError):
        create_annotation_server(path, {}, token_env=TOKEN_ENV, enable_attachments=True)
    with AnnotationStore(path, create=False) as store:
        assert not store.execution_enabled and not store.attachments_enabled
    with running(path), AnnotationStore(path, create=False) as store:
        assert store.execution_enabled and store.attachments_enabled
        assert store.get("event").event.digest == source().digest
    with running(path, enabled=False), AnnotationStore(path, create=False) as store:
        assert store.attachments_enabled


@pytest.mark.parametrize("options", [{"enable_attachments": 1}, {"attachment_limits": {}}, {"attachment_limits": True}])
def test_invalid_attachment_configuration_does_not_create_database(tmp_path: Path, options: Any) -> None:
    path = tmp_path / "absent.sqlite"
    with pytest.raises(InputError):
        create_annotation_server(path, {}, token_env=TOKEN_ENV, **options)
    assert not path.exists()


def test_service_snapshots_and_revalidates_local_attachment_limits(tmp_path: Path) -> None:
    limits = AttachmentLimits(max_store_bytes=128)
    with running(tmp_path / "store", attachment_limits=limits) as server:
        assert server.attachment_limits == limits
        assert server.attachment_limits is not limits


def attached(client: AnnotationClient, data: bytes = bytes(range(256)), name: str = "payload") -> AnnotationRevision:
    initial = client.create(source())
    return client.attach(
        "event",
        name,
        data,
        "application/octet-stream",
        expected_revision=initial.revision,
        expected_digest=initial.digest,
        command_id="attach-1",
    )


def test_attach_read_list_detach_and_old_history_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "store"
    data = bytes(range(256)) + b"\x00\x00\xff"
    with running(path) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        result = attached(client, data)
        assert result.revision == 2 and result.event.version == 2
        assert result.event.documents == source().documents
        assert result.event.metadata == source().metadata
        assert client.get("event").digest == result.digest
        assert client.read_attachment("event", "payload", revision=2, expected_digest=result.digest) == data
        assert client.list_attachments("event", revision=2, expected_digest=result.digest) == result.event.attachments
        empty = client.attach(
            "event", "empty", b"", "text/plain", expected_revision=2, expected_digest=result.digest, command_id="empty"
        )
        assert client.read_attachment("event", "empty", revision=3, expected_digest=empty.digest) == b""
        detached = client.detach(
            "event", "payload", expected_revision=3, expected_digest=empty.digest, command_id="detach"
        )
        assert detached.revision == 4
        assert [item.name for item in detached.event.attachments] == ["empty"]
        retry = client.attach(
            "event",
            "payload",
            data,
            "application/octet-stream",
            expected_revision=1,
            expected_digest=result.parent_digest,
            command_id="attach-1",
        )
        assert retry.digest == result.digest and client.get("event").revision == 4
        with pytest.raises(AnnotationClientError, match="not_found"):
            client.read_attachment("event", "payload", revision=4, expected_digest=detached.digest)
    with running(path, enabled=False) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        assert client.read_attachment("event", "payload", revision=2, expected_digest=result.digest) == data
        assert [info.revision for info in client.history("event")] == [1, 2, 3, 4]


def test_lost_response_explicit_same_command_retry_returns_original_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        initial = client.create(source())
        dispatch = server.dispatch
        lost = False

        def lose_once(command: str, arguments: dict[str, Any]) -> Any:
            nonlocal lost
            result = dispatch(command, arguments)
            if command == "attachment_attach" and not lost:
                lost = True
                raise OSError("private postcommit transport loss")
            return result

        monkeypatch.setattr(server, "dispatch", lose_once)
        args = {"expected_revision": 1, "expected_digest": initial.digest, "command_id": "lost"}
        with pytest.raises(AnnotationClientError, match="transport_failed"):
            client.attach("event", "data", b"\x00\xff", "application/octet-stream", **args)
        assert client.get("event").revision == 2
        recovered = client.attach("event", "data", b"\x00\xff", "application/octet-stream", **args)
        assert recovered.revision == 2 and len(client.history("event")) == 2
        with pytest.raises(AnnotationClientError, match="conflict"):
            client.attach("event", "data", b"changed", "application/octet-stream", **args)


def test_independent_connections_racing_one_head_have_exactly_one_winner(tmp_path: Path) -> None:
    with running(tmp_path / "store") as server:
        initial = AnnotationClient(server.endpoint, TOKEN_ENV).create(source())
        barrier = threading.Barrier(2)

        def publish(name: str) -> str:
            client = AnnotationClient(server.endpoint, TOKEN_ENV)
            barrier.wait(timeout=2)
            try:
                client.attach(
                    "event",
                    name,
                    name.encode(),
                    "text/plain",
                    expected_revision=1,
                    expected_digest=initial.digest,
                    command_id=name,
                )
                return "committed"
            except AnnotationClientError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(publish, ("first", "second"))) == ["committed", "conflict"]
        assert AnnotationClient(server.endpoint, TOKEN_ENV).get("event").revision == 2


def test_snapshot_import_is_pinned_new_history_and_explicitly_idempotent(tmp_path: Path) -> None:
    with running(tmp_path / "source") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        origin = attached(client)
        snapshot = client.export_attachment_snapshot("event", revision=2, expected_digest=origin.digest)
        assert snapshot.source == {"event_id": "event", "revision": 2, "digest": origin.digest}
    with running(tmp_path / "target") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        with pytest.raises(AnnotationClientError, match="request_invalid"):
            client.import_attachment_snapshot(snapshot, command_id="import", expected_snapshot_digest="0" * 64)
        assert client.list() == ()
        imported = client.import_attachment_snapshot(
            snapshot, command_id="import", expected_snapshot_digest=snapshot.digest
        )
        assert imported.revision == 1 and imported.parent_digest is None
        assert imported.event.digest == origin.event.digest and imported.digest != origin.digest
        assert client.read_attachment("event", "payload", revision=1, expected_digest=imported.digest) == bytes(
            range(256)
        )
        retry = client.import_attachment_snapshot(
            snapshot, command_id="import", expected_snapshot_digest=snapshot.digest
        )
        assert retry.digest == imported.digest and len(client.history("event")) == 1
        replaced = client.import_attachment_snapshot(
            snapshot,
            command_id="replace",
            expected_snapshot_digest=snapshot.digest,
            expected_revision=1,
            expected_digest=imported.digest,
        )
        assert replaced.revision == 2 and replaced.parent_digest == imported.digest


@pytest.mark.parametrize(
    "command",
    [
        "attachment_attach",
        "attachment_detach",
        "attachment_get",
        "attachment_list",
        "attachment_snapshot_export",
        "attachment_snapshot_import",
    ],
)
def test_remote_requests_cannot_enable_schema(tmp_path: Path, command: str) -> None:
    from corpusledger.annotation_service import _ARGUMENTS

    path = tmp_path / "store"
    with running(path, enabled=False) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        with pytest.raises(AnnotationClientError, match="attachments_disabled"):
            client._exchange(command, dict.fromkeys(_ARGUMENTS[command]))
    with AnnotationStore(path, create=False) as store:
        assert not store.execution_enabled and not store.attachments_enabled


def test_all_pinned_reads_reject_different_digest(tmp_path: Path) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        attached(client)
        for method, args in (
            (client.read_attachment, ("event", "payload")),
            (client.list_attachments, ("event",)),
            (client.export_attachment_snapshot, ("event",)),
        ):
            with pytest.raises(AnnotationClientError, match="conflict"):
                method(*args, revision=2, expected_digest="0" * 64)


def test_response_budget_is_checked_before_mutation(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with AnnotationStore(path) as store:
        initial = store.put(AnnotationEvent("event", (AnnotationDocument("large", "x" * 3000),)))
    with running(path, max_wire_bytes=1024) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        with pytest.raises(AnnotationClientError, match="too_large"):
            client.attach(
                "event",
                "a",
                b"",
                "text/plain",
                expected_revision=1,
                expected_digest=initial.digest,
                command_id="budget",
            )
    with AnnotationStore(path, create=False) as store:
        assert store.get("event").revision == 1
        assert store._connection.execute("SELECT count(*) FROM annotation_blobs").fetchone()[0] == 0
        assert store._connection.execute("SELECT count(*) FROM annotation_attachment_receipts").fetchone()[0] == 0


def test_store_quota_failure_is_atomic_and_receipt_retry_survives_exhaustion(tmp_path: Path) -> None:
    with running(tmp_path / "store", attachment_limits=AttachmentLimits(max_store_bytes=2, max_receipts=1)) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        initial = client.create(source())
        with pytest.raises(AnnotationClientError, match="too_large"):
            client.attach(
                "event",
                "large",
                b"abc",
                "text/plain",
                expected_revision=1,
                expected_digest=initial.digest,
                command_id="over",
            )
        result = client.attach(
            "event", "small", b"ab", "text/plain", expected_revision=1, expected_digest=initial.digest, command_id="ok"
        )
        with pytest.raises(AnnotationClientError, match="too_large"):
            client.detach("event", "small", expected_revision=2, expected_digest=result.digest, command_id="no-receipt")
        retry = client.attach(
            "event", "small", b"ab", "text/plain", expected_revision=1, expected_digest=initial.digest, command_id="ok"
        )
        assert retry.digest == result.digest and client.get("event").revision == 2


def test_corrupt_blob_is_redacted_unavailable_without_partial_bytes(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with running(path) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        result = attached(client, b"safe")
        # Fault injection in this disposable test DB deliberately bypasses the
        # append-only guard, then restores its exact definition before reopening.
        with closing(sqlite3.connect(path)) as connection, connection:
            definition = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='annotation_blobs_no_update'"
            ).fetchone()[0]
            connection.execute("DROP TRIGGER annotation_blobs_no_update")
            connection.execute("UPDATE annotation_blobs SET body=?", (b"EVIL",))
            connection.execute(definition)
        with pytest.raises(AnnotationClientError) as caught:
            client.read_attachment("event", "payload", revision=2, expected_digest=result.digest)
        assert caught.value.code == "unavailable"
        assert "EVIL" not in str(caught.value) and caught.value.__cause__ is None


@pytest.mark.parametrize("change", ["pin", "bool_revision", "name", "sha", "size", "base64", "extra"])
def test_client_rejects_self_inconsistent_attachment_data_over_real_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        result = attached(client, b"a")
        dispatch = server.dispatch

        def malformed(command: str, arguments: dict[str, Any]) -> Any:
            reply = dispatch(command, arguments)
            if command == "attachment_get":
                if change == "pin":
                    reply["digest"] = "0" * 64
                elif change == "bool_revision":
                    reply["revision"] = True
                elif change == "name":
                    reply["attachment"]["name"] = "other"
                elif change == "sha":
                    reply["attachment"]["sha256"] = hashlib.sha256(b"b").hexdigest()
                elif change == "size":
                    reply["attachment"]["size"] = 2
                elif change == "base64":
                    reply["data"] = "YR=="
                else:
                    reply["extra"] = None
            return reply

        monkeypatch.setattr(server, "dispatch", malformed)
        with pytest.raises(AnnotationClientError, match="response_invalid"):
            client.read_attachment("event", "payload", revision=2, expected_digest=result.digest)


def test_existing_execution_preserves_v2_manifest_binary_and_unselected_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    description = ProcessorDescription("span", "1", "a" * 64, produces=(AnnotationType("span"),))
    worker = RemoteAnnotationProcessor("http://127.0.0.1:49998", description, TOKEN_ENV)
    pipeline = RemoteAnnotationPipeline("spans", "1", (worker,))
    calls = []
    monkeypatch.setattr(RemoteAnnotationProcessor, "verify", lambda _: None)

    def execute(_: Any, incoming: Any) -> AnnotationResponse:
        calls.append(incoming.operation_id)
        return AnnotationResponse(
            incoming.operation_id,
            incoming.step_id,
            description,
            incoming.document.digest,
            (SpanAnnotation("whole", "span", 0, len(incoming.document.text)),),
        )

    monkeypatch.setattr(RemoteAnnotationProcessor, "execute", execute)
    with running(tmp_path / "store", pipelines={"spans": pipeline}) as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        original = attached(client)
        assert (
            client.begin(
                "op", "event", {"original": "spans"}, expected_revision=2, expected_digest=original.digest
            ).status
            == "ready"
        )
        assert not calls
        assert client.resume("op", {"original": "spans"}).status == "committed"
        final = client.get("event")
        assert final.revision == 3 and final.event.version == 2
        assert final.event.attachments == original.event.attachments
        assert final.event.get_document("sibling").digest == original.event.get_document("sibling").digest
        assert len(final.event.get_document("original").annotations) == 1
        assert client.read_attachment("event", "payload", revision=3, expected_digest=final.digest) == bytes(range(256))
        assert client.resume("op", {"original": "spans"}).status == "committed" and calls == ["op"]


@pytest.mark.parametrize("change", ["command_id", "request", "manifest"])
def test_self_consistent_revision_with_wrong_attachment_command_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        initial = client.create(source())
        dispatch = server.dispatch

        def malformed(command: str, arguments: dict[str, Any]) -> Any:
            result = dispatch(command, arguments)
            if command == "attachment_attach":
                if change == "command_id":
                    result["provenance"]["annotation_attachment"]["command_id"] = "different"
                elif change == "request":
                    result["provenance"]["annotation_attachment"]["request"]["expected_revision"] = True
                else:
                    result["event"]["attachments"][0]["name"] = "not-requested"
                event = AnnotationEvent.from_dict(result["event"])
                descriptor = {
                    "format": "corpusledger.annotation-store.v2",
                    "event_id": event.event_id,
                    "metadata": result["event"]["metadata"],
                    "documents": [{"id": doc.document_id, "digest": doc.digest} for doc in event.documents],
                    "attachments": result["event"]["attachments"],
                    "revision": result["revision"],
                    "parent_digest": result["parent_digest"],
                    "provenance": result["provenance"],
                }
                result["digest"] = hashlib.sha256(encode_wire(descriptor)).hexdigest()
            return result

        monkeypatch.setattr(server, "dispatch", malformed)
        with pytest.raises(AnnotationClientError, match="response_invalid"):
            client.attach(
                "event",
                "payload",
                b"a",
                "text/plain",
                expected_revision=1,
                expected_digest=initial.digest,
                command_id="right",
            )


@pytest.mark.parametrize("change", ["duplicate", "unsorted", "extra", "not-list", "invalid-manifest"])
def test_client_validates_complete_manifest_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        first = attached(client)
        result = client.attach(
            "event", "a", b"a", "text/plain", expected_revision=2, expected_digest=first.digest, command_id="second"
        )
        dispatch = server.dispatch

        def malformed(command: str, arguments: dict[str, Any]) -> Any:
            reply = dispatch(command, arguments)
            if command == "attachment_list":
                if change == "duplicate":
                    reply["attachments"].append(reply["attachments"][0])
                elif change == "unsorted":
                    reply["attachments"].reverse()
                elif change == "extra":
                    reply["unexpected"] = None
                elif change == "not-list":
                    reply["attachments"] = None
                else:
                    reply["attachments"][0]["size"] = True
            return reply

        monkeypatch.setattr(server, "dispatch", malformed)
        with pytest.raises(AnnotationClientError, match="response_invalid"):
            client.list_attachments("event", revision=3, expected_digest=result.digest)


@pytest.mark.parametrize("kind", ["revision", "digest", "name", "data", "media", "command", "read-none", "import-none"])
def test_invalid_typed_input_is_rejected_before_any_socket_io(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    def forbidden(*_: Any) -> None:
        pytest.fail("invalid typed input reached network")

    monkeypatch.setattr(AnnotationClient, "_exchange", forbidden)
    client = AnnotationClient("http://127.0.0.1:49998", TOKEN_ENV)
    fields = {
        "event_id": "event",
        "name": "payload",
        "data": b"a",
        "media_type": "text/plain",
        "expected_revision": 1,
        "expected_digest": "a" * 64,
        "command_id": "command",
    }
    changes = {
        "revision": ("expected_revision", True),
        "digest": ("expected_digest", None),
        "name": ("name", "../path"),
        "data": ("data", bytearray(b"a")),
        "media": ("media_type", "text/plain;charset=utf8"),
        "command": ("command_id", None),
    }
    with pytest.raises(AnnotationClientError):
        if kind == "read-none":
            client.list_attachments("event", revision=None, expected_digest="a" * 64)
        elif kind == "import-none":
            client.import_attachment_snapshot(None, command_id="x", expected_snapshot_digest="a" * 64)
        else:
            key, value = changes[kind]
            fields[key] = value
            client.attach(**fields)


@pytest.mark.parametrize("alter", ["unknown", "missing", "bool_revision", "bad_digest", "path_name", "bad_base64"])
def test_raw_attachment_commands_are_closed_and_strict(tmp_path: Path, alter: str) -> None:
    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        initial = client.create(source())
        arguments = {
            "event_id": "event",
            "name": "file",
            "data": "YQ==",
            "media_type": "text/plain",
            "expected_revision": 1,
            "expected_digest": initial.digest,
            "command_id": "raw",
        }
        if alter == "unknown":
            arguments["url"] = "http://untrusted"
        elif alter == "missing":
            del arguments["command_id"]
        else:
            key, value = {
                "bool_revision": ("expected_revision", True),
                "bad_digest": ("expected_digest", None),
                "path_name": ("name", "C:\\private"),
                "bad_base64": ("data", "YR=="),
            }[alter]
            arguments[key] = value
        with pytest.raises(AnnotationClientError, match="request_invalid"):
            client._exchange("attachment_attach", arguments)
        assert client.get("event").revision == 1


def test_snapshot_external_pin_is_checked_on_server_before_import(tmp_path: Path) -> None:
    with running(tmp_path / "source") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        original = attached(client)
        snapshot = client.export_attachment_snapshot("event", revision=2, expected_digest=original.digest)
    with running(tmp_path / "target") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        with pytest.raises(AnnotationClientError, match="conflict"):
            client._exchange(
                "attachment_snapshot_import",
                {
                    "snapshot": snapshot.to_dict(),
                    "command_id": "import",
                    "expected_revision": 0,
                    "expected_digest": None,
                    "expected_snapshot_digest": "0" * 64,
                },
            )
        assert client.list() == ()


@pytest.mark.parametrize("change", ["source-pin", "payload", "extra"])
def test_snapshot_export_client_checks_pin_and_complete_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from corpusledger.annotation_attachment_snapshot import AnnotationAttachmentSnapshot

    with running(tmp_path / "store") as server:
        client = AnnotationClient(server.endpoint, TOKEN_ENV)
        selected = attached(client)
        dispatch = server.dispatch

        def malformed(command: str, arguments: dict[str, Any]) -> Any:
            result = dispatch(command, arguments)
            if command == "attachment_snapshot_export":
                if change == "source-pin":
                    snapshot = AnnotationAttachmentSnapshot.from_dict(result)
                    result = AnnotationAttachmentSnapshot(
                        snapshot.event, snapshot.source_revision, "0" * 64, snapshot.blobs
                    ).to_dict()
                elif change == "payload":
                    result["blobs"][0]["base64"] = "AA=="
                else:
                    result["extra"] = None
            return result

        monkeypatch.setattr(server, "dispatch", malformed)
        with pytest.raises(AnnotationClientError, match="response_invalid"):
            client.export_attachment_snapshot("event", revision=2, expected_digest=selected.digest)


def test_module_startup_exposes_separate_flags_and_local_quota(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from corpusledger.annotation_service import CONFIG_FORMAT

    path = tmp_path / "store"
    config = tmp_path / "workers.json"
    config.write_text(json.dumps({"format": CONFIG_FORMAT, "pipelines": []}), encoding="utf-8")
    observed = []

    def serve(server: AnnotationServer, **_: Any) -> None:
        observed.append(server.attachment_limits.max_store_bytes)
        with AnnotationStore(server.store_path, create=False) as store:
            assert store.attachments_enabled

    monkeypatch.setattr(AnnotationServer, "serve_forever", serve)
    assert (
        main(
            [
                "--store",
                str(path),
                "--config",
                str(config),
                "--token-env",
                TOKEN_ENV,
                "--enable-execution-journal",
                "--enable-attachments",
                "--attachment-max-store-bytes",
                "123",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    assert observed == [123] and output.out.startswith("http://127.0.0.1:") and TOKEN not in output.out
    missing = tmp_path / "absent"
    assert (
        main(
            [
                "--store",
                str(missing),
                "--config",
                str(config),
                "--token-env",
                TOKEN_ENV,
                "--enable-execution-journal",
                "--enable-attachments",
                "--attachment-max-store-bytes",
                "-1",
            ]
        )
        == 2
    )
    assert not missing.exists()
    assert TOKEN not in capsys.readouterr().err


def test_invalid_detach_is_rejected_without_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_: Any) -> None:
        pytest.fail("invalid detach reached network")

    monkeypatch.setattr(AnnotationClient, "_exchange", forbidden)
    with pytest.raises(AnnotationClientError, match="request_invalid"):
        AnnotationClient("http://127.0.0.1:49998", TOKEN_ENV).detach(
            "event", "bad/path", expected_revision=1, expected_digest="a" * 64, command_id="detach"
        )
