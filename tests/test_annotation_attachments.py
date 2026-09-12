"""Binary preservation, transactional publication and exact attachment boundaries."""

from __future__ import annotations

import base64
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace

import pytest

from corpusledger.annotation_attachment_snapshot import (
    AnnotationAttachmentSnapshot,
    AttachmentBlob,
    export_snapshot,
    import_snapshot,
    preview_import_snapshot,
)
from corpusledger.annotation_attachments import (
    AnnotationAttachments,
    AttachmentConflictError,
    AttachmentCorruptionError,
)
from corpusledger.annotation_store import (
    AnnotationConflictError,
    AnnotationEvent,
    AnnotationStore,
    AnnotationStoreError,
)
from corpusledger.annotations import AnnotationDocument
from corpusledger.attachment_types import AttachmentError, AttachmentLimits, AttachmentManifest, AttachmentQuotaError
from corpusledger.errors import InputError


def event(identifier="event"):
    return AnnotationEvent(
        identifier,
        (AnnotationDocument("original", "雪 😀\x00\r\n"), AnnotationDocument("sibling", "keep")),
        {"true": True},
    )


@pytest.fixture
def store(tmp_path):
    with AnnotationStore(tmp_path / "annotations.sqlite") as opened:
        opened.put(event())
        opened.enable_execution_journal()
        opened.enable_attachments()
        yield opened


def attach(
    store, name="image.bin", data=b"\x00\xff\xfe" + bytes(range(256)), command="attach", limits=None, event_id="event"
):
    source = store.get(event_id)
    return AnnotationAttachments(store, limits).attach(
        event_id,
        name,
        data,
        "application/octet-stream",
        expected_revision=source.revision,
        expected_digest=source.digest,
        command_id=command,
    )


def raw_rows(store):
    return tuple(
        store._connection.execute(
            "SELECT event_id,revision,digest,CAST(body AS BLOB) FROM revisions ORDER BY event_id,revision"
        )
    ), tuple(store._connection.execute("SELECT digest,CAST(body AS BLOB) FROM documents ORDER BY digest"))


def counts(store):
    return tuple(
        store._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("annotation_blobs", "annotation_attachment_receipts", "revisions", "documents")
    )


def test_v1_event_roundtrip_and_digest_are_exactly_historical():
    original = event()
    value = {
        "format": "corpusledger.annotation-event.v1",
        "id": original.event_id,
        "documents": [doc.to_dict() for doc in original.documents],
        "metadata": {"true": True},
    }
    expected = hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert original.to_dict() == value
    assert original.digest == expected
    assert AnnotationEvent.from_dict(value) == original
    manifest = AttachmentManifest("raw", hashlib.sha256(b"").hexdigest(), 0, "application/octet-stream")
    with pytest.raises(AnnotationStoreError, match="explicit"):
        replace(original, attachments=(manifest,))
    with pytest.raises(AnnotationStoreError):
        replace(original, version=True)
    modern = replace(original, attachments=(manifest,), version=2)
    assert AnnotationEvent.from_dict(modern.to_dict()) == modern
    assert modern.digest != original.digest
    assert replace(modern, attachments=()).version == 2
    with pytest.raises(FrozenInstanceError):
        modern.version = 1


@pytest.mark.parametrize("content", [b"", b"\x00", bytes(range(256)), b"\xff\xfe\x80\x00\r\n"])
def test_arbitrary_bytes_pin_read_detach_history_and_reopen(store, content):
    original = store.get("event")
    manager = AnnotationAttachments(store)
    preview = manager.preview_attach(
        "event",
        "raw",
        content,
        "application/octet-stream",
        expected_revision=1,
        expected_digest=original.digest,
        command_id="a",
    )
    assert counts(store) == (0, 0, 1, 2)
    written = manager.attach(
        "event",
        "raw",
        content,
        "application/octet-stream",
        expected_revision=1,
        expected_digest=original.digest,
        command_id="a",
    )
    assert written == preview
    assert manager.read("event", "raw", revision=2) == content
    assert manager.list("event", revision=2) == written.event.attachments
    before = raw_rows(store)
    detach_preview = manager.preview_detach(
        "event", "raw", expected_revision=2, expected_digest=written.digest, command_id="d"
    )
    detached = manager.detach("event", "raw", expected_revision=2, expected_digest=written.digest, command_id="d")
    assert detached == detach_preview
    assert detached.event.version == 2 and detached.event.attachments == ()
    assert detached.event.documents == original.event.documents
    assert detached.event.metadata == original.event.metadata
    assert raw_rows(store)[0][:2] == before[0]
    assert manager.read("event", "raw", revision=2) == content
    with pytest.raises(KeyError):
        manager.read("event", "raw", revision=3)
    assert (
        manager.attach(
            "event",
            "raw",
            content,
            "application/octet-stream",
            expected_revision=1,
            expected_digest=original.digest,
            command_id="a",
        )
        == written
    )
    assert manager.receipt("a") == written
    assert counts(store) == (1, 2, 3, 2)
    assert store.verify().to_dict() == {
        "format": "corpusledger.annotation-store-verification.v1",
        "events": 1,
        "revisions": 3,
        "documents": 2,
        "valid": True,
    }
    with AnnotationStore(store.path, create=False) as reopened:
        other = AnnotationAttachments(reopened)
        assert other.read("event", "raw", revision=2) == content
        assert (
            other.detach("event", "raw", expected_revision=2, expected_digest=written.digest, command_id="d")
            == detached
        )


def test_dedup_across_names_and_events_but_logical_bytes_count_each_name(store):
    first = attach(store, "first", b"abc", "one")
    attach(store, "second", b"abc", "two")
    store.put(event("other"))
    attach(store, "third", b"abc", "three", event_id="other")
    assert counts(store)[:2] == (1, 3)
    assert sum(item.size for item in store.get("event").event.attachments) == 6
    with pytest.raises(AttachmentQuotaError):
        attach(store, "fourth", b"abc", "four", limits=AttachmentLimits(max_event_bytes=8))
    assert AnnotationAttachments(store).read("event", "first", revision=first.revision) == b"abc"


@pytest.mark.parametrize("changed", ["bytes", "name", "media", "revision", "digest", "action"])
def test_command_id_cannot_change_request(store, changed):
    source = store.get("event")
    first = attach(store, "raw", b"a", "command")
    manager = AnnotationAttachments(store)
    kwargs = {
        "event_id": "event",
        "name": "raw",
        "data": b"a",
        "media_type": "application/octet-stream",
        "expected_revision": 1,
        "expected_digest": source.digest,
        "command_id": "command",
    }
    if changed == "action":
        with pytest.raises(AttachmentConflictError):
            manager.detach("event", "raw", expected_revision=1, expected_digest=source.digest, command_id="command")
    else:
        key, value = {
            "bytes": ("data", b"b"),
            "name": ("name", "new"),
            "media": ("media_type", "text/plain"),
            "revision": ("expected_revision", 2),
            "digest": ("expected_digest", "f" * 64),
        }[changed]
        kwargs[key] = value
        with pytest.raises(AttachmentConflictError):
            manager.attach(**kwargs)
    assert store.get("event") == first and counts(store)[:2] == (1, 1)


def test_no_name_overwrites_and_bad_pin_has_no_effects(store):
    first = attach(store, data=b"a")
    with pytest.raises(AttachmentConflictError, match="already exists"):
        attach(store, data=b"new", command="other")
    with pytest.raises(AttachmentConflictError, match="digest"):
        AnnotationAttachments(store).attach(
            "event", "next", b"b", "text/plain", expected_revision=2, expected_digest="0" * 64, command_id="bad"
        )
    assert store.get("event") == first and counts(store)[:2] == (1, 1)


@pytest.mark.parametrize("table", ["annotation_blobs", "revisions", "annotation_attachment_receipts"])
def test_insert_failures_roll_back_blobs_revision_and_receipt(store, table):
    original = store.get("event")
    before = counts(store)
    store._connection.execute(
        f"CREATE TRIGGER injected BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'fault'); END"
    )
    with pytest.raises(AnnotationStoreError, match="transaction failed"):
        attach(store)
    assert counts(store) == before and store.get("event") == original
    store._connection.execute("DROP TRIGGER injected")
    assert attach(store).revision == 2


def test_two_actual_connections_cas_one_winner_and_no_orphan(store):
    source = store.get("event")
    barrier = threading.Barrier(2)

    def writer(index):
        with AnnotationStore(store.path, create=False) as opened:
            manager = AnnotationAttachments(opened)
            barrier.wait(timeout=10)
            try:
                value = manager.attach(
                    "event",
                    f"raw{index}",
                    bytes([index]),
                    "application/octet-stream",
                    expected_revision=1,
                    expected_digest=source.digest,
                    command_id=f"command{index}",
                )
                return "success", value.revision
            except AnnotationConflictError as error:
                return "conflict", error.actual_revision

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer, index) for index in (1, 2)]
        assert sorted(future.result(timeout=20) for future in futures) == [("conflict", 2), ("success", 2)]
    assert counts(store)[:3] == (1, 1, 2)


def test_same_command_concurrent_connections_share_original_result(store):
    source = store.get("event")
    barrier = threading.Barrier(2)

    def writer():
        with AnnotationStore(store.path, create=False) as opened:
            barrier.wait(timeout=10)
            return AnnotationAttachments(opened).attach(
                "event",
                "raw",
                b"same",
                "text/plain",
                expected_revision=1,
                expected_digest=source.digest,
                command_id="same",
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer) for _ in range(2)]
        results = [future.result(timeout=20) for future in futures]
    assert results[0] == results[1] and counts(store)[:3] == (1, 1, 2)


@pytest.mark.parametrize(
    ("limits", "first", "second"),
    [
        (AttachmentLimits(max_store_bytes=3), b"abc", b"d"),
        (AttachmentLimits(max_store_blobs=1), b"a", b"b"),
        (AttachmentLimits(max_names=1), b"a", b"a"),
        (AttachmentLimits(max_receipts=1), b"a", b"a"),
        (AttachmentLimits(max_event_bytes=3), b"abc", b"a"),
    ],
)
def test_exact_configured_quota_plus_one_is_atomic(store, limits, first, second):
    original = store.get("event")
    value = attach(store, "a", first, "a", limits)
    before = counts(store)
    with pytest.raises(AttachmentQuotaError):
        attach(store, "b", second, "b", limits)
    assert counts(store) == before and store.get("event") == value
    assert (
        AnnotationAttachments(store, limits).attach(
            "event",
            "a",
            first,
            "application/octet-stream",
            expected_revision=1,
            expected_digest=original.digest,
            command_id="a",
        )
        == value
    )


def test_exact_four_mib_and_sixtyfour_name_hard_limits(store):
    data = b"\0" * (4 * 1024 * 1024)
    first = attach(store, "a", data, "a")
    second = attach(store, "b", data, "b")  # Logical8MiB, physical4MiB.
    with pytest.raises(AttachmentQuotaError):
        attach(store, "c", b"x", "c")
    with pytest.raises(AttachmentQuotaError):
        attach(store, "d", data + b"x", "d")
    assert counts(store)[:2] == (1, 2)
    assert AnnotationAttachments(store).read("event", "a", revision=first.revision) == data
    empty_digest = hashlib.sha256(b"").hexdigest()
    existing = second.event.attachments
    allowed = tuple(
        AttachmentManifest(f"n{index:02d}", empty_digest, 0, "application/octet-stream") for index in range(64)
    )
    assert len(replace(event(), attachments=allowed, version=2).attachments) == 64
    with pytest.raises(AttachmentQuotaError):
        replace(event(), attachments=(*allowed, AttachmentManifest("extra", empty_digest, 0, "text/plain")), version=2)
    assert store.get("event").event.attachments == existing


@pytest.mark.parametrize("mode", ["missing", "hash", "text", "declared_size", "huge_blob"])
def test_corrupt_blob_read_list_get_verify_reject_before_unbounded_fetch(store, mode):
    written = attach(store, data=b"abc")
    digest = written.event.attachments[0].sha256
    store._connection.execute("DROP TRIGGER annotation_blobs_no_update")
    store._connection.execute("DROP TRIGGER annotation_blobs_no_delete")
    if mode == "missing":
        store._connection.execute("DELETE FROM annotation_blobs")
    elif mode == "hash":
        store._connection.execute("UPDATE annotation_blobs SET body=?", (b"def",))
    elif mode == "text":
        store._connection.execute("UPDATE annotation_blobs SET body='abc'")
    elif mode == "declared_size":
        store._connection.execute("UPDATE annotation_blobs SET size=4")
    else:
        store._connection.execute("UPDATE annotation_blobs SET body=zeroblob(4194305),size=4194305")
    # Restore guards: corruption, not schema mismatch, must be what fails.
    from corpusledger.annotation_store import _ATTACHMENT_GUARDS

    for name, statement in _ATTACHMENT_GUARDS.items():
        if name.startswith("annotation_blobs"):
            store._connection.execute(statement)
    manager = AnnotationAttachments(store)
    for operation in (
        lambda: manager.read("event", "image.bin", revision=2),
        lambda: manager.list("event", revision=2),
        lambda: store.get("event"),
        store.verify,
    ):
        with pytest.raises(AnnotationStoreError, match="BLOB"):
            operation()
    assert store._connection.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == 2
    assert len(digest) == 64


def test_missing_blob_cannot_be_smuggled_through_ordinary_put_or_old_schema(tmp_path, store):
    manifest = AttachmentManifest("missing", "0" * 64, 0, "text/plain")
    new = replace(event(), version=2, attachments=(manifest,))
    with pytest.raises(AnnotationStoreError, match="BLOB"):
        store.put(new, expected_revision=1)
    with AnnotationStore(tmp_path / "old.sqlite") as old:
        with pytest.raises(AnnotationStoreError, match="enable"):
            old.put(replace(event(), version=2))
        assert old.verify().revisions == 0


def test_legacy_caller_cannot_downgrade_v2_and_silently_drop_manifest(store):
    written = attach(store)
    before = counts(store), raw_rows(store)
    legacy_update = AnnotationEvent("event", (AnnotationDocument("new", "changed"),), {"legacy": True})
    with pytest.raises(AnnotationStoreError, match="downgraded"):
        store.put(legacy_update, expected_revision=written.revision)
    assert (counts(store), raw_rows(store)) == before
    assert store.get("event") == written
    assert store.get("event", 1).event.version == 1
    # Low-level explicit v2 empty manifest remains a supported event replacement;
    # it has ordinary put provenance, not a fabricated attachment receipt.
    cleared = store.put(replace(written.event, attachments=()), expected_revision=written.revision)
    assert cleared.event.version == 2 and cleared.event.attachments == ()
    assert counts(store)[:2] == (1, 1)
    assert AnnotationAttachments(store).read("event", "image.bin", revision=written.revision)


def test_snapshot_cannot_downgrade_existing_v2_event(store):
    legacy = export_snapshot(store, "event", 1)
    written = attach(store)
    before = counts(store), raw_rows(store)
    with pytest.raises(AnnotationStoreError, match="downgraded"):
        preview_import_snapshot(
            store, legacy, command_id="downgrade", expected_revision=2, expected_digest=written.digest
        )
    with pytest.raises(AnnotationStoreError, match="downgraded"):
        import_snapshot(store, legacy, command_id="downgrade", expected_revision=2, expected_digest=written.digest)
    assert (counts(store), raw_rows(store)) == before


def snapshot(store):
    attach(store, "a", b"\x00\xff" + bytes(range(256)), "a")
    attach(store, "b", b"\x00\xff" + bytes(range(256)), "b")
    return export_snapshot(store, "event", 3)


def resign(value):
    value["sha256"] = hashlib.sha256(
        json.dumps(
            {key: item for key, item in value.items() if key != "sha256"},
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return value


def test_snapshot_roundtrip_dedup_atomic_import_and_pinned_retry(store, tmp_path):
    exported = snapshot(store)
    assert len(exported.blobs) == 1 and len(exported.event.attachments) == 2
    assert AnnotationAttachmentSnapshot.from_dict(exported.to_dict()) == exported
    with AnnotationStore(tmp_path / "import.sqlite") as target:
        target.enable_execution_journal()
        target.enable_attachments()
        predicted = preview_import_snapshot(target, exported, command_id="import")
        assert counts(target) == (0, 0, 0, 0)
        imported = import_snapshot(target, exported, command_id="import")
        assert imported == predicted
        assert imported.revision == 1 and imported.parent_digest is None
        assert imported.event == exported.event and imported.digest != exported.source_digest
        assert counts(target) == (1, 1, 1, 2)
        assert AnnotationAttachments(target).read("event", "a", revision=1) == exported.blobs[0].data
        attach(target, "later", b"later", "later")
        assert import_snapshot(target, exported, command_id="import") == imported
        with pytest.raises(AttachmentConflictError):
            import_snapshot(
                target, exported, command_id="import", expected_revision=2, expected_digest=target.get("event").digest
            )


@pytest.mark.parametrize(
    "mode", ["missing", "extra", "duplicate", "size", "hash", "base64", "padding", "source", "envelope", "format"]
)
def test_rehashed_snapshot_tampering_is_rejected(store, mode):
    value = snapshot(store).to_dict()
    blob = value["blobs"][0]
    if mode == "missing":
        value["blobs"] = []
    elif mode == "extra":
        value["blobs"].append(AttachmentBlob(hashlib.sha256(b"extra").hexdigest(), b"extra").to_dict())
    elif mode == "duplicate":
        value["blobs"].append(dict(blob))
    elif mode == "size":
        blob["size"] += 1
    elif mode == "hash":
        blob["sha256"] = "0" * 64
    elif mode == "base64":
        blob["base64"] = "!" + blob["base64"][1:]
    elif mode == "padding":
        # Add forbidden whitespace while leaving decoded bytes unchanged.
        blob["base64"] += "\n"
    elif mode == "source":
        value["source"]["event_id"] = "renamed"
    elif mode == "envelope":
        value["extra"] = True
    else:
        value["format"] = "future"
    with pytest.raises(InputError):
        AnnotationAttachmentSnapshot.from_dict(resign(value))


def test_snapshot_noncanonical_base64_pad_bits_and_invalid_bytes():
    value = {"sha256": hashlib.sha256(b"a").hexdigest(), "size": 1, "base64": "YR=="}
    assert base64.b64decode(value["base64"]) == b"a"
    with pytest.raises(AttachmentError, match="canonical"):
        AttachmentBlob.from_dict(value)
    for data in (bytearray(b"a"), "a", b"x" * (4 * 1024 * 1024 + 1)):
        with pytest.raises(AttachmentError):
            AttachmentBlob("0" * 64, data)


def test_snapshot_cap_is_checked_before_whole_event_dict_expansion(store, monkeypatch):
    from corpusledger import annotation_attachment_snapshot as module

    huge = replace(event(), documents=(AnnotationDocument("huge", "x" * 5000),))
    monkeypatch.setattr(module, "MAX_SNAPSHOT_BYTES", 4096)
    monkeypatch.setattr(
        AnnotationEvent, "to_dict", lambda self: (_ for _ in ()).throw(AssertionError("whole event expanded"))
    )
    with pytest.raises(AttachmentError, match="byte limit"):
        AnnotationAttachmentSnapshot(huge, 1, "a" * 64)


def test_snapshot_full_envelope_not_only_body_cap(store, monkeypatch):
    from corpusledger import annotation_attachment_snapshot as module

    exported = export_snapshot(store, "event", 1)
    size = len(exported.to_bytes())
    monkeypatch.setattr(module, "MAX_SNAPSHOT_BYTES", size - 1)
    with pytest.raises(AttachmentError, match="byte limit"):
        AnnotationAttachmentSnapshot(exported.event, exported.source_revision, exported.source_digest)


def test_snapshot_chunked_unicode_hash_and_escaped_json_match_independent_encoding(monkeypatch):
    from corpusledger import annotation_attachment_snapshot as module
    from corpusledger.attachment_types import bounded_json

    text = '雪\x00\n"\\😀' * 1000
    value = {"text": text, "metadata": {"false": False, "number": 2.5}}
    expected = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert bounded_json(value, len(expected)) == expected
    with pytest.raises(AttachmentQuotaError):
        bounded_json(value, len(expected) - 1)
    snapshot_event = AnnotationEvent("event", (AnnotationDocument("doc", text),))
    copied = AnnotationAttachmentSnapshot(snapshot_event, 1, "a" * 64)
    assert copied.to_dict()["event"] == snapshot_event.to_dict()
    # UTF-8 size, not codepoint count, exceeds this cap. Hashing must not call the
    # document's full-string encoding property before admitting the text.
    monkeypatch.setattr(module, "MAX_SNAPSHOT_BYTES", 4096)
    monkeypatch.setattr(
        AnnotationDocument,
        "text_sha256",
        property(lambda self: (_ for _ in ()).throw(AssertionError("unbounded hash"))),
    )
    with pytest.raises(AttachmentQuotaError):
        AnnotationAttachmentSnapshot(AnnotationEvent("event", (AnnotationDocument("d", "雪" * 2000),)), 1, "a" * 64)


def test_snapshot_import_fault_rolls_back_new_blobs_documents_revision_and_receipt(store, tmp_path):
    exported = snapshot(store)
    with AnnotationStore(tmp_path / "target.sqlite") as target:
        target.enable_execution_journal()
        target.enable_attachments()
        target._connection.execute(
            "CREATE TRIGGER fault BEFORE INSERT ON annotation_attachment_receipts "
            "BEGIN SELECT RAISE(ABORT,'fault'); END"
        )
        with pytest.raises(AnnotationStoreError):
            import_snapshot(target, exported, command_id="import")
        assert counts(target) == (0, 0, 0, 0)
        target._connection.execute("DROP TRIGGER fault")
        imported = import_snapshot(target, exported, command_id="import")
        assert imported.revision == 1
        assert counts(target) == (1, 1, 1, 2)


@pytest.mark.parametrize(
    "kwargs", [{"max_blob_bytes": True}, {"max_names": 65}, {"max_receipts": -1}, {"max_store_bytes": 10**400}]
)
def test_quota_configuration_is_strict(kwargs):
    with pytest.raises(AttachmentError):
        AttachmentLimits(**kwargs)


def test_receipt_tampering_and_missing_result_are_not_success(store):
    attach(store)
    store._connection.execute("DROP TRIGGER annotation_attachment_receipts_no_update")
    store._connection.execute("UPDATE annotation_attachment_receipts SET body='{}'")
    from corpusledger.annotation_store import _ATTACHMENT_GUARDS

    store._connection.execute(_ATTACHMENT_GUARDS["annotation_attachment_receipts_no_update"])
    with pytest.raises(AttachmentCorruptionError):
        AnnotationAttachments(store).receipt("attach")


@pytest.mark.parametrize("name", ["", ".", "..", "../x", "a/b", "a\\b", "C:x", "nul\0", " leading", "\ud800"])
def test_logical_names_not_paths(name):
    with pytest.raises(AttachmentError):
        AttachmentManifest(name, "0" * 64, 0, "text/plain")


@pytest.mark.parametrize(
    "change",
    [
        {"size": True},
        {"size": -1},
        {"sha256": "A" * 64},
        {"media_type": "text/plain; charset=utf8"},
        {"media_type": "TEXT/PLAIN"},
        {"media_type": "../x"},
    ],
)
def test_manifest_closed_immutable_types(change):
    fields = {"name": "data", "sha256": "0" * 64, "size": 0, "media_type": "text/plain"}
    with pytest.raises(AttachmentError):
        AttachmentManifest(**{**fields, **change})
    with pytest.raises(InputError):
        AttachmentManifest.from_dict({**fields, "path": "ignored"})
