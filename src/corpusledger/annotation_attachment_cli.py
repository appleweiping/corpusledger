"""Bounded local attachment commands with explicit migrations and exclusive exports."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import tempfile
from contextlib import suppress
from dataclasses import fields
from pathlib import Path
from typing import Any

from .annotation_attachment_snapshot import AnnotationAttachmentSnapshot, export_snapshot, import_snapshot
from .annotation_attachments import AnnotationAttachments, AttachmentConflictError
from .annotation_store import AnnotationConflictError, AnnotationRevision, AnnotationStore
from .annotation_store_cli import _same_file
from .attachment_types import AttachmentLimits
from .errors import InputError
from .strictjson import bounded_int, finite_float, object_without_duplicates, reject_constant

MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024
_SIDECARS = ("-wal", "-shm", "-journal")


class _CommandInputError(InputError):
    """A static diagnostic owned here, safe to retain across the private-data boundary."""


def configure_annotation_attachment_parser(parser: argparse.ArgumentParser) -> None:
    """Register local commands, without exposing remote migration or file overwrite."""
    actions = parser.add_subparsers(dest="attachment_action", required=True)
    enable_execution = actions.add_parser("enable-execution", help="explicitly upgrade existing schema v1 to v2")
    enable = actions.add_parser("enable", help="explicitly upgrade existing schema v2 to attachment schema v3")
    attach = actions.add_parser("attach", help="attach one bounded binary file as a new event revision")
    detach = actions.add_parser("detach", help="remove a name from the head; retain old revision bytes")
    get = actions.add_parser("get", help="export exact bytes from a pinned revision to a new file")
    listing = actions.add_parser("list", help="list manifests from a pinned revision")
    export = actions.add_parser("export", help="export one complete pinned event and its bytes to a new file")
    restore = actions.add_parser("import", help="atomically import one snapshot, not restore event history")
    for action in (enable_execution, enable, attach, detach, get, listing, export, restore):
        action.add_argument("database")
    for action in (attach, detach, get, listing, export):
        action.add_argument("event_id")
    for action in (attach, detach, get):
        action.add_argument("name", help="logical attachment name, not a storage path")
    attach.add_argument("input")
    attach.add_argument("--media-type", default="application/octet-stream")
    restore.add_argument("input")
    restore.add_argument("--snapshot-digest", required=True, help="caller-trusted snapshot SHA-256")
    for action in (attach, detach, restore):
        action.add_argument("--expected-revision", type=int, required=True)
        action.add_argument("--expected-digest", required=action is not restore)
        action.add_argument("--command-id", required=True, help="stable ID for explicit identical-request retry")
    for action in (get, listing, export):
        action.add_argument("--revision", type=int, required=True)
        action.add_argument("--expected-digest", required=True)
        action.add_argument("--output", required=action is not listing)
    for action in (attach, detach, get, listing, export, restore):
        for field in fields(AttachmentLimits):
            action.add_argument("--" + field.name.replace("_", "-"), type=int, default=field.default)


def _warning(message: str) -> None:
    with suppress(OSError, ValueError, UnicodeError, AttributeError):
        sys.stderr.write("corpusledger: " + message + "\n")
        sys.stderr.flush()


def _silence_failed_stdout() -> None:
    # An actual failed descriptor would otherwise fail again at interpreter exit.
    # Do not replace host-owned StringIO or change a successful stream.
    with suppress(OSError, ValueError, AttributeError):
        descriptor = sys.stdout.fileno()
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), descriptor)


def _stdout(payload: Any, *, committed: bool = False) -> None:
    text = json.dumps(payload, ensure_ascii=True, sort_keys=True, allow_nan=False) + "\n"
    try:
        if sys.stdout.write(text) != len(text):
            raise OSError("short output write")
        sys.stdout.flush()
    except (OSError, ValueError, UnicodeError, AttributeError):
        _silence_failed_stdout()
        if not committed:
            raise _CommandInputError(
                "attachment report could not be written; standard output may contain a prefix"
            ) from None
        _warning("attachment transaction committed, but its optional summary could not be written")


def _publish(data: bytes, destination: Path) -> None:
    temporary: str | None = None
    published = False
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".corpusledger-attachment-", dir=destination.parent)
        with os.fdopen(descriptor, "wb") as stream:
            if stream.write(data) != len(data):
                raise OSError("short file write")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
        published = True
    except OSError:
        raise _CommandInputError("attachment export could not be published to a new file") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                _warning(
                    "attachment export published; a private temporary copy remains"
                    if published
                    else "attachment export failed; a private temporary copy may remain"
                )


def _paths(args: argparse.Namespace) -> tuple[Path, Path | None, Path | None]:
    try:
        database = Path(args.database).resolve()
        if not stat.S_ISREG(database.stat().st_mode):
            raise _CommandInputError("attachment commands require an existing regular database file")
        reserved = (database, *(Path(str(database) + suffix).resolve() for suffix in _SIDECARS))
        for index, path in enumerate(reserved):
            if any(_same_file(path, previous) for previous in reserved[:index]):
                raise _CommandInputError("attachment database and SQLite sidecars must not alias")
        source = Path(args.input).resolve() if args.attachment_action in ("attach", "import") else None
        output_arg = getattr(args, "output", None)
        output = None
        if output_arg is not None:
            original = Path(output_arg)
            if os.path.lexists(original):
                raise _CommandInputError("attachment exports require a new output file")
            output = original.resolve()
            if not output.parent.is_dir():
                raise _CommandInputError("attachment export parent must already exist")
        if source is not None:
            if not stat.S_ISREG(source.stat().st_mode):
                raise _CommandInputError("attachment input must be a regular file")
            if any(_same_file(source, protected) for protected in reserved):
                raise _CommandInputError("attachment input must not alias the database or SQLite sidecars")
        if output is not None and any(
            _same_file(output, protected) for protected in (*reserved, *((source,) if source else ()))
        ):
            raise _CommandInputError("attachment output must not alias database, SQLite sidecars or input")
        return database, source, output
    except (OSError, RuntimeError, ValueError):
        raise _CommandInputError("cannot resolve or inspect attachment input/output identities") from None


def _read(source: Path, maximum: int) -> bytes:
    try:
        with source.open("rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise _CommandInputError("attachment input must be a regular file")
            raw = stream.read(maximum + 1)
        if len(raw) > maximum:
            raise _CommandInputError("attachment input exceeds its configured byte limit")
        return raw
    except OSError:
        raise _CommandInputError("cannot read attachment input") from None


def _snapshot(raw: bytes, expected_digest: str) -> AnnotationAttachmentSnapshot:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
            parse_int=bounded_int,
        )
        snapshot = AnnotationAttachmentSnapshot.from_dict(value)
        if snapshot.digest != expected_digest:
            raise InputError("attachment snapshot does not match the caller-declared digest")
        return snapshot
    except (InputError, ValueError, UnicodeError, RecursionError):
        raise _CommandInputError("invalid attachment snapshot JSON or digest") from None


def _receipt(revision: AnnotationRevision) -> dict[str, Any]:
    return {
        "format": "corpusledger.annotation-attachment-cli-receipt.v1",
        "private_data": True,
        "committed": True,
        "event_id": revision.event_id,
        "revision": revision.revision,
        "digest": revision.digest,
        "parent_digest": revision.parent_digest,
    }


def run_annotation_attachment_command(args: argparse.Namespace) -> int:
    """Complete validation before writes; never retry external/file publication implicitly."""
    database, source, output = _paths(args)
    action = args.attachment_action
    try:
        if action in ("enable-execution", "enable"):
            with AnnotationStore(database, create=False) as store:
                if action == "enable-execution":
                    store.enable_execution_journal()
                else:
                    store.enable_attachments()
                payload = {
                    "format": "corpusledger.annotation-attachment-schema.v1",
                    "committed": True,
                    "execution_enabled": store.execution_enabled,
                    "attachments_enabled": store.attachments_enabled,
                }
            _stdout(payload, committed=True)
            return 0
        limits = AttachmentLimits(**{field.name: getattr(args, field.name) for field in fields(AttachmentLimits)})
        raw = _read(source, limits.max_blob_bytes if action == "attach" else MAX_SNAPSHOT_BYTES) if source else None
        snapshot = _snapshot(raw, args.snapshot_digest) if action == "import" and raw is not None else None
        with AnnotationStore(database, create=False) as store:
            attachments = AnnotationAttachments(store, limits=limits)
            if action in ("attach", "detach", "import"):
                expected = {
                    "expected_revision": args.expected_revision,
                    "expected_digest": args.expected_digest,
                    "command_id": args.command_id,
                }
                if action == "attach" and raw is not None:
                    revision = attachments.attach(args.event_id, args.name, raw, args.media_type, **expected)
                elif action == "detach":
                    revision = attachments.detach(args.event_id, args.name, **expected)
                elif action == "import" and snapshot is not None:
                    revision = import_snapshot(store, snapshot, limits=limits, **expected)
                else:
                    raise _CommandInputError("attachment command is missing its validated input")
                payload = _receipt(revision)
            else:
                pinned = store.get(args.event_id, args.revision)
                if pinned.digest != args.expected_digest:
                    raise _CommandInputError("pinned attachment revision does not match its expected digest")
                if action == "get":
                    data = attachments.read(args.event_id, args.name, revision=args.revision)
                elif action == "export":
                    exported = export_snapshot(store, args.event_id, args.revision, limits=limits)
                    data = exported.to_bytes()
                elif action == "list":
                    payload = {
                        "format": "corpusledger.annotation-attachment-cli-list.v1",
                        "private_data": True,
                        "event_id": pinned.event_id,
                        "revision": pinned.revision,
                        "digest": pinned.digest,
                        "attachments": [
                            item.to_dict() for item in attachments.list(args.event_id, revision=args.revision)
                        ],
                    }
                    data = json.dumps(payload, ensure_ascii=True, sort_keys=True, allow_nan=False).encode("utf-8")
                else:
                    raise _CommandInputError("unknown attachment command")
        if action in ("attach", "detach", "import"):
            _stdout(payload, committed=True)
        elif output is not None:
            _publish(data, output)
        elif action == "list":
            _stdout(payload)
        else:
            raise _CommandInputError("binary/snapshot attachment export requires a new output file")
        return 0
    except KeyError:
        raise InputError("attachment event, revision or name was not found") from None
    except _CommandInputError:
        raise
    except (AnnotationConflictError, AttachmentConflictError):
        raise InputError(
            "attachment command conflict; verify the expected revision, digest, name and command ID"
        ) from None
    except InputError:
        # Typed document decoding can include private type/feature names even
        # before the stored content digest is checked. Do not echo that cause.
        raise InputError("attachment input or stored data could not be validated") from None
