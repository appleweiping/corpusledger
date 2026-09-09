"""Strict local CLI for versioned multi-document annotation events."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .annotation_cli import _write
from .annotation_store import AnnotationEvent, AnnotationStore
from .errors import InputError
from .strictjson import bounded_int, finite_float, object_without_duplicates, reject_constant

MAX_EVENT_BYTES = 128 * 1024 * 1024
_SIDECARS = ("-wal", "-shm", "-journal")


def configure_annotation_store_parser(parser: argparse.ArgumentParser) -> None:
    """Register local event import, export and audit commands; no dynamic code loading."""
    actions = parser.add_subparsers(dest="annotation_store_action", required=True)
    put = actions.add_parser("put", help="create an event or append a compare-and-swap revision")
    put.add_argument("database")
    put.add_argument("input", help="strict corpusledger.annotation-event.v1 JSON")
    put.add_argument(
        "--expected-revision", type=int, default=0, help="0 creates only; positive values require this head"
    )
    listing = actions.add_parser("list", help="list event heads in event-ID order")
    listing.add_argument("database")
    listing.add_argument("--after-event-id")
    listing.add_argument("--limit", type=int, default=100)
    get = actions.add_parser("get", help="export one complete event snapshot or one document")
    get.add_argument("database")
    get.add_argument("event_id")
    get.add_argument("--revision", type=int, help="omit to export the current head")
    get.add_argument("--document", help="export only this document ID as annotation-document JSON")
    history = actions.add_parser("history", help="list immutable revisions in ascending revision order")
    history.add_argument("database")
    history.add_argument("event_id")
    history.add_argument("--after-revision", type=int, default=0)
    history.add_argument("--limit", type=int, default=100)
    verify = actions.add_parser("verify", help="verify stored snapshots and revision digest chains")
    verify.add_argument("database")
    verify.add_argument("--event-id")
    for action in (put, listing, get, history, verify):
        action.add_argument("--output", help="atomically write JSON to this path instead of standard output")


def _same_file(left: Path, right: Path) -> bool:
    # Resolve catches symlinks; samefile also catches differently named hardlinks.
    # Permission/stat errors must fail closed instead of bypassing this guard.
    if left == right:
        return True
    try:
        left_stat = left.stat()
    except FileNotFoundError:
        return False
    try:
        right_stat = right.stat()
    except FileNotFoundError:
        return False
    return (left_stat.st_dev, left_stat.st_ino) == (right_stat.st_dev, right_stat.st_ino)


def _paths(args: argparse.Namespace) -> tuple[Path, Path | None, Path | None]:
    try:
        database = Path(args.database).resolve()
        reserved = (database, *(Path(str(database) + suffix).resolve() for suffix in _SIDECARS))
        source = Path(args.input).resolve() if args.annotation_store_action == "put" else None
        output = Path(args.output).resolve() if args.output is not None else None
        for index, path in enumerate(reserved):
            if any(_same_file(path, other) for other in reserved[:index]):
                raise InputError("annotation database and SQLite sidecars must not alias each other")
        if source is not None and any(_same_file(source, protected) for protected in reserved):
            raise InputError("annotation event input must not alias the database or SQLite sidecars")
        if output is not None:
            protected_paths = reserved if source is None else (*reserved, source)
            if any(_same_file(output, protected) for protected in protected_paths):
                raise InputError(
                    "annotation store output must not overwrite or alias database, SQLite sidecars, or input"
                )
            if output.exists() and not output.is_file():
                raise InputError("annotation store output must be a file, not a directory")
        return database, source, output
    except (OSError, RuntimeError, ValueError) as exc:
        raise InputError("cannot resolve/check annotation store file identities") from exc


def _load_event(source: Path) -> AnnotationEvent:
    try:
        with source.open("rb") as stream:
            raw = stream.read(MAX_EVENT_BYTES + 1)
        if len(raw) > MAX_EVENT_BYTES:
            raise InputError(f"annotation event input exceeds {MAX_EVENT_BYTES} bytes")
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
            parse_int=bounded_int,
        )
        return AnnotationEvent.from_dict(payload)
    except (OSError, UnicodeError) as exc:
        raise InputError(f"cannot read UTF-8 annotation event input {source}") from exc
    except (ValueError, RecursionError) as exc:
        raise InputError(f"invalid annotation event JSON: {exc}") from exc


def run_annotation_store_command(args: argparse.Namespace) -> int:
    """Protect every file identity before opening SQLite; validate imports before mutation."""
    database, source, output = _paths(args)
    action = args.annotation_store_action
    if action == "put" and (type(args.expected_revision) is not int or not 0 <= args.expected_revision < 2**63 - 1):
        raise InputError("expected revision must be a bounded non-negative integer")
    # Complete validation is deliberately outside the store context: malformed
    # input must not create even an empty database or modify an existing store.
    event = _load_event(source) if source is not None else None
    payload: dict[str, Any]
    try:
        with AnnotationStore(database, create=action == "put") as store:
            if action == "put":
                if event is None:  # Defensive guard for direct Namespace callers.
                    raise InputError("put requires an annotation event input")
                payload = store.put(event, expected_revision=args.expected_revision).to_dict()
            elif action == "get":
                revision = store.get(args.event_id, revision=args.revision)
                payload = (
                    revision.event.to_dict()
                    if args.document is None
                    else revision.event.get_document(args.document).to_dict()
                )
            elif action == "list":
                rows = store.list(after_event_id=args.after_event_id, limit=args.limit)
                payload = {
                    "format": "corpusledger.annotation-event-list.v1",
                    "events": [row.to_dict() for row in rows],
                    "after_event_id": args.after_event_id,
                    "last_event_id": rows[-1].event_id if rows else None,
                }
            elif action == "history":
                rows = store.history(args.event_id, after_revision=args.after_revision, limit=args.limit)
                payload = {
                    "format": "corpusledger.annotation-event-history.v1",
                    "event_id": args.event_id,
                    "revisions": [row.to_dict() for row in rows],
                    "after_revision": args.after_revision,
                    "last_revision": rows[-1].revision if rows else None,
                }
            elif action == "verify":
                payload = store.verify(args.event_id).to_dict()
            else:
                raise InputError(f"unknown annotation store command {action!r}")
    except KeyError as exc:
        raise InputError("annotation event, revision, or document was not found") from exc
    _write(payload, output)
    return 0
