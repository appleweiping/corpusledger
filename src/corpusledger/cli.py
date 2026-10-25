"""``corpusledger snapshot`` and ``corpusledger diff``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .diff import compare
from .errors import CorpusLedgerError, InputError, SnapshotError
from .report import render_json, render_text
from .snapshot import Match, Snapshot, build_snapshot, is_snapshot


def _names(value: str) -> tuple[str, ...]:
    names = tuple(part.strip() for part in value.split(",") if part.strip())
    if not names:
        raise argparse.ArgumentTypeError("expected one or more comma-separated field names")
    return names


def _matching_options(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--id", metavar="FIELD", help="match records by this ID field")
    group.add_argument(
        "--key",
        metavar="FIELDS",
        type=_names,
        help="match records by a hash of these fields, e.g. instruction,input",
    )
    group.add_argument(
        "--content", action="store_true", help="match records by their whole content"
    )
    parser.add_argument(
        "--ignore",
        metavar="FIELDS",
        type=_names,
        default=(),
        help="leave these fields out of every hash, e.g. text or /meta/timestamp",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corpusledger",
        description="See what changed between two versions of a dataset.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    snap = commands.add_parser(
        "snapshot",
        help="save record IDs and hashes so a later version can be diffed against them",
    )
    snap.add_argument("data", help="file or directory")
    snap.add_argument("-o", "--output", required=True, help="snapshot file to write (.json)")
    _matching_options(snap)

    diff = commands.add_parser(
        "diff",
        help="compare two versions (data files, directories or snapshots)",
        description="Compare two versions of a dataset. Each side may be a data file, a "
        "directory or a snapshot. Exit status: 0 no differences, 1 differences, 2 error.",
    )
    diff.add_argument("before")
    diff.add_argument("after")
    _matching_options(diff)
    diff.add_argument("--json", action="store_true", help="print the full result as JSON")
    diff.add_argument(
        "--examples", type=int, default=3, help="examples per kind of change (default 3)"
    )
    diff.add_argument("-o", "--output", help="write the report to a file instead of stdout")
    return parser


def _requested(args: argparse.Namespace) -> Match | None:
    if args.id:
        return Match("id", (args.id,))
    if args.key:
        return Match("key", tuple(args.key))
    if args.content:
        return Match("content")
    return None


def _load_side(path: str, match: Match | None, ignore: tuple[str, ...]) -> Snapshot:
    if is_snapshot(path):
        snapshot = Snapshot.load(path)
        if match is not None and snapshot.match != match:
            raise SnapshotError(
                f"{path} matches records by {snapshot.match.describe()}, "
                f"not {match.describe()}; snapshot again with the same options"
            )
        if snapshot.ignore != ignore:
            raise SnapshotError(
                f"{path} was made with --ignore {','.join(snapshot.ignore) or '(nothing)'}; "
                "use the same --ignore on both sides"
            )
        return snapshot
    return build_snapshot(path, match, ignore=ignore, keep_rows=True)


def _diff(args: argparse.Namespace) -> int:
    match = _requested(args)
    ignore = tuple(args.ignore)
    reference = next((side for side in (args.before, args.after) if is_snapshot(side)), None)
    if reference is not None:
        stored = Snapshot.load(reference)
        match = match or stored.match
        if not args.ignore:
            ignore = stored.ignore
    before = _load_side(args.before, match, ignore)
    after = _load_side(args.after, match or before.match, ignore)
    result = compare(before, after)
    text = render_json(result) if args.json else render_text(result, max(args.examples, 0))
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 1 if result.has_changes else 0


def _snapshot(args: argparse.Namespace) -> int:
    source, output = Path(args.data).resolve(), Path(args.output).resolve()
    if source == output:
        raise InputError("the snapshot would overwrite the data")
    if output.exists() and not is_snapshot(output):
        raise InputError(f"{args.output} exists and is not a snapshot; refusing to overwrite it")
    snapshot = build_snapshot(args.data, _requested(args), ignore=tuple(args.ignore))
    snapshot.save(args.output)
    note = f", {snapshot.duplicates} repeated IDs" if snapshot.duplicates else ""
    print(
        f"{len(snapshot.entries):,} records matched by {snapshot.match.describe()}{note}; "
        f"wrote {args.output}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return _snapshot(args) if args.command == "snapshot" else _diff(args)
    except CorpusLedgerError as exc:
        print(f"corpusledger: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
