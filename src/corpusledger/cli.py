"""Command-line interface for CorpusLedger."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .canonical import CanonicalPolicy
from .diff import compare
from .errors import CorpusLedgerError, InputError, ManifestError
from .manifest import Manifest, build_manifest
from .privacy import PrivacyConfig
from .reporting import render


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corpusledger",
        description="Reproducible manifests and diffs for JSON NLP corpora",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot = subparsers.add_parser("snapshot", help="create a corpus manifest")
    snapshot.add_argument("input")
    snapshot.add_argument("output")
    snapshot.add_argument("--id-field", default="id")
    snapshot.add_argument("--algorithm", choices=("sha256", "blake2b"), default="sha256")
    snapshot.add_argument("--sort-lists", action="store_true", help="treat lists as set-like (use with care)")

    difference = subparsers.add_parser("diff", help="compare two manifests")
    difference.add_argument("before")
    difference.add_argument("after")
    difference.add_argument("--format", choices=("json", "markdown"), default="markdown")
    difference.add_argument("--output")

    verify = subparsers.add_parser("verify", help="rebuild and verify a manifest")
    verify.add_argument("manifest")
    verify.add_argument("--input", help="override the source path recorded in the manifest")
    return parser


def _write_report(text: str, destination: str | None) -> None:
    if destination:
        try:
            Path(destination).write_text(text, encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise InputError(f"cannot write report {destination}: {exc}") from exc
    else:
        sys.stdout.write(text)


def run(argv: list[str] | None = None) -> int:
    """Execute the CLI and return a process status."""
    args = _parser().parse_args(argv)
    if args.command == "snapshot":
        input_path = Path(args.input).resolve()
        output_path = Path(args.output).resolve()
        if input_path == output_path:
            raise InputError("snapshot output must not overwrite the input corpus file")
        if output_path.exists():
            try:
                Manifest.load(output_path)
            except ManifestError as exc:
                raise InputError("snapshot output already exists and is not a CorpusLedger manifest") from exc
        policy = CanonicalPolicy(list_strategy="sort" if args.sort_lists else "preserve")
        manifest = build_manifest(
            input_path,
            id_field=args.id_field,
            algorithm=args.algorithm,
            policy=policy,
            exclude_paths=(output_path,),
        )
        manifest.save(args.output)
        print(f"wrote {len(manifest.records)} records to {args.output}")
        return 0
    if args.command == "diff":
        if args.output:
            output_path = Path(args.output).resolve()
            inputs = {Path(args.before).resolve(), Path(args.after).resolve()}
            if output_path in inputs:
                raise InputError("diff output must not overwrite an input manifest")
        result = compare(Manifest.load(args.before), Manifest.load(args.after))
        _write_report(render(result, args.format), args.output)
        return 1 if result.has_changes else 0
    existing = Manifest.load(args.manifest)
    source = args.input or existing.source
    if Path(source).resolve() == Path(args.manifest).resolve():
        raise InputError("verification source must not be the manifest itself")
    algorithm = existing.hash_metadata["algorithm"]
    policy_data = existing.hash_metadata["policy"]
    policy = CanonicalPolicy(**policy_data)
    privacy = PrivacyConfig.from_dict(existing.privacy_metadata["config"])
    rebuilt = build_manifest(
        source,
        id_field=existing.id_field,
        algorithm=algorithm,
        policy=policy,
        privacy=privacy,
        exclude_paths=(args.manifest,),
    )
    mismatches = _manifest_mismatches(existing, rebuilt)
    if not mismatches:
        print(f"verified {len(rebuilt.records)} records")
        return 0
    print(f"verification failed: {', '.join(mismatches)} differ", file=sys.stderr)
    return 1


def _manifest_mismatches(existing: Manifest, rebuilt: Manifest) -> list[str]:
    """Name every derived manifest section that no longer matches the source."""
    fields = (
        "format",
        "id_field",
        "hash_metadata",
        "privacy_metadata",
        "corpus_hash",
        "order_hash",
        "files",
        "records",
        "schema",
        "privacy_findings",
    )
    return [name for name in fields if getattr(existing, name) != getattr(rebuilt, name)]


def main() -> None:
    """Console-script entry point with concise expected-error handling."""
    try:
        raise SystemExit(run())
    except CorpusLedgerError as exc:
        print(f"corpusledger: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
