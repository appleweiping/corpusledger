"""Command-line interface for CorpusLedger."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .canonical import CanonicalPolicy
from .diff import compare
from .errors import (
    CorpusLedgerError,
    InputError,
    ManifestError,
    SignatureError,
    SignatureVerificationError,
)
from .manifest import Manifest, build_manifest
from .privacy import PrivacyConfig
from .readers import ReaderAdapter, load_reader_adapter
from .reporting import render
from .signing import SignatureEnvelope, sign_manifest, verify_manifest_signature
from .store import ObjectStore, bundle_snapshot


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corpusledger",
        description="Reproducible manifests and diffs for JSON NLP corpora",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 0.2.0")
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot = subparsers.add_parser("snapshot", help="create a corpus manifest")
    snapshot.add_argument("input")
    snapshot.add_argument("output")
    snapshot.add_argument("--id-field", default="id")
    snapshot.add_argument("--algorithm", choices=("sha256", "blake2b"), default="sha256")
    snapshot.add_argument("--sort-lists", action="store_true", help="treat lists as set-like (use with care)")
    snapshot.add_argument(
        "--reader",
        help="explicit corpusledger.readers entry point (third-party code is loaded only when named)",
    )

    difference = subparsers.add_parser("diff", help="compare two manifests")
    difference.add_argument("before")
    difference.add_argument("after")
    difference.add_argument("--format", choices=("json", "markdown"), default="markdown")
    difference.add_argument("--output")

    verify = subparsers.add_parser("verify", help="rebuild and verify a manifest")
    verify.add_argument("manifest")
    verify.add_argument("--input", help="override the source path recorded in the manifest")
    verify.add_argument("--reader", help="override or confirm the recorded reader entry point")

    sign = subparsers.add_parser("sign", help="create a detached Ed25519 signature")
    sign.add_argument("manifest")
    sign.add_argument("--private-key", required=True, help="Ed25519 PEM private key path")
    sign.add_argument("--output", help="signature path (default: MANIFEST.sig)")
    sign.add_argument(
        "--password-env",
        help="environment variable containing the PEM password; the value is never printed",
    )

    verify_signature = subparsers.add_parser(
        "verify-signature",
        help="authenticate exact manifest bytes against a trusted Ed25519 public key",
    )
    verify_signature.add_argument("manifest")
    verify_signature.add_argument("signature")
    verify_signature.add_argument("--public-key", required=True, help="trusted Ed25519 PEM public key path")
    bundle = subparsers.add_parser("bundle", help="create a deterministic source snapshot ZIP")
    bundle.add_argument("manifest")
    bundle.add_argument("output")
    bundle.add_argument("--input", help="override the source path recorded in the manifest")
    bundle.add_argument("--store", help="optional SHA-256 object-store directory")
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
        _require_distinct(
            output_path,
            input_path,
            message="snapshot output must not overwrite the input corpus file",
        )
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
            reader=_reader(args.reader),
        )
        manifest.save(args.output)
        print(f"wrote {len(manifest.records)} records to {args.output}")
        return 0
    if args.command == "bundle":
        manifest_path = Path(args.manifest).resolve()
        output_path = Path(args.output).resolve()
        source_path = Path(args.input or Manifest.load(manifest_path).source).resolve()
        _require_distinct(output_path, manifest_path, message="bundle output must differ from its manifest")
        manifest = Manifest.load(manifest_path)
        store = ObjectStore(args.store) if args.store else None
        report = bundle_snapshot(manifest, source_path, output_path, store=store)
        print(
            json.dumps(
                {
                    "archive_digest": report.archive_digest,
                    "manifest_digest": report.manifest_digest,
                    "files": list(report.files),
                    "bytes": report.bytes,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "diff":
        if args.output:
            output_path = Path(args.output).resolve()
            _require_distinct(
                output_path,
                Path(args.before).resolve(),
                Path(args.after).resolve(),
                message="diff output must not overwrite an input manifest",
            )
        result = compare(Manifest.load(args.before), Manifest.load(args.after))
        _write_report(render(result, args.format), args.output)
        return 1 if result.has_changes else 0
    if args.command == "sign":
        manifest_path = Path(args.manifest).resolve()
        private_key_path = Path(args.private_key).resolve()
        output_path = Path(args.output or f"{args.manifest}.sig").resolve()
        _require_distinct(
            output_path,
            manifest_path,
            private_key_path,
            message="signature output must not overwrite the manifest or private key",
        )
        Manifest.load(manifest_path)
        if output_path.exists():
            try:
                SignatureEnvelope.load(output_path)
            except SignatureError as exc:
                raise InputError(
                    "signature output already exists and is not a CorpusLedger signature envelope"
                ) from exc
        password = _password_from_environment(args.password_env)
        envelope = sign_manifest(manifest_path, private_key_path, password=password)
        envelope.save(output_path)
        print(f"signed manifest with key {envelope.key_id}; wrote {output_path}")
        return 0
    if args.command == "verify-signature":
        manifest_path = Path(args.manifest).resolve()
        signature_path = Path(args.signature).resolve()
        public_key_path = Path(args.public_key).resolve()
        Manifest.load(manifest_path)
        try:
            envelope = verify_manifest_signature(manifest_path, signature_path, public_key_path)
        except SignatureVerificationError as exc:
            print(f"signature verification failed: {exc}", file=sys.stderr)
            return 1
        print(f"verified Ed25519 signature from key {envelope.key_id}")
        return 0
    existing = Manifest.load(args.manifest)
    source = args.input or existing.source
    _require_distinct(
        Path(source).resolve(),
        Path(args.manifest).resolve(),
        message="verification source must not be the manifest itself",
    )
    algorithm = existing.hash_metadata["algorithm"]
    policy_data = existing.hash_metadata["policy"]
    policy = CanonicalPolicy(**policy_data)
    privacy = PrivacyConfig.from_dict(existing.privacy_metadata["config"])
    reader = _verification_reader(existing, args.reader)
    rebuilt = build_manifest(
        source,
        id_field=existing.id_field,
        algorithm=algorithm,
        policy=policy,
        privacy=privacy,
        exclude_paths=(args.manifest,),
        reader=reader,
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
        "reader_metadata",
    )
    return [name for name in fields if getattr(existing, name) != getattr(rebuilt, name)]


def _reader(name: str | None) -> ReaderAdapter | None:
    return load_reader_adapter(name) if name is not None else None


def _verification_reader(manifest: Manifest, requested: str | None) -> ReaderAdapter | None:
    recorded = manifest.reader_metadata
    if recorded is None:
        if requested is not None:
            raise InputError("manifest does not record a third-party reader adapter")
        return None
    if requested is not None and requested != recorded["name"]:
        raise InputError(f"manifest requires reader {recorded['name']!r}, not requested reader {requested!r}")
    reader = load_reader_adapter(recorded["name"])
    if reader.version != recorded["version"]:
        raise InputError(
            f"manifest requires reader {reader.name!r} version {recorded['version']!r}; "
            f"installed version is {reader.version!r}"
        )
    return reader


def _require_distinct(destination: Path, *protected: Path, message: str) -> None:
    for existing in protected:
        if destination == existing:
            raise InputError(message)
        try:
            aliases_existing_file = destination.exists() and existing.exists() and destination.samefile(existing)
        except OSError:
            aliases_existing_file = False
        if aliases_existing_file:
            raise InputError(message)


def _password_from_environment(name: str | None) -> bytes | None:
    if name is None:
        return None
    if (
        not name
        or name != name.strip()
        or "=" in name
        or any(ord(character) < 32 or ord(character) == 127 or 0xD800 <= ord(character) <= 0xDFFF for character in name)
    ):
        raise InputError("password environment variable name is invalid")
    value = os.environ.get(name)
    if value is None:
        raise InputError(f"password environment variable {name!r} is not set")
    try:
        return value.encode("utf-8")
    except UnicodeError as exc:
        raise InputError(f"password environment variable {name!r} is not valid UTF-8 text") from exc


def main() -> None:
    """Console-script entry point with concise expected-error handling."""
    try:
        raise SystemExit(run())
    except CorpusLedgerError as exc:
        print(f"corpusledger: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
