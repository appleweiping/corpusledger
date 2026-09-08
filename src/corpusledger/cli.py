"""Command-line interface for CorpusLedger."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from .canonical import CanonicalPolicy
from .catalog import SnapshotCatalog, SnapshotRef
from .diff import compare
from .errors import (
    CorpusLedgerError,
    InputError,
    ManifestError,
    SignatureError,
    SignatureVerificationError,
)
from .external_sort import external_sort_jsonl
from .index import ManifestIndex
from .manifest import Manifest, build_manifest
from .pipeline import drop_fields, rename_field, run_pipeline, select_fields
from .plan import load_pipeline_plan
from .privacy import PrivacyConfig
from .readers import ReaderAdapter, iter_corpus, load_reader_adapter
from .reporting import render
from .schema import compare_json_schemas, to_json_schema, validate_json_schema
from .service import create_server
from .signing import SignatureEnvelope, sign_manifest, verify_manifest_signature
from .store import ObjectStore, bundle_snapshot, extract_bundle, verify_bundle
from .stream import NdjsonGateway


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
        "--sort-path",
        action="append",
        default=[],
        help="sort only the list at this dotted field path (repeatable)",
    )
    external_sort = subparsers.add_parser("sort-jsonl", help="externally sort JSONL records by canonical content")
    external_sort.add_argument("input")
    external_sort.add_argument("output")
    external_sort.add_argument("--chunk-size", type=int, default=10_000)
    external_sort.add_argument("--id-field", default="id")
    snapshot.add_argument(
        "--reader",
        help="explicit corpusledger.readers entry point (third-party code is loaded only when named)",
    )

    schema_export = subparsers.add_parser("schema", help="export a manifest's observed schema as JSON Schema")
    schema_export.add_argument("manifest")
    schema_export.add_argument("--title")
    schema_export.add_argument("--id", dest="schema_id")
    schema_export.add_argument("--output")
    schema_validate = subparsers.add_parser(
        "schema-validate", help="validate corpus records against a JSON Schema subset"
    )
    schema_validate.add_argument("input")
    schema_validate.add_argument("schema")
    schema_validate.add_argument("--id-field", default="id")
    schema_validate.add_argument("--max-errors", type=int, default=100)
    schema_validate.add_argument("--output")
    schema_compat = subparsers.add_parser(
        "schema-compat", help="check backward/forward compatibility of two JSON Schemas"
    )
    schema_compat.add_argument("before")
    schema_compat.add_argument("after")
    schema_compat.add_argument("--mode", choices=("backward", "forward", "full"), default="backward")
    schema_compat.add_argument("--output")

    difference = subparsers.add_parser("diff", help="compare two manifests")
    difference.add_argument("before")
    difference.add_argument("after")
    difference.add_argument("--format", choices=("json", "markdown"), default="markdown")
    difference.add_argument("--output")

    verify = subparsers.add_parser("verify", help="rebuild and verify a manifest")
    verify.add_argument("manifest")
    verify.add_argument("--input", help="override the source path recorded in the manifest")
    verify.add_argument("--reader", help="override or confirm the recorded reader entry point")
    index = subparsers.add_parser("index", help="build a queryable SQLite index for a manifest")
    index.add_argument("manifest")
    index.add_argument("output")
    verify_index = subparsers.add_parser("verify-index", help="verify an index against a manifest")
    verify_index.add_argument("manifest")
    verify_index.add_argument("index")
    query_index = subparsers.add_parser("query-index", help="query indexed manifest metadata")
    query_index.add_argument("index")
    query_index.add_argument("--id-prefix")
    query_index.add_argument("--source")
    query_index.add_argument("--field")
    query_index.add_argument("--field-hash")
    query_index.add_argument("--limit", type=int, default=100)
    query_index.add_argument("--output")
    duplicates = subparsers.add_parser("duplicate-fields", help="find repeated authenticated field values in an index")
    duplicates.add_argument("index")
    duplicates.add_argument("--field")
    duplicates.add_argument("--limit", type=int, default=100)
    duplicates.add_argument("--output")

    catalog = subparsers.add_parser("catalog", help="register and inspect named manifest snapshots")
    catalog_actions = catalog.add_subparsers(dest="catalog_action", required=True)
    register = catalog_actions.add_parser("register", help="append a manifest version")
    register.add_argument("database")
    register.add_argument("name")
    register.add_argument("manifest")
    register.add_argument("--parent")
    register.add_argument("--tag", action="append", default=[])
    catalog_list = catalog_actions.add_parser("list", help="list named snapshot versions")
    catalog_list.add_argument("database")
    catalog_list.add_argument("--name")
    catalog_list.add_argument("--output")
    lineage = catalog_actions.add_parser("lineage", help="follow a snapshot parent chain")
    lineage.add_argument("database")
    lineage.add_argument("corpus_hash")
    lineage.add_argument("--output")
    catalog_diff = catalog_actions.add_parser("diff", help="diff two versions in a snapshot series")
    catalog_diff.add_argument("database")
    catalog_diff.add_argument("name")
    catalog_diff.add_argument("before", type=int)
    catalog_diff.add_argument("after", type=int)
    catalog_diff.add_argument("--format", choices=("json", "markdown"), default="json")
    catalog_diff.add_argument("--output")

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
    bundle_verify = subparsers.add_parser("verify-bundle", help="authenticate a snapshot ZIP")
    bundle_verify.add_argument("bundle")
    bundle_verify.add_argument("--digest")
    bundle_extract = subparsers.add_parser("extract-bundle", help="verify and safely extract a snapshot ZIP")
    bundle_extract.add_argument("bundle")
    bundle_extract.add_argument("destination")
    bundle_extract.add_argument("--overwrite", action="store_true")
    gc = subparsers.add_parser("gc", help="plan or collect unreferenced object-store bytes")
    gc.add_argument("store")
    gc.add_argument("--keep", action="append", default=[])
    gc.add_argument("--delete", action="store_true", help="remove unreferenced objects")
    serve = subparsers.add_parser("serve", help="serve the local HTTP/JSON dispatch API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    stream = subparsers.add_parser("stream", help="process gateway requests from stdin as NDJSON")
    stream.add_argument(
        "--max-line-bytes",
        type=int,
        default=1_048_576,
        help="reject request lines larger than this UTF-8 byte limit",
    )
    stream.add_argument(
        "--field",
        action="append",
        default=[],
        help="field exposed by the built-in select processor (repeat for multiple fields)",
    )
    stream.add_argument(
        "--report-output",
        help="write a JSON digest/count report after responses have been emitted",
    )
    stream.add_argument(
        "--strict",
        action="store_true",
        help="return status 2 when any input line produces an error response",
    )
    pipeline = subparsers.add_parser("pipeline", help="run a resumable deterministic record pipeline")
    pipeline.add_argument("input")
    pipeline.add_argument("output")
    pipeline.add_argument("--id-field", default="id")
    pipeline.add_argument("--select", action="append", default=[], help="comma-separated fields to keep")
    pipeline.add_argument("--drop", action="append", default=[], help="comma-separated fields to drop")
    pipeline.add_argument("--rename", action="append", default=[], help="rename OLD=NEW (repeatable)")
    pipeline.add_argument("--plan", help="strict versioned JSON plan for select/drop/rename steps")
    pipeline.add_argument("--state", help="checkpoint path (default: OUTPUT.state.json)")
    pipeline.add_argument("--resume", action="store_true", help="reuse a matching completed checkpoint")
    return parser


def _write_report(text: str, destination: str | None) -> None:
    if destination:
        try:
            Path(destination).write_text(text, encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise InputError(f"cannot write report {destination}: {exc}") from exc
    else:
        sys.stdout.write(text)


def _snapshot_ref_payload(ref: SnapshotRef) -> dict[str, object]:
    return {
        "name": ref.name,
        "version": ref.version,
        "corpus_hash": ref.corpus_hash,
        "parent": ref.parent,
        "tags": list(ref.tags),
    }


def _catalog_command(args: argparse.Namespace) -> int:
    with SnapshotCatalog(args.database) as catalog:
        if args.catalog_action == "register":
            manifest = Manifest.load(args.manifest)
            ref = catalog.register(
                args.name,
                manifest,
                parent=args.parent,
                tags=tuple(args.tag),
            )
            payload: object = _snapshot_ref_payload(ref)
        elif args.catalog_action == "list":
            payload = [_snapshot_ref_payload(item) for item in catalog.list(args.name)]
        elif args.catalog_action == "lineage":
            payload = [_snapshot_ref_payload(item) for item in catalog.lineage(args.corpus_hash)]
        else:
            difference = catalog.diff(args.name, args.before, args.after)
            _write_report(render(difference, args.format), args.output)
            return 0
    destination = getattr(args, "output", None)
    _write_report(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", destination)
    return 0


def run(argv: list[str] | None = None) -> int:
    """Execute the CLI and return a process status."""
    args = _parser().parse_args(argv)
    if args.command == "catalog":
        return _catalog_command(args)
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
        policy = CanonicalPolicy(
            list_strategy="sort" if args.sort_lists else "preserve",
            sort_paths=tuple(args.sort_path),
        )
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
    if args.command == "sort-jsonl":
        input_path = Path(args.input).resolve()
        output_path = Path(args.output).resolve()
        _require_distinct(output_path, input_path, message="sort output must differ from input")
        sort_report = external_sort_jsonl(
            input_path,
            output_path,
            chunk_size=args.chunk_size,
            id_field=args.id_field,
        )
        print(json.dumps(sort_report.to_dict(), sort_keys=True))
        return 0
    if args.command == "schema":
        manifest_path = Path(args.manifest).resolve()
        schema_output = Path(args.output).resolve() if args.output else None
        if schema_output is not None:
            _require_distinct(
                schema_output,
                manifest_path,
                message="schema output must not overwrite the manifest",
            )
        manifest = Manifest.load(manifest_path)
        payload = to_json_schema(manifest.schema, title=args.title, schema_id=args.schema_id)
        _write_report(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", args.output)
        return 0
    if args.command == "schema-validate":
        input_path = Path(args.input).resolve()
        schema_path = Path(args.schema).resolve()
        if not schema_path.is_file():
            raise InputError(f"schema does not exist: {schema_path}")
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise InputError(f"cannot read schema {schema_path}: {exc}") from exc
        records_checked = 0

        def values() -> Iterator[dict[str, Any]]:
            nonlocal records_checked
            for record in iter_corpus(input_path, id_field=args.id_field):
                records_checked += 1
                yield record.data

        issues = validate_json_schema(values(), schema, max_errors=args.max_errors)
        payload = {
            "valid": not issues,
            "records_checked": records_checked,
            "errors": [item.to_dict() for item in issues],
        }
        rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        _write_report(rendered, args.output)
        return 0 if not issues else 2
    if args.command == "schema-compat":
        before_path = Path(args.before).resolve()
        after_path = Path(args.after).resolve()
        try:
            before = json.loads(before_path.read_text(encoding="utf-8"))
            after = json.loads(after_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise InputError(f"cannot read schema pair: {exc}") from exc
        compat_report = compare_json_schemas(before, after, mode=args.mode)
        rendered = json.dumps(compat_report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        _write_report(rendered, args.output)
        return 0 if compat_report.compatible else 2
    if args.command == "index":
        manifest_path = Path(args.manifest).resolve()
        output_path = Path(args.output).resolve()
        _require_distinct(output_path, manifest_path, message="index output must differ from its manifest")
        manifest = Manifest.load(manifest_path)
        with ManifestIndex.build(manifest, output_path) as manifest_index:
            print(json.dumps(manifest_index.stats(), sort_keys=True))
        return 0
    if args.command == "verify-index":
        manifest = Manifest.load(args.manifest)
        with ManifestIndex(args.index) as manifest_index:
            manifest_index.verify(manifest)
            print(json.dumps(manifest_index.stats(), sort_keys=True))
        return 0
    if args.command == "query-index":
        with ManifestIndex(args.index) as manifest_index:
            rows = manifest_index.query(
                id_prefix=args.id_prefix,
                source=args.source,
                field_path=args.field,
                field_hash=args.field_hash,
                limit=args.limit,
            )
            payload = {"index": manifest_index.stats(), "records": [row.to_dict() for row in rows]}
        _write_report(json.dumps(payload, indent=2, sort_keys=True) + "\n", args.output)
        return 0
    if args.command == "duplicate-fields":
        with ManifestIndex(args.index) as manifest_index:
            groups = manifest_index.duplicate_fields(field_path=args.field, limit=args.limit)
            payload = {
                "index": manifest_index.stats(),
                "groups": [group.to_dict() for group in groups],
            }
        _write_report(json.dumps(payload, indent=2, sort_keys=True) + "\n", args.output)
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
    if args.command == "verify-bundle":
        verification = verify_bundle(args.bundle, expected_archive_digest=args.digest)
        print(
            json.dumps(
                {
                    "archive_digest": verification.archive_digest,
                    "manifest_digest": verification.manifest_digest,
                    "files": list(verification.files),
                    "bytes": verification.bytes,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "extract-bundle":
        verification = extract_bundle(args.bundle, args.destination, overwrite=args.overwrite)
        print(
            json.dumps(
                {
                    "archive_digest": verification.archive_digest,
                    "manifest_digest": verification.manifest_digest,
                    "files": list(verification.files),
                    "bytes": verification.bytes,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "gc":
        gc_report = ObjectStore(args.store).collect_unreferenced(tuple(args.keep), dry_run=not args.delete)
        print(
            json.dumps(
                {
                    "kept": list(gc_report.kept),
                    "removed": list(gc_report.removed),
                    "bytes": gc_report.bytes,
                    "dry_run": not args.delete,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "serve":
        server = create_server(host=args.host, port=args.port)
        print(f"serving CorpusLedger on http://{args.host}:{server.server_port}/v1/dispatch")
        try:
            server.serve_forever()
        finally:
            server.server_close()
        return 0
    if args.command == "stream":
        return _run_stream(args)
    if args.command == "pipeline":
        if args.plan and (args.select or args.drop or args.rename):
            raise InputError("--plan cannot be combined with --select, --drop, or --rename")
        steps = list(load_pipeline_plan(args.plan).compile()) if args.plan else []
        for value in args.select:
            steps.append(select_fields(tuple(part.strip() for part in value.split(","))))
        for value in args.rename:
            if value.count("=") != 1:
                raise InputError("--rename expects OLD=NEW")
            old, new = (part.strip() for part in value.split("=", 1))
            steps.append(rename_field(old, new))
        for value in args.drop:
            steps.append(drop_fields(tuple(part.strip() for part in value.split(","))))
        output_path = Path(args.output).resolve()
        _require_distinct(output_path, Path(args.input).resolve(), message="pipeline output must differ from its input")
        if args.state:
            _require_distinct(
                Path(args.state).resolve(),
                Path(args.input).resolve(),
                output_path,
                message="pipeline state must differ from input and output",
            )
        pipeline_report = run_pipeline(
            args.input,
            args.output,
            steps,
            id_field=args.id_field,
            state=args.state,
            resume=args.resume,
        )
        print(json.dumps(pipeline_report.to_dict(), sort_keys=True))
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
    policy = CanonicalPolicy.from_dict(policy_data)
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


def _run_stream(args: argparse.Namespace) -> int:
    """Run the language-neutral gateway over stdin/stdout.

    The command deliberately exposes only deterministic, dependency-free
    processors. Applications that need domain-specific processing can use the
    :class:`NdjsonGateway` API and register their own processors.
    """

    if args.max_line_bytes < 1:
        raise InputError("--max-line-bytes must be positive")

    gateway = NdjsonGateway(max_line_bytes=args.max_line_bytes)

    def identity(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return payload

    selected_fields = tuple(args.field)

    def select(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return {field: payload[field] for field in selected_fields if field in payload}

    gateway.register("identity", identity)
    gateway.register("select", select)
    input_stream = getattr(sys.stdin, "buffer", sys.stdin)
    responses, report = gateway.process_lines(input_stream)
    for response in responses:
        sys.stdout.write(response)
    summary = {
        "input_digest": report.input_digest,
        "output_digest": report.output_digest,
        "records": report.records,
        "successes": report.successes,
        "failures": report.failures,
    }
    if args.report_output:
        _write_report(json.dumps(summary, sort_keys=True) + "\n", args.report_output)
    else:
        sys.stderr.write(json.dumps(summary, sort_keys=True) + "\n")
    return 2 if args.strict and report.failures else 0


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
