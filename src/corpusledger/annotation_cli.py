"""Strict file-oriented entry points for local typed annotation documents."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any

from . import annotations as annotation_model
from .annotations import AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation
from .errors import InputError
from .strictjson import bounded_int, finite_float, object_without_duplicates, reject_constant

MAX_DOCUMENT_BYTES = 128 * 1024 * 1024


def configure_annotation_parser(parser: argparse.ArgumentParser) -> None:
    """Register a deliberately closed set of local annotation commands."""
    actions = parser.add_subparsers(dest="annotation_action", required=True)
    create = actions.add_parser("create", help="wrap exact UTF-8 text as an empty annotation document")
    create.add_argument("input")
    create.add_argument("output")
    create.add_argument("--id", required=True, dest="document_id")
    validate = actions.add_parser("validate", help="validate all spans, feature types and references")
    validate.add_argument("input")
    validate.add_argument("--output")
    query = actions.add_parser("query", help="query typed spans at Unicode code-point offsets")
    query.add_argument("input")
    query.add_argument("start", type=int)
    query.add_argument("end", type=int)
    query.add_argument("--relation", choices=("exact", "inside", "covering", "overlapping"), default="overlapping")
    query.add_argument("--type", dest="type_name")
    query.add_argument("--output")
    tokenize = actions.add_parser("tokenize", help="append a deterministic Unicode word/punctuation layer")
    tokenize.add_argument("input")
    tokenize.add_argument("output")
    tokenize.add_argument("--type", default="token", dest="type_name")


def _read(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise InputError(f"annotation input exceeds {MAX_DOCUMENT_BYTES} bytes")
        # Binary decode deliberately preserves CRLF and combining marks.
        return raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise InputError(f"cannot read UTF-8 annotation input {path}") from exc


def _load(path: Path) -> AnnotationDocument:
    try:
        payload = json.loads(
            _read(path),
            object_pairs_hook=object_without_duplicates,
            parse_constant=reject_constant,
            parse_float=finite_float,
            parse_int=bounded_int,
        )
        return AnnotationDocument.from_dict(payload)
    except (ValueError, RecursionError) as exc:
        raise InputError(f"invalid annotation document: {exc}") from exc


def _write(payload: Any, destination: Path | None) -> None:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if len(text.encode("utf-8")) > MAX_DOCUMENT_BYTES:
        raise InputError(f"annotation output exceeds {MAX_DOCUMENT_BYTES} bytes")
    if destination is None:
        print(text, end="")
        return
    temporary: str | None = None
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
        os.replace(temporary, destination)
    except OSError as exc:
        raise InputError(f"cannot write annotation output {destination}") from exc
    finally:
        if temporary is not None:
            with suppress(OSError):
                os.unlink(temporary)


def run_annotation_command(args: argparse.Namespace) -> int:
    """Execute after protecting every output from aliasing the source file."""
    try:
        source = Path(args.input).resolve()
        destination = Path(args.output).resolve() if args.output is not None else None
        if destination is not None and (
            source == destination or (source.exists() and destination.exists() and source.samefile(destination))
        ):
            raise InputError("annotation output must not overwrite or alias input")
    except (OSError, RuntimeError) as exc:
        raise InputError("cannot resolve/check annotation input/output identity") from exc
    if args.annotation_action == "create":
        document = AnnotationDocument(args.document_id, _read(source))
        payload = document.to_dict()
    else:
        document = _load(source)
        if args.annotation_action == "validate":
            payload = {
                "format": "corpusledger.annotation-validation.v1",
                "id": document.document_id,
                "digest": document.digest,
                "text_sha256": document.text_sha256,
                "codepoints": len(document.text),
                "types": len(document.annotation_types),
                "annotations": len(document.annotations),
                "valid": True,
            }
        elif args.annotation_action == "query":
            matches = document.index(args.type_name).query(args.start, args.end, relation=args.relation)
            payload = {
                "format": "corpusledger.annotation-query.v1",
                "document_digest": document.digest,
                "relation": args.relation,
                "start": args.start,
                "end": args.end,
                "type": args.type_name,
                "annotations": [item.to_dict() for item in matches],
            }
        else:
            schema = AnnotationType(args.type_name, {"text": AnnotationField()})
            if schema.name in {item.name for item in document.annotation_types}:
                raise InputError(f"token layer {schema.name!r} already exists")
            tokens = []
            for index, match in enumerate(re.finditer(r"\w+|[^\w\s]", document.text)):
                if index >= annotation_model.MAX_ANNOTATIONS - len(document.annotations):
                    raise InputError(f"document exceeds {annotation_model.MAX_ANNOTATIONS} annotations")
                tokens.append(
                    SpanAnnotation(
                        f"{args.type_name}:{index}", args.type_name, match.start(), match.end(), {"text": match.group()}
                    )
                )
            result = AnnotationDocument(
                document.document_id,
                document.text,
                (*document.annotation_types, schema),
                (*document.annotations, *tokens),
            )
            payload = result.to_dict()
    _write(payload, destination)
    return 0
