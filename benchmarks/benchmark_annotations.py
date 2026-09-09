"""Check typed annotation spans on real text and a 100,000-span synthetic index.

The Cornell archive is supplied locally and hash-pinned. Only counters, hashes,
timings and environment details are emitted; no dialogue or token vocabulary is
redistributed. Regex-generated spans are engineering fixtures, not gold labels.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import platform
import random
import re
import sys
import time
import tracemalloc
import zipfile
from pathlib import Path

import corpusledger
from corpusledger import (
    AnnotationDocument,
    AnnotationField,
    AnnotationIndex,
    AnnotationType,
    SpanAnnotation,
    codepoint_to_utf16,
    utf16_to_codepoint,
)

SOURCE_URL = "https://www.cs.cornell.edu/~cristian/data/cornell_movie_dialogs_corpus.zip"
SOURCE_SHA256 = "3bde8a571f615201bc2d2453e22878090719638592f774720eddec739de8c900"
MEMBER = "cornell movie-dialogs corpus/movie_lines.txt"
SYNTHETIC_SPANS = 100_000
REAL_QUERIES = 200
SYNTHETIC_QUERIES = 16
LEXICAL_PATTERN = r"\w+|[^\w\s]"


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def runtime_sources() -> dict[str, str]:
    return {path.name: digest(path) for path in sorted(Path(corpusledger.__file__).parent.glob("*.py"))}


def real_texts(archive: Path, limit: int) -> tuple[list[tuple[str, str]], dict[str, object]]:
    if digest(archive) != SOURCE_SHA256:
        raise ValueError("archive does not match the pinned Cornell SHA-256")
    selected_digest = hashlib.sha256()
    records: list[tuple[str, str]] = []
    with zipfile.ZipFile(archive) as source:
        readme = source.read("cornell movie-dialogs corpus/README.txt")
        member_digest = hashlib.sha256()
        with source.open(MEMBER) as raw:
            for block in iter(lambda: raw.read(1024 * 1024), b""):
                member_digest.update(block)
        with source.open(MEMBER) as raw, io.TextIOWrapper(raw, encoding="latin-1", newline="") as lines:
            for position, line in enumerate(lines, 1):
                fields = line.rstrip("\r\n").split(" +++$+++ ", 4)
                if len(fields) != 5:
                    raise ValueError(f"expected five fields on movie line {position}")
                identifier, _speaker, _movie, _character, text = fields
                records.append((identifier, text))
                selected_digest.update(line.encode("latin-1"))
                if len(records) == limit:
                    break
    if len(records) != limit or len({identifier for identifier, _ in records}) != limit:
        raise ValueError("the selected source record count or ID uniqueness check failed")
    return records, {
        "dataset": "Cornell Movie-Dialogs Corpus",
        "dataset_url": SOURCE_URL,
        "archive_sha256": SOURCE_SHA256,
        "archive_bytes": archive.stat().st_size,
        "member": MEMBER,
        "member_sha256": member_digest.hexdigest(),
        "readme_sha256": hashlib.sha256(readme).hexdigest(),
        "selected_raw_lines_sha256": selected_digest.hexdigest(),
        "selection": f"first {limit} movie_lines.txt records in archive order",
        "decoding": "Latin-1, preserve line endings while hashing; split at most four delimiters",
        "license_status": "No explicit license grant found in archive README; raw data not redistributed.",
        "citation": "Danescu-Niculescu-Mizil and Lee (2011), Chameleons in Imagined Conversations, CMCL/ACL.",
    }


def verify_queries(
    documents: list[AnnotationDocument],
    queries: list[tuple[int, int, int]],
    actual: list[tuple[SpanAnnotation, ...]],
) -> int:
    """Exhaustively evaluate the interval predicate without using index internals."""
    candidates_checked = 0
    for (document_index, start, end), found in zip(queries, actual, strict=True):
        document = documents[document_index]
        expected = [
            annotation
            for annotation in document.annotations
            if start < end
            and annotation.start < annotation.end
            and max(start, annotation.start) < min(end, annotation.end)
        ]
        expected.sort(key=lambda annotation: (annotation.start, annotation.end, annotation.annotation_id))
        if found != tuple(expected):
            raise AssertionError("overlap index differs from exhaustive half-open interval arithmetic")
        candidates_checked += len(document.annotations)
    return candidates_checked


def sampled_queries(documents: list[AnnotationDocument], count: int, seed: int) -> list[tuple[int, int, int]]:
    # Reproducible benchmark sampling does not use security-sensitive randomness.
    generator = random.Random(seed)  # nosec B311
    queries = []
    for position in range(count):
        document_index = generator.randrange(len(documents))
        length = len(documents[document_index].text)
        if position % 10 == 0:
            start, end = 0, length
        elif position % 10 == 1:
            start = end = length
        elif position % 10 == 2:
            start = end = 0
        else:
            start, end = sorted((generator.randrange(length + 1), generator.randrange(length + 1)))
        queries.append((document_index, start, end))
    return queries


def run_real(records: list[tuple[str, str]], seed: int) -> dict[str, object]:
    schema = AnnotationType("token", {"surface": AnnotationField("string")})
    tracemalloc.start()
    started = time.perf_counter()
    try:
        documents = [
            AnnotationDocument(
                identifier,
                text,
                (schema,),
                tuple(
                    SpanAnnotation(f"token:{position}", "token", match.start(), match.end(), {"surface": match.group()})
                    for position, match in enumerate(re.finditer(LEXICAL_PATTERN, text))
                ),
            )
            for identifier, text in records
        ]
        indexes = [document.index("token") for document in documents]
        build_seconds = time.perf_counter() - started
        queries = sampled_queries(documents, REAL_QUERIES, seed)
        started_queries = time.perf_counter()
        actual = [indexes[identifier].query(start, end) for identifier, start, end in queries]
        query_seconds = time.perf_counter() - started_queries
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    checked_boundaries = 0
    documents_digest = hashlib.sha256()
    for document in documents:
        documents_digest.update(bytes.fromhex(document.digest))
        for annotation in document.annotations:
            if document.span_text(annotation.annotation_id) != annotation.features["surface"]:
                raise AssertionError("annotation Unicode offsets no longer select the regex match")
            for boundary in (annotation.start, annotation.end):
                units = len(document.text[:boundary].encode("utf-16-le")) // 2
                if (
                    codepoint_to_utf16(document.text, boundary) != units
                    or utf16_to_codepoint(document.text, units) != boundary
                ):
                    raise AssertionError("Unicode offset conversion differs from UTF-16 encoding")
                checked_boundaries += 1
    candidates_checked = verify_queries(documents, queries, actual)
    return {
        "kind": "rule-generated-annotations-on-external-real-text",
        "records": len(documents),
        "text_codepoints": sum(len(document.text) for document in documents),
        "annotations": sum(len(document.annotations) for document in documents),
        "token_rule": LEXICAL_PATTERN,
        "span_substrings_verified": True,
        "utf16_boundaries_verified": checked_boundaries,
        "queries": len(queries),
        "query_matches": sum(len(matches) for matches in actual),
        "oracle_candidates_checked": candidates_checked,
        "overlap_oracle_verified": True,
        "documents_digest_sha256": documents_digest.hexdigest(),
        "build_seconds": build_seconds,
        "index_query_seconds": query_seconds,
        "peak_python_bytes": peak,
        "gold_annotation_accuracy_evaluated": False,
    }


def run_synthetic(seed: int) -> dict[str, object]:
    # Include supplementary-plane characters and a combining mark. Offsets
    # remain code points, so UTF-8 bytes, UTF-16 units and graphemes differ.
    text = "A😀e\u0301 " * 40_000
    # The recorded seed makes these synthetic intervals reproducible.
    generator = random.Random(seed)  # nosec B311
    intervals = []
    for position in range(SYNTHETIC_SPANS):
        start = generator.randrange(len(text) + 1)
        width = 0 if position % 10 == 0 else generator.randrange(1, 5001 if position % 5 == 0 else 13)
        intervals.append((start, min(len(text), start + width)))
    schema = AnnotationType("span")
    tracemalloc.start()
    started = time.perf_counter()
    try:
        document = AnnotationDocument(
            "synthetic-unicode",
            text,
            (schema,),
            tuple(
                SpanAnnotation(f"span:{position}", "span", start, end)
                for position, (start, end) in enumerate(intervals)
            ),
        )
        index: AnnotationIndex = document.index()
        build_seconds = time.perf_counter() - started
        queries = sampled_queries([document], SYNTHETIC_QUERIES, seed + 1)
        started_queries = time.perf_counter()
        actual = [index.query(start, end) for _, start, end in queries]
        query_seconds = time.perf_counter() - started_queries
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    candidates_checked = verify_queries([document], queries, actual)
    # Independently check code-point boundaries at a supplementary character,
    # a combining mark, and the final code-point boundary.
    boundaries = (0, 1, 2, 3, 4, 5, len(text) - 1, len(text))
    for boundary in boundaries:
        units = len(text[:boundary].encode("utf-16-le")) // 2
        if codepoint_to_utf16(text, boundary) != units or utf16_to_codepoint(text, units) != boundary:
            raise AssertionError("synthetic Unicode offset conversion mismatch")
    return {
        "kind": "synthetic-overlapping-and-empty-spans",
        "annotations": len(document.annotations),
        "text_codepoints": len(text),
        "text_sha256": document.text_sha256,
        "empty_spans": sum(annotation.start == annotation.end for annotation in document.annotations),
        "generator": "v1: 100k random starts; each tenth is an anchor; each other fifth spans <=5000; others <=12",
        "queries": len(queries),
        "query_matches": sum(len(matches) for matches in actual),
        "oracle_candidates_checked": candidates_checked,
        "overlap_oracle_verified": True,
        "unicode_boundaries_verified": len(boundaries),
        "document_digest": document.digest,
        "build_seconds": build_seconds,
        "index_query_seconds": query_seconds,
        "peak_python_bytes": peak,
    }


def run(archive: Path, *, limit: int = 1000, seed: int = 20260909) -> dict[str, object]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("real-text record limit must be an integer in [1, 1000]")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    sources_before = runtime_sources()
    records, provenance = real_texts(archive, limit)
    real = run_real(records, seed)
    synthetic = run_synthetic(seed)
    if runtime_sources() != sources_before:
        raise RuntimeError("runtime source files changed while benchmarking; rerun on a stable tree")
    return {
        "format": "corpusledger.annotation-benchmark.v1",
        "source": provenance,
        "real_text": real,
        "synthetic": synthetic,
        "seed": seed,
        "python": sys.version,
        "platform": platform.platform(),
        "corpusledger_version": corpusledger.__version__,
        "runtime_sources": sources_before,
        "benchmark_sha256": digest(Path(__file__)),
        "claim": "Span/offset/index engineering checks; rule-generated annotations have no gold linguistic labels.",
        "timing_scope": (
            "Annotation construction, validation, indexes and indexed queries; "
            "excludes parsing/generation/oracles/digests."
        ),
        "memory_scope": (
            "Traced Python allocations during construction/indexing/queries; "
            "excludes already loaded text and generated intervals."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve() == args.archive.resolve() or (args.output.exists() and args.output.samefile(args.archive)):
        parser.error("output must not overwrite the source archive")
    payload = run(args.archive, limit=args.limit, seed=args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
