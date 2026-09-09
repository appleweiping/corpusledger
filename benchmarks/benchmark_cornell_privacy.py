"""Measure privacy scanning on the official Cornell movie-dialogue archive.

The archive is user-supplied and digest-pinned. No dataset is downloaded or
redistributed by this script. Output contains provenance and aggregate counters,
not movie dialogue or privacy findings.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import platform
import sys
import tempfile
import time
import tracemalloc
import zipfile
from collections import Counter
from pathlib import Path

import corpusledger
from corpusledger import PrivacyConfig, __version__, scan_corpus

SOURCE_URL = "https://www.cs.cornell.edu/~cristian/data/cornell_movie_dialogs_corpus.zip"
SOURCE_SHA256 = "3bde8a571f615201bc2d2453e22878090719638592f774720eddec739de8c900"
MEMBER = "cornell movie-dialogs corpus/movie_lines.txt"
EXPECTED_RECORDS = 304_713


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def run(archive: Path, *, limit: int | None = None) -> dict[str, object]:
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise ValueError("limit must be a positive integer or omitted")
    if digest(archive) != SOURCE_SHA256:
        raise ValueError("archive does not match the recorded Cornell release SHA-256")
    with tempfile.TemporaryDirectory(prefix="corpusledger-cornell-") as directory:
        corpus = Path(directory) / "movie-lines.jsonl"
        with zipfile.ZipFile(archive) as source:
            readme = source.read("cornell movie-dialogs corpus/README.txt")
            raw_digest = hashlib.sha256()
            with source.open(MEMBER) as raw:
                for block in iter(lambda: raw.read(1024 * 1024), b""):
                    raw_digest.update(block)
            count = 0
            with (
                source.open(MEMBER) as raw,
                io.TextIOWrapper(raw, encoding="latin-1", newline="") as lines,
                corpus.open("w", encoding="utf-8", newline="\n") as output,
            ):
                for position, line in enumerate(lines, start=1):
                    if limit is not None and count == limit:
                        break
                    fields = line.rstrip("\r\n").split(" +++$+++ ", 4)
                    if len(fields) != 5:
                        raise ValueError(f"movie_lines.txt line {position}: expected five fields")
                    identifier, speaker, movie, _character_name, text = fields
                    output.write(
                        json.dumps(
                            {"id": identifier, "speaker": speaker, "movie": movie, "text": text},
                            sort_keys=True,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    count += 1
        if count != min(limit or EXPECTED_RECORDS, EXPECTED_RECORDS):
            raise ValueError(f"unexpected record count: {count}")
        config = PrivacyConfig.from_pack("pii")
        tracemalloc.start()
        started = time.perf_counter()
        try:
            report = scan_corpus(corpus, config=config)
            elapsed = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        if report.records_checked != count:
            raise AssertionError("scanner count differs from independent archive conversion count")
        return {
            "kind": "external-real-data-engineering",
            "dataset": "Cornell Movie-Dialogs Corpus",
            "dataset_url": SOURCE_URL,
            "archive_sha256": SOURCE_SHA256,
            "archive_bytes": archive.stat().st_size,
            "member": MEMBER,
            "member_sha256": raw_digest.hexdigest(),
            "readme_sha256": hashlib.sha256(readme).hexdigest(),
            "license_status": (
                "No explicit license grant found in the downloaded archive README; raw data not redistributed."
            ),
            "citation": "Danescu-Niculescu-Mizil and Lee (2011), Chameleons in Imagined Conversations, CMCL/ACL.",
            "selection": "all archive records" if limit is None else f"first {limit} archive records",
            "seed": None,
            "conversion": (
                "v1: Latin-1 text, separator split at most four times, id/speaker/movie/text fields, UTF-8 JSONL"
            ),
            "corpus_sha256": digest(corpus),
            "corpus_bytes": corpus.stat().st_size,
            "records": count,
            "records_checked": report.records_checked,
            "config": config.to_dict(),
            "finding_count": len(report.findings),
            "findings_by_kind": dict(sorted(Counter(str(item["kind"]) for item in report.findings).items())),
            "elapsed_seconds": elapsed,
            "peak_python_bytes": peak,
            "python": sys.version,
            "platform": platform.platform(),
            "corpusledger_version": __version__,
            "runtime_sources": {
                path.name: digest(path) for path in sorted(Path(corpusledger.__file__).parent.glob("*.py"))
            },
            "benchmark_sha256": digest(Path(__file__)),
            "claim": (
                "Parsing/scanning throughput and complete record accounting; no labelled privacy accuracy evaluation."
            ),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.resolve() == args.archive.resolve() or (args.output.exists() and args.output.samefile(args.archive)):
        parser.error("output must not overwrite the source archive")
    payload = run(args.archive, limit=args.limit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
