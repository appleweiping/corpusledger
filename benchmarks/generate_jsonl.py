"""Generate a deterministic synthetic JSONL corpus without retaining records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def generate_jsonl(path: Path, records: int, payload_bytes: int) -> int:
    """Write ``records`` deterministic rows and return the resulting byte size."""

    if records < 0:
        raise ValueError("records must be non-negative")
    if payload_bytes < 0:
        raise ValueError("payload-bytes must be non-negative")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for index in range(records):
            prefix = f"sample-{index:09d}-"
            text = (prefix + "x" * payload_bytes)[:payload_bytes]
            row = {
                "group": f"source-{index // 5:09d}",
                "id": f"record-{index:09d}",
                "label": ("negative", "neutral", "positive")[index % 3],
                "metadata": {"batch": index // 1000, "reviewed": index % 7 == 0},
                "text": text,
            }
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
            stream.write("\n")
    return path.stat().st_size


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    args = parser.parse_args()
    size = generate_jsonl(args.output, args.records, args.payload_bytes)
    print(f"wrote {args.records} records ({size} bytes) to {args.output}")


if __name__ == "__main__":
    main()
