"""corpusledger: see which records and fields changed between two versions of a dataset."""

from .canonical import CanonicalPolicy, canonical_json, canonicalize
from .diff import Change, DatasetDiff, compare
from .readers import Row, iter_rows
from .snapshot import Entry, Match, Snapshot, build_snapshot

__version__ = "0.3.0"

__all__ = [
    "CanonicalPolicy",
    "Change",
    "DatasetDiff",
    "Entry",
    "Match",
    "Row",
    "Snapshot",
    "__version__",
    "build_snapshot",
    "canonical_json",
    "canonicalize",
    "compare",
    "iter_rows",
]
