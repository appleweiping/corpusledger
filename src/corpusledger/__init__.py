"""CorpusLedger: reproducible manifests and diffs for JSON NLP corpora."""

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes, canonical_json, canonicalize
from .diff import CorpusDiff, compare
from .manifest import MANIFEST_FORMAT, Manifest, build_manifest
from .privacy import PrivacyConfig

__all__ = [
    "CANONICAL_VERSION",
    "MANIFEST_FORMAT",
    "CanonicalPolicy",
    "CorpusDiff",
    "Manifest",
    "PrivacyConfig",
    "build_manifest",
    "canonical_bytes",
    "canonical_json",
    "canonicalize",
    "compare",
]

__version__ = "0.1.0"
