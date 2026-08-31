"""CorpusLedger: reproducible manifests and diffs for JSON NLP corpora."""

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes, canonical_json, canonicalize
from .diff import CorpusDiff, compare
from .manifest import MANIFEST_FORMAT, Manifest, build_manifest
from .privacy import PrivacyConfig
from .readers import READER_ENTRY_POINT_GROUP, ReaderAdapter, Record, iter_corpus, load_reader_adapter
from .signing import (
    SIGNATURE_FORMAT,
    SignatureEnvelope,
    sign_manifest,
    verify_manifest_signature,
)

__all__ = [
    "CANONICAL_VERSION",
    "MANIFEST_FORMAT",
    "READER_ENTRY_POINT_GROUP",
    "SIGNATURE_FORMAT",
    "CanonicalPolicy",
    "CorpusDiff",
    "Manifest",
    "PrivacyConfig",
    "ReaderAdapter",
    "Record",
    "SignatureEnvelope",
    "build_manifest",
    "canonical_bytes",
    "canonical_json",
    "canonicalize",
    "compare",
    "iter_corpus",
    "load_reader_adapter",
    "sign_manifest",
    "verify_manifest_signature",
]

__version__ = "0.2.0"
