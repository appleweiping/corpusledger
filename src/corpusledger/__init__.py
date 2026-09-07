"""CorpusLedger: reproducible manifests and diffs for JSON NLP corpora."""

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes, canonical_json, canonicalize
from .catalog import SnapshotCatalog, SnapshotRef
from .diff import CorpusDiff, compare
from .errors import InputError
from .manifest import MANIFEST_FORMAT, Manifest, build_manifest
from .pipeline import PipelineReport, PipelineStep, drop_fields, rename_field, run_pipeline, select_fields
from .privacy import PrivacyConfig
from .readers import READER_ENTRY_POINT_GROUP, ReaderAdapter, Record, iter_corpus, load_reader_adapter
from .service import CorpusService, create_server
from .signing import (
    SIGNATURE_FORMAT,
    SignatureEnvelope,
    sign_manifest,
    verify_manifest_signature,
)
from .store import (
    BundleReport,
    BundleVerification,
    GarbageCollectionReport,
    ObjectStore,
    bundle_snapshot,
    extract_bundle,
    verify_bundle,
)

__all__ = [
    "CANONICAL_VERSION",
    "MANIFEST_FORMAT",
    "READER_ENTRY_POINT_GROUP",
    "SIGNATURE_FORMAT",
    "BundleReport",
    "BundleVerification",
    "CanonicalPolicy",
    "CorpusDiff",
    "CorpusService",
    "GarbageCollectionReport",
    "InputError",
    "Manifest",
    "ObjectStore",
    "PipelineReport",
    "PipelineStep",
    "PrivacyConfig",
    "ReaderAdapter",
    "Record",
    "SignatureEnvelope",
    "SnapshotCatalog",
    "SnapshotRef",
    "build_manifest",
    "bundle_snapshot",
    "canonical_bytes",
    "canonical_json",
    "canonicalize",
    "compare",
    "create_server",
    "drop_fields",
    "extract_bundle",
    "iter_corpus",
    "load_reader_adapter",
    "rename_field",
    "run_pipeline",
    "select_fields",
    "sign_manifest",
    "verify_bundle",
    "verify_manifest_signature",
]

__version__ = "0.2.0"
