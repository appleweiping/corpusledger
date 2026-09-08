"""CorpusLedger: reproducible manifests and diffs for JSON NLP corpora."""

from .canonical import CANONICAL_VERSION, CanonicalPolicy, canonical_bytes, canonical_json, canonicalize
from .catalog import SnapshotCatalog, SnapshotRef
from .diff import CorpusDiff, compare
from .errors import InputError
from .external_sort import ExternalSortReport, external_sort_jsonl
from .index import INDEX_FORMAT, DuplicateFieldGroup, IndexRecord, ManifestIndex
from .manifest import MANIFEST_FORMAT, Manifest, RecordEntry, build_manifest
from .pipeline import PipelineReport, PipelineStep, drop_fields, rename_field, run_pipeline, select_fields
from .plan import PLAN_FORMAT, PipelinePlan, PlanStep, load_pipeline_plan
from .privacy import PrivacyConfig, privacy_packs
from .readers import READER_ENTRY_POINT_GROUP, ReaderAdapter, Record, iter_corpus, load_reader_adapter
from .schema import (
    SchemaCompatibilityIssue,
    SchemaCompatibilityReport,
    SchemaValidationIssue,
    compare_json_schemas,
    to_json_schema,
    validate_json_schema,
)
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
from .stream import NdjsonGateway, StreamReport

__all__ = [
    "CANONICAL_VERSION",
    "INDEX_FORMAT",
    "MANIFEST_FORMAT",
    "PLAN_FORMAT",
    "READER_ENTRY_POINT_GROUP",
    "SIGNATURE_FORMAT",
    "BundleReport",
    "BundleVerification",
    "CanonicalPolicy",
    "CorpusDiff",
    "CorpusService",
    "DuplicateFieldGroup",
    "ExternalSortReport",
    "GarbageCollectionReport",
    "IndexRecord",
    "InputError",
    "Manifest",
    "ManifestIndex",
    "NdjsonGateway",
    "ObjectStore",
    "PipelinePlan",
    "PipelineReport",
    "PipelineStep",
    "PlanStep",
    "PrivacyConfig",
    "ReaderAdapter",
    "Record",
    "RecordEntry",
    "SchemaCompatibilityIssue",
    "SchemaCompatibilityReport",
    "SchemaValidationIssue",
    "SignatureEnvelope",
    "SnapshotCatalog",
    "SnapshotRef",
    "StreamReport",
    "build_manifest",
    "bundle_snapshot",
    "canonical_bytes",
    "canonical_json",
    "canonicalize",
    "compare",
    "compare_json_schemas",
    "create_server",
    "drop_fields",
    "external_sort_jsonl",
    "extract_bundle",
    "iter_corpus",
    "load_pipeline_plan",
    "load_reader_adapter",
    "privacy_packs",
    "rename_field",
    "run_pipeline",
    "select_fields",
    "sign_manifest",
    "to_json_schema",
    "validate_json_schema",
    "verify_bundle",
    "verify_manifest_signature",
]

__version__ = "0.2.0"
