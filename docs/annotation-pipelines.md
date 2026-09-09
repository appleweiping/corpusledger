# Typed annotation processor pipelines

`AnnotationPipeline` runs an explicitly supplied set of local Python processors
over an immutable `AnnotationDocument`. Each processor declares its exact input
schemas, new output schemas, a stable name, and an implementation/configuration
version. Planning validates the entire dependency graph before running any code.

This pipeline complements the JSON-record transformations in `pipeline.py`: it
preserves one document's original text and adds typed span layers, including
references between annotations. It does not launch a remote service, import a
callable named in a configuration file, discover plugins, or provide deployment
or distributed scheduling.

```python
import re

from corpusledger.annotations import (
    AnnotationDocument, AnnotationField, AnnotationType, SpanAnnotation,
)
from corpusledger.annotation_pipeline import AnnotationPipeline, AnnotationProcessor

token_type = AnnotationType("Token", {"surface": AnnotationField("string")})
sentence_type = AnnotationType(
    "Sentence",
    {"tokens": AnnotationField("references", target_type="Token")},
)

def tokens(document):
    for index, match in enumerate(re.finditer(r"\w+", document.text)):
        yield SpanAnnotation(
            f"token-{index}", "Token", match.start(), match.end(),
            {"surface": match.group()},
        )

def whole_document_sentence(document):
    token_ids = [
        annotation.annotation_id
        for annotation in document.annotations
        if annotation.type_name == "Token"
    ]
    yield SpanAnnotation(
        "sentence-0", "Sentence", 0, len(document.text), {"tokens": token_ids},
    )

pipeline = AnnotationPipeline([
    # Declaration order does not override dependencies.
    AnnotationProcessor(
        "sentence", "1", whole_document_sentence,
        requires=(token_type,), produces=(sentence_type,),
    ),
    AnnotationProcessor("tokens", "1", tokens, produces=(token_type,)),
])
source = AnnotationDocument("example", "Café 🌍 speaks.")
assert pipeline.plan(source) == ("tokens", "sentence")
result = pipeline.run(source)
assert source.annotations == ()
assert result.document.span_text("token-1") == "speaks"
assert result.document.get("sentence-0").features["tokens"] == ("token-0", "token-1")
provenance = result.to_dict()
```

The example's regular expression is a small illustrative tokenizer; the second
processor deliberately labels the whole document as one sentence. Applications
can supply their own algorithms while retaining the schema and provenance checks.

## Planning and typed layers

`requires` and `produces` contain `AnnotationType` values. Input requirements must
match complete schemas, including feature kinds, required/nullable flags, and
reference target types. A matching name with different fields fails during
planning. Empty layers still supply a schema, so a downstream processor can handle
documents that contain no tokens or no matching entities.

Every output type has one producer. A producer cannot overwrite a type already
present in the input document. Use those input types as complete seed layers; use
a distinct output type when deriving or replacing an existing representation.
Duplicate producer names, duplicate output types, missing inputs, incompatible
schemas, and dependency cycles fail before any callback executes. A reference
target type must be declared in the processor's `requires` or `produces` fields.
Mutual references within one processor's output are valid when the complete output
document resolves them.

Independent ready processors execute in lexical name order. The pipeline is
sequential; it does not parallelize potentially stateful callbacks. The source
document and each completed intermediate document use canonical schema/span
ordering. Reordering processor declarations or equivalent yielded annotations
therefore does not change the resulting content or provenance ordering.

## Execution, failure, and provenance

Each callback receives an immutable document and returns an iterable of
`SpanAnnotation` values for its declared output types. The pipeline collects and
validates that output before invoking the next callback. It checks annotation
types, duplicate IDs, code-point offsets, feature types, and reference targets
through `AnnotationDocument`. Results may include zero-length anchors. Text and
document ID are preserved exactly; no Unicode normalization occurs.

Output collection respects the document's annotation-count limit and stops a
generator after one excess annotation. It cannot interrupt arbitrary Python code
that hangs before yielding. The complete document remains in memory; this API is
not the streaming JSON-record pipeline.

`AnnotationPipelineResult.document` is the final successor. `input_digest` and
`output_digest` identify the original and resulting complete documents. Each
`AnnotationStepReport` records the processor name/version, required and produced
types, new annotation IDs, and the before/after document digests. Adjacent step
digests form a provenance chain. `result.to_dict()` exports that metadata without
copying document text or feature values; use `result.document.to_dict()` when the
full annotated document is needed.

Versions are caller declarations, not automatic proofs of callable identity.
Change a processor's version when its code, options, model weights, dictionary,
or other resources change. Deterministic replay also requires deterministic
callbacks and those same resources; the pipeline neither caches callbacks nor
claims to recreate unrecorded external state. A version-only change appears in
provenance even when the resulting document content stays identical.

Planning failures invoke no callbacks. An execution or output-validation failure
raises without returning a partially completed pipeline result, and the original
document remains unchanged. Callbacks may already have observed intermediate
documents or performed file, network, database, or other external effects. Those
effects are **not rolled back**. Use pure callbacks when all-or-nothing application
behavior is required; supplying a Python callable is explicit code execution, not
a sandbox.
