# Typed document annotations

CorpusLedger can preserve an exact text document together with typed, named span
layers. An `AnnotationDocument` is an immutable local snapshot, separate from the
JSON-record manifests and transformations. It supports feature validation,
cross-annotation references, interval queries, JSON round-trips and UTF-16 offset
conversion. The [processor pipeline](annotation-pipelines.md) adds dependency
planning and per-step provenance over these snapshots.

## One document, two related layers

```python
from corpusledger import (
    AnnotationDocument,
    AnnotationField,
    AnnotationType,
    SpanAnnotation,
    codepoint_to_utf16,
    utf16_to_codepoint,
)

token = AnnotationType("token", {"surface": AnnotationField()})
entity = AnnotationType(
    "entity",
    {
        "kind": AnnotationField(),
        "tokens": AnnotationField("references", target_type="token"),
    },
)
document = AnnotationDocument(
    "demo",
    "Hi 🌍",
    (token, entity),
    (
        SpanAnnotation("t1", "token", 0, 2, {"surface": "Hi"}),
        SpanAnnotation("t2", "token", 3, 4, {"surface": "🌍"}),
        SpanAnnotation("e1", "entity", 3, 4, {"kind": "symbol", "tokens": ["t2"]}),
    ),
)
assert document.span_text("t2") == "🌍"
assert document.index("token").query(3, 4, relation="exact")[0].annotation_id == "t2"
assert codepoint_to_utf16(document.text, 4) == 5
assert utf16_to_codepoint(document.text, 5) == 4
assert AnnotationDocument.from_dict(document.to_dict()) == document
```

Annotation IDs are unique within a document. A type's field mapping is a closed
schema: undeclared fields, missing required values and implicit type conversions
are rejected. `integer` and `number` never accept booleans. Field kinds are
`string`, `integer`, `number`, `boolean`, `object`, `array`, `reference`, and
`references`. Optional (`required=False`) and nullable (`nullable=True`) are
independent. Container fields permit finite JSON content rather than pretending
to enforce a complete JSON Schema dialect.

A reference is an annotation ID; `references` is an ordered array of IDs.
`target_type` optionally constrains the target layer. All targets must exist in
the same resulting document, including forward targets. Cyclic and self
references are permitted: annotation relations are data, not processor scheduling
dependencies. Reference target types must be declared even in empty layers.

The constructor snapshots caller mappings/lists into read-only mappings/tuples.
Mutating input containers or a later `to_dict()` result cannot change the document.
The original text is never normalized, edited, or silently realigned with spans.
Applications must construct a new, valid snapshot when changing text.

## Offset and interval contract

Offsets are Unicode code-point boundaries, with `0 <= start <= end <= len(text)`.
Intervals are half-open `[start, end)`. They are not UTF-8 byte offsets, UTF-16
code-unit offsets, token positions or grapheme counts. A combining sequence can
span multiple code points. A supplementary character such as `🌍` occupies one
code point but two UTF-16 units. Conversion functions reject a UTF-16 boundary
inside a surrogate pair. Unpaired surrogates are invalid everywhere in the model.

| Query | Rule |
| --- | --- |
| `exact` | Both boundaries are equal. |
| `inside` | Span boundaries are within query boundaries. Empty anchors at either boundary are included. |
| `covering` | Span contains the query; for a point query, a nonempty span includes its start but excludes its end. An anchor covers itself. |
| `overlapping` | Nonempty intersection of two nonempty half-open intervals. Anchors never overlap. |

Results are sorted by `(start, end, annotation_id)`, not insertion order. An index
can contain all layers or one declared type. It stores sorted starts and prefix
maximum ends; bisect prunes candidates in logarithmic time, followed by filtering.
The worst case is linear per query for heavily nested/long overlapping spans.
Construction uses `O(n log n)` time and `O(n)` additional storage. Reuse an index
for repeated queries; `document.index()` constructs a new one.

## CLI and serialization

```console
corpusledger annotations create text.txt document.json --id example
corpusledger annotations tokenize document.json tokenized.json
corpusledger annotations validate tokenized.json
corpusledger annotations query tokenized.json 0 5 --type token --relation overlapping
```

`create` reads UTF-8 bytes without newline translation, preserving CRLF and
combining characters. `tokenize` appends one deterministic layer using
`\w+|[^\w\s]` under Python's Unicode regular-expression behavior. Each annotation
has a `text` feature and an ID `<type>:<zero-based-index>`. This intentionally small
word/punctuation tokenizer is not a learned model or a linguistic tokenization
quality claim; for example, a combining mark can be its own token. Use the Python
processor API for custom models and multi-layer workflows. An existing output
type or annotation ID is never overwritten.

The `corpusledger.annotations.v1` document envelope contains `id`, exact `text`,
`text_sha256`, `offset_unit`, `types`, and `annotations`, plus the `format` tag.
Every field descriptor records its kind, required/nullable flags and target type.
`from_dict()` verifies the exact envelope, reference integrity and text hash.
The full-document `digest` is SHA-256 of its canonical sorted-key UTF-8 JSON.
It includes text, schemas, features and spans. It is a content identity, not a
signature or protection against an attacker who can replace the whole document.

CLI JSON decoding rejects duplicate keys and nonfinite/oversized numbers. Report
and document outputs cannot alias the input by resolved path or hard link. Output
is staged in the destination directory and atomically replaced only after complete
validation; malformed input and oversized output preserve an existing destination.
This protects normal local use, not hostile concurrent filesystem symlink races.
Annotation documents include raw text and feature values: they are **not redacted
privacy reports**. Query output also includes matched feature values.

## Limits and remaining scope

The local model allows at most 10,000,000 text code points and 1,000,000 annotations
per document. Features permit 32 nesting levels and integers of at most 4,300
digits (or the active Python interpreter's smaller conversion limit). Float
values must be finite. The CLI bounds input and serialized output to 128 MiB.
Token collection stops at the remaining annotation budget before allocating an
excess token. These are ceilings, not fixed memory-use promises: complete text,
annotations and serialized data remain in memory. Keep interpreter JSON integer
limits stable while constructing and serializing a snapshot.

This delivers a Python local document and span-processing workflow. The
[annotation store](annotation-store.md) adds versioned multi-document event
storage and atomic publication of processed document snapshots. Remote processor
orchestration, cross-language RPC/protocol implementations, distributed deployment,
trained NLP models and gold-standard annotation accuracy remain separate requirements
in the whole-repository alignment audit.
