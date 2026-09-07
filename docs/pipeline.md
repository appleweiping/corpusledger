# Streaming transformation pipelines

`run_pipeline` applies named pure Python transformations to a strict CorpusLedger
input and writes a derived JSONL corpus atomically. It keeps the reader's global
duplicate-ID index plus one record in memory; it does not materialize the whole
dataset. Every step must preserve the normalized ID field. This makes derived
manifests joinable to the source and avoids accidental identity changes.

```python
from corpusledger import drop_fields, rename_field, run_pipeline, select_fields

report = run_pipeline(
    "raw/", "derived.jsonl",
    [select_fields(("id", "text", "lang")), rename_field("text", "content")],
    state="derived.state.json",
)
```

The output is written to a sibling temporary file and atomically replaced only
after every input record and transformation succeeds. Existing output and state
files remain untouched on parse, transform, duplicate-ID, or serialization
failure. A completion state records schema version, source digest, output digest,
ID field, record count, and ordered step names.

`resume=True` reuses output only when the checkpoint is complete and the source
digest, output digest, output path, ID field, and ordered step names all match.
The digest covers supported input bytes and the canonicalization policy. A step
name is provenance metadata, not a hash of callable code; changing a function
without changing its name requires a new name or deleting the checkpoint.
Stale, malformed, or manually edited checkpoints are ignored and the pipeline
runs again.

Built-in steps are `select_fields`, `drop_fields`, and `rename_field`. Selection
omits absent fields; callers must explicitly retain the ID. Renaming refuses
destination collisions. Custom steps receive a fresh dictionary and should not
mutate external state. Their outputs must be JSON objects accepted by the strict
canonicalizer. The module intentionally does not execute untrusted pipeline
code, provide distributed scheduling, or pretend arbitrary callables are
reproducible without versioned source/build metadata.
