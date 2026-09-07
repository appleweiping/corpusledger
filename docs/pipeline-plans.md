# Versioned pipeline plans

CorpusLedger's pipeline executor remains callable-first, but its built-in
`select`, `drop`, and `rename` operations can also be represented as a strict
versioned JSON plan. Plans are deliberately limited to these pure operations;
arbitrary Python is never imported from a data file.

```json
{
  "format": "corpusledger.pipeline-plan.v1",
  "steps": [
    {"kind": "select", "fields": ["id", "text"]},
    {"kind": "rename", "old": "text", "new": "content"}
  ]
}
```

Run a plan through the same atomic, resumable executor:

```bash
corpusledger pipeline input.jsonl output.jsonl --plan plan.json --resume
```

`--plan` cannot be combined with the imperative `--select`, `--drop`, or
`--rename` flags. Unknown fields, duplicate fields, unsupported step kinds, and
missing version markers fail before the input is read. The Python API is
`PipelinePlan.load(...).compile()` or the convenience `load_pipeline_plan()`.
