# Resumable record pipelines

The `pipeline` command exposes the same atomic, checkpointed transformation
engine as the Python API:

```console
corpusledger pipeline source.jsonl derived.jsonl \
  --select id,text --rename text=content --drop internal --resume
```

Each step receives one JSON object and must preserve the configured ID field.
The output is written atomically, and a state file records the source/output
digests, ordered step names, record count, and ID field. `--resume` reuses the
output only when every recorded input and pipeline property still matches;
otherwise the source is streamed again. The CLI refuses input/output/state
path collisions and never imports arbitrary code from configuration files.
