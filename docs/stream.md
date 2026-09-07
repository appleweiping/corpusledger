# NDJSON gateway

`NdjsonGateway` is a small transport-neutral boundary for record processors. It is useful when a corpus operation needs
to be driven from a shell, another language, or a service adapter while preserving deterministic request/response
alignment.

## Wire format

Each input line is a UTF-8 JSON object:

```json
{"processor":"normalize","payload":{"text":" hello "}}
```

The gateway emits one compact JSON line for every input line. Successes have the shape
`{"ok":true,"processor":"...","result":{...}}`; malformed JSON, invalid UTF-8, unknown processors, oversized
lines, and processor exceptions become `{"ok":false,"error":"..."}`. A bad line therefore never shifts the response
sequence. Processor results must be JSON objects.

`NdjsonGateway.process_lines()` returns both rendered responses and a `StreamReport` containing SHA-256 digests of the
normalized input and output streams, the record count, and success/failure counts. Newline normalization is part of the
digest contract: each line is hashed with exactly one trailing `\n`.

## CLI

The CLI registers two safe built-ins:

```console
printf '%s\n' '{"processor":"identity","payload":{"id":"a"}}' \
  | corpusledger stream
printf '%s\n' '{"processor":"select","payload":{"id":"a","text":"hello"}}' \
  | corpusledger stream --field id --report-output stream-report.json
```

`select` keeps only fields named by repeated `--field` options. `--max-line-bytes` bounds each normalized request and
`--strict` returns process status `2` if any response is an error; non-strict mode still emits every error response and
returns status `0`. Response NDJSON is written to stdout. Without `--report-output`, the digest/count report is written
to stderr so stdout remains a machine-readable stream.

Applications needing domain-specific processors should register them through the Python API rather than asking the CLI to
import untrusted code.
