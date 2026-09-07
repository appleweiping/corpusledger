# Schema validation

`corpusledger schema-validate INPUT SCHEMA.json` checks a JSON/JSONL corpus
without retaining the whole corpus. It reports JSON Pointer paths and exits 0
when every record satisfies the supported subset, or 2 when a record fails.

The dependency-free validator supports `type`, `enum`, `const`, `required`,
`properties`, `additionalProperties: false`, `items`, array bounds, string
length/pattern, numeric bounds, `anyOf`, and `allOf`. Unknown keywords are
ignored deliberately; use a full JSON Schema implementation when vocabulary
complete draft-2020-12 validation is required.

```console
$ corpusledger schema-validate records.jsonl observed-schema.json
{
  "errors": [],
  "records_checked": 12,
  "valid": true
}
```
