# JSON Schema compatibility

CorpusLedger can compare two exported JSON Schemas without installing a schema
library:

```bash
corpusledger schema-compat before.schema.json after.schema.json --mode full
```

The report identifies required-property changes, type narrowing or widening,
enum changes, array/string constraint changes, nested property changes, and
`additionalProperties` transitions. `backward` asks whether values accepted by
the old schema remain accepted by the new schema; `forward` asks the reverse;
`full` requires both directions. Exit status `0` means compatible and `2` means
that at least one issue was found.

The same operation is available as `compare_json_schemas` and through the local
service operation `schema_compat`. Unknown JSON Schema keywords are ignored,
matching CorpusLedger's dependency-free validation subset. This is a
conservative structural compatibility signal, not a proof that application
semantics or model behavior remain unchanged.
