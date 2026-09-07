# Observed schema export

`corpusledger schema` converts the authenticated `schema` section of a
manifest into a draft-2020-12 JSON Schema:

```console
corpusledger schema manifest.json --title "support-corpus" --output schema.json
```

The exporter is deliberately conservative. It preserves mixed observed JSON
types, reports observed non-optional fields as `required`, includes array
length bounds when available, and keeps object `additionalProperties` open.
The result describes what the snapshot contained; it does not invent domain
constraints or replace `corpusledger verify`.

The same operation is available from the local service:

```json
{
  "operation": "schema",
  "manifest": "manifest.json",
  "title": "support-corpus",
  "id": "urn:example:support-corpus"
}
```

Because the source is a loaded manifest, malformed or tampered manifest files
are rejected before a schema is emitted.
