# Standalone privacy scanning

Scan a JSON file, JSONL file, or directory of those files without creating a
manifest. The API, CLI, and local service share the same scanner and report:

```python
from corpusledger import PrivacyConfig, scan_corpus

report = scan_corpus("records.jsonl", config=PrivacyConfig.from_pack("pii"))
print(report.records_checked, len(report.findings))
```

```bash
corpusledger privacy records.jsonl --pack pii --output privacy.json
corpusledger privacy records.jsonl --id-field record_id \
  --pack credentials --min-token-length 32 --entropy-threshold 4.0
```

The service request is an object with `operation: "privacy"` and an `input`
path. Optional fields match the API/CLI: `id_field` defaults to `id`, `pack`
to `default`, `min_token_length` to 24, and `entropy_threshold` to 3.7.

Reports have `schema_version: 1`, `privacy_version: "1"`, the resolved input
path, ID field, full scanner configuration, checked-record count, finding
count, and sorted findings. Record count includes clean records. Two detectors
may report the same field: a `token` with a long, high-entropy value can yield
both a sensitive-field-name finding and a high-entropy-token finding. An empty
JSONL file is a successful scan of zero records.

The `default` and `credentials` packs flag credential-like field names; `pii`
also flags names such as `email` and `phone`. Matching is case-insensitive and
uses whole field names. Entropy detection tests whole strings composed of the
documented token alphabet (`A-Z`, `a-z`, `0-9`, `_+./=-`); it does not locate
embedded secrets in prose. Findings are review hints, not proof that a corpus
contains or lacks private information. They contain no matched field values,
but retain record IDs, field paths, and (for entropy findings) length, entropy,
and a short evidence hash. Do not share reports containing sensitive IDs or
keys. Evidence hashes are not encryption or anonymization.

The command returns 0 when scanning and report writing succeed, including when
findings exist, and 2 for expected input/configuration errors. Duplicate IDs
and malformed input fail before report writing. Reports cannot replace an
input file (including a hardlink alias) or be written inside a scanned directory.
This keeps subsequent scans from consuming their own report. Put reports next
to the corpus directory, not inside it.

JSONL bodies are read one at a time; the readers retain unique IDs and the
scanner retains all findings to sort them. Memory is proportional to the
largest record, unique IDs, input file list, and findings. JSON arrays are
loaded as whole documents. Scanning visits nested values and characters once;
sorting F findings costs O(F log F). Reports are deterministic for the same
corpus path, records, configuration, and scanner version.
