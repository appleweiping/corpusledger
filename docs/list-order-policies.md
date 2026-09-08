# Per-field list-order policies

Lists preserve order by default because token and turn sequences are semantic.
When a field is a set-like collection, sort only that field instead of
reordering every list:

```bash
corpusledger snapshot corpus.jsonl manifest.json \
  --sort-path labels --sort-path metadata.languages
```

The dotted selectors are recorded in `hash_metadata.policy`, so a verifier can
reproduce the exact fingerprint. `--sort-lists` remains available for the
legacy all-list policy; providing no selector keeps the previous behavior.
Selectors apply to lists at the matching object path and are deterministic
under Unicode normalization.
