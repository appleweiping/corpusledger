# corpusledger

corpusledger shows which records and fields changed between two versions of a dataset (JSONL,
JSON, CSV or Parquet), even when the data has no ID column. Think `git diff`, but per record
instead of per line.

Here it is on the original Stanford Alpaca data
([tatsu-lab/alpaca](https://huggingface.co/datasets/tatsu-lab/alpaca)) and its cleaned version
([yahma/alpaca-cleaned](https://huggingface.co/datasets/yahma/alpaca-cleaned)). Neither has an ID,
so records are matched by their `instruction` + `input`; the `text` column only exists in the
first file and is left out:

```console
$ corpusledger diff alpaca.parquet alpaca_data_cleaned.json --key instruction,input --ignore text
alpaca.parquet -> alpaca_data_cleaned.json
records matched by key instruction + input, ignoring text

  before      52,002
  after       51,760

  unchanged    1,832
  changed     47,447
  key edited      51   key fields edited, rest identical
  removed      2,672
  added        2,430
  note: 13 repeated IDs in after (alpaca_data_cleaned.json), numbered #2, #3, ...

fields changed (records)
  /output       47,447
  /instruction      43
  /input            29

row order of the common records changed

examples
  changed    Give three tips for staying healthy.
             /output
               - "1.Eat a balanced diet and make sure to include plenty of fruits an..."
               + "1. Eat a balanced and nutritious diet: Make sure your meals are in..."
  changed    What are the three primary colors?
             /output
               - "... and yellow."
               + "... and yellow. These colors are called primary because they cannot b..."
  changed    Describe the structure of an atom.
             /output
               - "An atom is made up of a nucleus, which contains protons and neutro..."
               + "An atom is the basic building block of all matter and is made up o..."
  key edited Pretend you are a project manager of a construction company. Describe...
             /instruction
               - "Describe a time when you had to make a difficult decision."
               + "Pretend you are a project manager of a construction company. Descr..."
  key edited Create a description for a person with the following name | John
             /instruction
               - "Describe the following person"
               + "Create a description for a person with the following name"
  key edited Suggest what content a web page about robotics should consist of.
             /instruction
               - "Create a web page about robotics."
               + "Suggest what content a web page about robotics should consist of."
  removed    Identify the odd one out. | Twitter, Instagram, Telegram
  removed    Render a 3D model of a house
  removed    Reverse engineer this code to create a new version | def factorialize...
  added      Write a concise summary of the following: "Commodore 64 (commonly kno...
  added      Refactor this code and add comments. | def factorialize(num): factori...
  added      Based on the information provided, rewrite the sentence by changing i...
```

So "cleaned" is a bigger change than the name suggests: of the 49,279 prompts that appear in
both versions, 47,447 (96%) got a different answer. I recounted with a separate plain-Python
script ([`examples/check_alpaca.py`](examples/check_alpaca.py)) and got the same numbers.

To reproduce (68 MB download):

```bash
curl -L -o alpaca.parquet https://huggingface.co/datasets/tatsu-lab/alpaca/resolve/main/data/train-00000-of-00001-a09b74b3ef9c3b56.parquet
curl -LO https://huggingface.co/datasets/yahma/alpaca-cleaned/resolve/main/alpaca_data_cleaned.json
corpusledger diff alpaca.parquet alpaca_data_cleaned.json --key instruction,input --ignore text
```

## Why I wrote it

When a dataset gets "cleaned", re-exported or re-annotated, I want to know what actually changed
before I retrain on it. `git diff` on a 50 MB JSON file is useless, and a file hash only tells me
*that* something changed. corpusledger answers the questions in between: how many records were
edited, added or dropped, in which fields, and what a few of the edits look like.

## Install

```bash
pip install "corpusledger[parquet] @ git+https://github.com/appleweiping/corpusledger"
```

No required dependencies; the `parquet` extra adds pyarrow. Python 3.10 or newer. Try it on the
two small files in `examples/`:

```console
$ corpusledger diff examples/before.jsonl examples/after.jsonl
before.jsonl -> after.jsonl
records matched by id field 'id'
...
examples
  changed    doc-2
             /metadata/reviewed  added: false
             /text
               - "需要人工复核。"
               + "需要再次人工复核。"
  added      doc-3
```

## Usage

```bash
corpusledger diff OLD NEW [--id FIELD | --key F1,F2 | --content] [--ignore F1,F2] [--json] [-o FILE]
corpusledger snapshot DATA -o DATA.snapshot.json [same matching options]
```

`OLD` and `NEW` can each be a file, a directory (all supported files inside, in path order) or
a snapshot.

**Matching records.** By default records are matched by an `id` field if the first record has
one. `--id FIELD` picks another field. `--key instruction,input` matches by a hash of those
fields, for data without IDs. `--content` matches by the whole record; then every edit shows up as
one removal plus one addition. With `--key`, a removed and an added record whose other fields are
identical are reported as **key edited** instead (someone fixed a typo in the prompt). Repeated
IDs are numbered `#2`, `#3`, ... in file order and reported.

**Ignoring fields.** `--ignore text,meta` drops top-level fields before hashing;
`--ignore /meta/timestamp` drops a nested one (JSON Pointer).

**Snapshots.** `corpusledger snapshot` stores, per record, the ID, a short label, and 64-bit
SHA-256 prefixes of the record and of every field (for Alpaca: 13 MB for 52,002 records). Diffing
against a snapshot gives the same counts without the old data, just no value examples. The file
is JSON with one record per line, so committing it next to your training code makes `git diff`
of the snapshot readable too. The matching options are saved in the snapshot and reused.

**CI.** Exit status is 0 when nothing differs, 1 when something does and 2 on errors. `--json`
prints everything: counts, per-field counts, schema changes and the full lists of changed,
removed and added IDs, e.g. to fail a job when a "frozen" eval set has moved:

```bash
corpusledger diff evals/test.snapshot.json data/test.jsonl --examples 0
```

**Normalization.** Records are compared after Unicode NFC normalization and key sorting, so
reformatting JSON or reordering keys is not a change. List order is kept, values keep their JSON
type (`1`, `1.0` and `"1"` are three different values), and Parquet dates and bytes become ISO
strings and base64.

## Why not ...

- **`git diff` / `diff`**: line-based. Fine for small JSONL with one record per line in a stable
  order, useless for a JSON array, a Parquet file, reordered rows or 47,000 edits you want counted.
- **DVC, git-lfs, file checksums**: they version whole files and tell you a file changed, not which
  records. Use them to store the data; use corpusledger to read the change.
- **Hugging Face `datasets` fingerprints**: a hash of the cache/transform history, meant for
  caching. Two equal fingerprints mean "same", different ones mean nothing more specific.
- **datacompy** and pandas `compare`: good for tables with a join column. corpusledger handles
  nested JSON, builds a key when there is no ID column, and can compare against a snapshot after
  the old data is gone.

## Limitations

- When both sides are data files, the normalized records are kept in memory to print examples.
  Alpaca (2 × 50k records) is fine; for multi-GB data, snapshot each side first.
- It is plain Python and not fast. The Alpaca diff above took about three minutes on my desktop
  while it was busy with other jobs; snapshotting one side took 1.5 minutes.
- Hashes are truncated to 64 bits. That is plenty to detect accidental changes, not a defense
  against someone crafting collisions.
- Matching is exact. A prompt with one changed character and a changed answer counts as removed
  plus added.

Related: [splitproof](https://github.com/appleweiping/splitproof) keeps near-duplicate texts on
one side of a train/test split, [turnscope](https://github.com/appleweiping/turnscope) lints chat
fine-tuning files; [metricguard](https://github.com/appleweiping/metricguard) and
[promptwitness](https://github.com/appleweiping/promptwitness) are in the same small toolbox.

MIT License. The 0.2 design (privacy scanner, reader plugins, Ed25519-signed manifests) is under
the `v0.2.0` tag; if you need a signed snapshot today, sign the file with `ssh-keygen -Y sign` or
minisign.
