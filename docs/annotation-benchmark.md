# Typed annotation benchmark

Run the annotation and span-index workflow against real dialogue text and a
separate 100,000-span synthetic index:

```bash
python benchmarks/benchmark_annotations.py /local/cornell_movie_dialogs_corpus.zip \
  --output benchmarks/results/annotations.json
```

The archive is supplied locally. The script never downloads it, extracts a
dialogue file to disk, or publishes text/vocabulary. It verifies the pinned
SHA-256 `3bde8a571f615201bc2d2453e22878090719638592f774720eddec739de8c900`
for the official [Cornell Movie-Dialogs Corpus archive](https://www.cs.cornell.edu/~cristian/data/cornell_movie_dialogs_corpus.zip).
The downloaded README contains no explicit redistribution license grant; the
raw archive remains outside the repository. Dataset citation: Cristian
Danescu-Niculescu-Mizil and Lillian Lee, *Chameleons in Imagined Conversations*,
CMCL/ACL 2011.

## Real-text workload

The default selects the first 1,000 physical records in `movie_lines.txt`
(a configurable `--limit` from 1 to 1,000). Latin-1 decoding and a delimiter
split limited to four occurrences recover the original line ID and exact
dialogue text. A hash of selected raw lines records the selection, while the
complete member and archive hashes record source provenance. This is a bounded
prefix sample, not a representative sample or a full-corpus performance claim.

The benchmark creates an `AnnotationType` with a required string surface field
and uses `SpanAnnotation` and `AnnotationDocument` to store matches from
`\w+|[^\w\s]`. Matches are Unicode word runs or individual punctuation/symbol
code points. Combining marks, contractions, and language-specific tokenization
may differ from linguistic token conventions. These annotations are generated
by a rule on real text; **they are not gold labels**, and the benchmark does
not measure annotation accuracy.

Every annotation's stored match is checked against the public `span_text`
result. Every start/end boundary is converted to UTF-16 and back, with the
expected UTF-16 length calculated independently using Python's UTF-16 encoder.
For 200 seeded query intervals, indexed overlaps must exactly equal an
exhaustive scan using `max(query_start, span_start) < min(query_end, span_end)`
with empty intervals excluded. The workload includes full-text and empty
queries at both text boundaries. Result order is also verified.

## Synthetic index workload

A separate document contains 200,000 code points made from ASCII, an emoji,
and a combining mark. Its 100,000 spans use deterministic random starts;
every tenth span is an empty anchor, other fifth spans extend up to 5,000 code
points, and the remaining spans extend up to 12. Endpoints are clipped to the
document boundary. The mix includes anchors, short spans, nested spans, and
large overlaps that can defeat start-position pruning.

All 16 seeded index queries are compared with an exhaustive scan of all
100,000 spans (1.6 million candidate comparisons). Supplementary-character,
combining-mark, and end-of-document boundaries also have independent UTF-16
conversion checks. This exercises correctness at a larger scale; the current
index still has linear candidate filtering in the worst case.

## Reading the output

`benchmarks/results/annotations.json` records workload counts, seed, source
digests, benchmark/runtime source hashes, environment, timings, and traced
Python allocation peaks. It contains no dialogue, token vocabulary, annotation
features, or selected record IDs. Runtime file hashes are checked before and
after the run; concurrent source changes abort publication of the result.

Construction time includes annotation creation, feature/schema/reference
validation, and index construction. Query time measures indexed retrieval.
Archive verification/parsing, synthetic text/interval generation, exhaustive
oracles, offset verification, and result digests are outside those times.
Traced peak memory covers construction/indexing/queries and excludes text and
intervals already loaded before tracing; it is not process RSS. Results are
machine-specific. Source hashes make a recorded run auditable, but do not
establish cross-platform performance or equivalence to another repository.
