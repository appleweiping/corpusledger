"""Recount the Alpaca vs alpaca-cleaned numbers in the README without corpusledger.

python examples/check_alpaca.py alpaca.parquet alpaca_data_cleaned.json
"""

import collections
import json
import sys

import pyarrow.parquet as pq


def keyed(rows):
    """(instruction, input) -> output; repeated keys get an occurrence number."""
    result, seen = {}, collections.Counter()
    for row in rows:
        key = (row["instruction"], row["input"])
        seen[key] += 1
        result[key if seen[key] == 1 else (key, seen[key])] = row["output"]
    return result


old = keyed(pq.read_table(sys.argv[1]).to_pylist())
with open(sys.argv[2], encoding="utf-8") as stream:
    new = keyed(json.load(stream))
common = old.keys() & new.keys()
unchanged = sum(old[key] == new[key] for key in common)
removed = [key for key in old if key not in new]
added = [key for key in new if key not in old]
# a removed and an added record with the same, otherwise unique, output: the prompt was edited
removed_outputs = collections.Counter(old[key] for key in removed)
added_outputs = collections.Counter(new[key] for key in added)
edited = {out for out, n in removed_outputs.items() if n == 1 and added_outputs.get(out) == 1}
print(f"unchanged {unchanged}  changed {len(common) - unchanged}  key edited {len(edited)}")
print(f"removed {len(removed) - len(edited)}  added {len(added) - len(edited)}")
