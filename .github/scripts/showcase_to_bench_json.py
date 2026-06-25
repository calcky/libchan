#!/usr/bin/env python3
"""Convert bench_showcase table output into github-action-benchmark JSON.

Usage:  showcase_to_bench_json.py <showcase.txt>   (or read stdin)

bench_showcase prints fixed-width rows:  <label>  <median_ns>  <min_ns>  <Mops>
We emit the throughput (Mops, bigger-is-better) for libchan's OWN metrics only
— rows whose label contains "chan" — so the regression dashboard tracks the
library, not the memcpy/atomic/ring reference floors.

Output: a JSON array of {name, unit:"Mops/s", value:<float>} on stdout, the
`customBiggerIsBetter` format consumed by benchmark-action/github-action-benchmark.
"""
import json
import sys


def is_float(tok: str) -> bool:
    try:
        float(tok)
        return True
    except ValueError:
        return False


def main() -> int:
    data = open(sys.argv[1]).read() if len(sys.argv) > 1 else sys.stdin.read()
    out = []
    for line in data.splitlines():
        toks = line.split()
        # need at least one label token + 3 trailing numeric columns
        if len(toks) < 4 or not all(is_float(t) for t in toks[-3:]):
            continue
        name = " ".join(toks[:-3]).strip()
        if "chan" not in name:          # libchan's own rows only
            continue
        mops = float(toks[-1])          # 4th-from-... last column is Mops/s
        out.append({"name": name, "unit": "Mops/s", "value": mops})

    if not out:
        sys.stderr.write("showcase_to_bench_json: no 'chan' rows parsed — "
                         "is the bench_showcase output format unchanged?\n")
        return 1
    json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
