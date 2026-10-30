"""Exact comparison of two normalised outcome files (snapshot_outcomes.py output).

usage: python compare_outcomes.py <before.json> <after.json> <label> <out.json>
Exit 1 on any difference. Run ids, timestamps, elapsed times, dbt invocation ids, publication
ids and content hashes (which embed Bronze run ids) are excluded by the normaliser.
"""

import json
import sys


def diff(a, b, path=""):
    if type(a) is not type(b):
        return [f"{path}: {a!r} != {b!r}"]
    if isinstance(a, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append(f"{path}/{k}: only in {'after' if k in b else 'before'}")
            else:
                out += diff(a[k], b[k], f"{path}/{k}")
        return out
    if isinstance(a, list):
        if len(a) != len(b):
            return [f"{path}: {len(a)} != {len(b)} items"]
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in diff(x, y, f"{path}[{i}]")]
    return [] if a == b else [f"{path}: {a!r} != {b!r}"]


before, after = (json.load(open(p)) for p in sys.argv[1:3])
differences = diff(before, after)
decimals = sum(1 for t in ("liquidity_summary", "exposure_compliance") for r in after[t] for v in r.values()
               if v is not None and v.replace(".", "", 1).lstrip("-").isdigit() and "." in v)
report = {
    "label": sys.argv[3],
    "identical": not differences,
    "differences": differences[:50],
    "compared": {
        "scenarios": [s["scenario"] + ":" + s["run_status"] + ":" + str(s["publication"]) for s in after["summary"]],
        "run_reports": len(after["runs"]),
        "liquidity_summary_rows": len(after["liquidity_summary"]),
        "exposure_compliance_rows": len(after["exposure_compliance"]),
        "decimal_values_compared_as_text": decimals,
        "publication_log_rows": len(after["publication_log"]),
    },
    "excluded": ["run ids", "timestamps", "elapsed times", "dbt invocation ids", "publication ids",
                 "content_sha256 (embeds Bronze run ids via lineage_bronze_runs)",
                 "lineage_bronze_runs column", "query text in landing manifests (not in reports)"],
}
json.dump(report, open(sys.argv[4], "w"), indent=2)
print(json.dumps({k: report[k] for k in ("label", "identical")} | {"n_diff": len(differences)}))
sys.exit(0 if not differences else 1)
