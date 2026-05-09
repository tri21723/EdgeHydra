import argparse
import csv
from pathlib import Path
from typing import Dict, List


def _to_int(v: str) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _is_summary_rows(rows: List[Dict[str, str]]) -> bool:
    if not rows:
        return False
    keys = set(rows[0].keys())
    return {"mean_time_s", "std_time_s", "min_time_s", "max_time_s", "mean_cost", "std_cost", "min_cost", "max_cost"}.issubset(keys)


def _check_required_rows(rows: List[Dict[str, str]]) -> List[str]:
    errs: List[str] = []
    if len(rows) != 20:
        errs.append(f"expected 20 data rows, got {len(rows)}")
    methods = sorted(set(r.get("method", "") for r in rows))
    scenarios = sorted(set(r.get("scenario", "") for r in rows))
    if methods != ["edgedis", "edgehydra"]:
        errs.append(f"unexpected methods: {methods}")
    expected_scenarios = ["failed_servers", "normal", "slow_servers", "variable_file_sizes"]
    if sorted(scenarios) != expected_scenarios:
        errs.append(f"unexpected scenarios: {scenarios}")
    return errs


def _check_variable_sizes(rows: List[Dict[str, str]]) -> List[str]:
    errs: List[str] = []
    vars_rows = [r for r in rows if r.get("scenario") == "variable_file_sizes"]
    for method in ["edgedis", "edgehydra"]:
        mrows = [r for r in vars_rows if r.get("method") == method]
        mrows.sort(key=lambda r: _to_int(r.get("file_bytes", "0")))
        if len(mrows) != 7:
            errs.append(f"{method}: expected 7 variable-size rows, got {len(mrows)}")
            continue
        # cloud_to_edge must be non-decreasing with file size
        c2e = [_to_int(r.get("cloud_to_edge_bytes", "0")) for r in mrows]
        if any(c2e[i] > c2e[i + 1] for i in range(len(c2e) - 1)):
            errs.append(f"{method}: cloud_to_edge_bytes is not non-decreasing by file size")
        # edge_to_edge should be > 0 for all variable-size rows
        e2e = [_to_int(r.get("edge_to_edge_bytes", "0")) for r in mrows]
        if any(x <= 0 for x in e2e):
            errs.append(f"{method}: some variable-size rows have edge_to_edge_bytes <= 0")
    return errs


def _check_nonzero_core(rows: List[Dict[str, str]]) -> List[str]:
    errs: List[str] = []
    core_rows = [r for r in rows if r.get("scenario") in {"normal", "slow_servers", "failed_servers"}]
    for r in core_rows:
        sc = r.get("scenario", "?")
        m = r.get("method", "?")
        e2e = _to_int(r.get("edge_to_edge_bytes", "0"))
        c2e = _to_int(r.get("cloud_to_edge_bytes", "0"))
        if c2e <= 0:
            errs.append(f"{sc}/{m}: cloud_to_edge_bytes <= 0")
        if e2e <= 0:
            errs.append(f"{sc}/{m}: edge_to_edge_bytes <= 0")
    return errs


def _to_float(v: str) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _check_summary_rows(rows: List[Dict[str, str]]) -> List[str]:
    errs: List[str] = []
    required = {
        "method",
        "scenario",
        "file_bytes",
        "mean_time_s",
        "std_time_s",
        "min_time_s",
        "max_time_s",
        "mean_cost",
        "std_cost",
        "min_cost",
        "max_cost",
        "runs",
        "n_runs",
        "n_after_outlier_removal",
        "outliers_removed",
    }
    keys = set(rows[0].keys()) if rows else set()
    missing = sorted(required - keys)
    if missing:
        errs.append(f"summary csv missing columns: {missing}")
        return errs
    for r in rows:
        label = f"{r.get('scenario','?')}/{r.get('method','?')}/{r.get('file_bytes','?')}"
        runs = _to_int(r.get("runs", "0"))
        if runs <= 0:
            errs.append(f"{label}: runs <= 0")
        min_t = _to_float(r.get("min_time_s", "0"))
        mean_t = _to_float(r.get("mean_time_s", "0"))
        max_t = _to_float(r.get("max_time_s", "0"))
        std_t = _to_float(r.get("std_time_s", "0"))
        if not (min_t <= mean_t <= max_t):
            errs.append(f"{label}: time stats out of order")
        if std_t < 0:
            errs.append(f"{label}: std_time_s < 0")
        min_c = _to_float(r.get("min_cost", "0"))
        mean_c = _to_float(r.get("mean_cost", "0"))
        max_c = _to_float(r.get("max_cost", "0"))
        std_c = _to_float(r.get("std_cost", "0"))
        if not (min_c <= mean_c <= max_c):
            errs.append(f"{label}: cost stats out of order")
        if std_c < 0:
            errs.append(f"{label}: std_cost < 0")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser(description="Sanity-check merged metrics CSV")
    ap.add_argument("--csv", default="results/compose/metrics_all.csv")
    args = ap.parse_args()

    path = Path(args.csv)
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        return 2

    rows = _load_rows(path)
    errors: List[str] = []
    summary_mode = _is_summary_rows(rows)
    if summary_mode:
        errors.extend(_check_summary_rows(rows))
    else:
        errors.extend(_check_required_rows(rows))
        errors.extend(_check_nonzero_core(rows))
        errors.extend(_check_variable_sizes(rows))

    if errors:
        print("METRICS VALIDATION: FAIL")
        for e in errors:
            print(f"- {e}")
        return 1

    print("METRICS VALIDATION: PASS")
    print(f"- rows: {len(rows)}")
    if summary_mode:
        print("- summary statistics checks passed")
    else:
        print("- scenarios/methods look complete")
        print("- cloud/e2e bytes checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

