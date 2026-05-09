import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


FIELDS = [
    "scenario",
    "method",
    "n",
    "k",
    "m",
    "file_path",
    "file_bytes",
    "distribution_time_s",
    "cloud_to_edge_bytes",
    "edge_to_edge_bytes",
    "distribution_cost_units",
    "time_ratio",
    "cost_ratio",
    "e2e_overhead_ratio",
    "time_winner",
    "cost_winner",
]

# Do not merge repeated-run CSVs or prior merged/summary artifacts (would double-count rows).
_MERGED_OR_REPEATED_NAMES = frozenset(
    {"metrics_all.csv", "metrics_all_repeated.csv", "metrics_summary.csv"}
)
_RUN_CSV_RE = re.compile(r"^run_\d+_[^.]+\.csv$")


def _should_merge_csv(p: Path, out_path: Path) -> bool:
    if p.resolve() == out_path.resolve():
        return False
    if p.name in _MERGED_OR_REPEATED_NAMES:
        return False
    if _RUN_CSV_RE.match(p.name):
        return False
    return True


def _read_rows(p: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with p.open(newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if not row:
                continue
            # Normalize to expected fields (some files may have extra columns)
            rows.append({k: (row.get(k) or "") for k in FIELDS})
    return rows


def _to_float(v: str) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _winner(a: Optional[float], b: Optional[float], *, eps: float = 1e-12) -> str:
    if a is None or b is None:
        return ""
    if abs(a - b) <= eps:
        return "tie"
    return "edgehydra" if a < b else "edgedis"


def _avg(values: List[Optional[float]]) -> Optional[float]:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def add_comparison_columns(df: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Add derived comparison columns while keeping backward compatibility.
    If a (scenario, file) group does not contain both methods, ratio/winner are left empty.
    """
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for row in df:
        key = (
            row.get("scenario", ""),
            row.get("file_bytes", ""),
        )
        groups.setdefault(key, []).append(row)

    summary_rows: List[Tuple[str, str, Optional[float], Optional[float], str]] = []
    for key, rows in groups.items():
        scenario, file_bytes = key
        hydra_rows = [r for r in rows if r.get("method", "") == "edgehydra"]
        dis_rows = [r for r in rows if r.get("method", "") == "edgedis"]

        # Per-row metric: e2e_overhead_ratio = E2E / C2E
        for row in rows:
            c2e = _to_float(row.get("cloud_to_edge_bytes", ""))
            e2e = _to_float(row.get("edge_to_edge_bytes", ""))
            if c2e is not None and c2e > 0 and e2e is not None:
                row["e2e_overhead_ratio"] = f"{(e2e / c2e):.6f}"
            else:
                row["e2e_overhead_ratio"] = ""
            # Defaults for backward compatibility in incomplete groups
            row.setdefault("time_ratio", "")
            row.setdefault("cost_ratio", "")
            row.setdefault("time_winner", "")
            row.setdefault("cost_winner", "")

        if not hydra_rows or not dis_rows:
            continue

        hydra_time = _avg([_to_float(r.get("distribution_time_s", "")) for r in hydra_rows])
        dis_time = _avg([_to_float(r.get("distribution_time_s", "")) for r in dis_rows])
        hydra_cost = _avg([_to_float(r.get("distribution_cost_units", "")) for r in hydra_rows])
        dis_cost = _avg([_to_float(r.get("distribution_cost_units", "")) for r in dis_rows])

        time_ratio: Optional[float] = None
        cost_ratio: Optional[float] = None
        if hydra_time is not None and dis_time is not None and dis_time != 0:
            time_ratio = hydra_time / dis_time
        if hydra_cost is not None and dis_cost is not None and dis_cost != 0:
            cost_ratio = hydra_cost / dis_cost

        time_winner = _winner(hydra_time, dis_time)
        cost_winner = _winner(hydra_cost, dis_cost)

        for row in rows:
            row["time_ratio"] = f"{time_ratio:.6f}" if time_ratio is not None else ""
            row["cost_ratio"] = f"{cost_ratio:.6f}" if cost_ratio is not None else ""
            row["time_winner"] = time_winner
            row["cost_winner"] = cost_winner

        summary_rows.append((scenario, file_bytes, time_ratio, cost_ratio, time_winner))

    # Human-readable summary.
    print("\n=== EdgeHydra vs EdgeDis Summary ===")
    print("Scenario       | File   | Time ratio | Cost ratio | Time winner")
    for scenario, file_bytes, time_ratio, cost_ratio, time_winner in sorted(summary_rows):
        file_mib = "?"
        try:
            file_mib = f"{int(file_bytes) // (1024 * 1024)}MiB"
        except Exception:
            pass
        tr = f"{time_ratio:.2f}" if time_ratio is not None else "?"
        cr = f"{cost_ratio:.2f}" if cost_ratio is not None else "?"
        suffix = " \u2713" if time_winner == "edgehydra" else ""
        tw = f"{time_winner}{suffix}" if time_winner else "?"
        print(f"{scenario:<14} | {file_mib:<6} | {tr:>10} | {cr:>10} | {tw}")

    return df


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge per-run compose CSVs into one file")
    ap.add_argument("--in-dir", default="results/compose", help="Directory containing per-run CSVs")
    ap.add_argument("--out", default="results/compose/metrics_all.csv")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Per-scenario compose outputs only — not run_* or metrics_* merged files.
    csvs = sorted(p for p in in_dir.glob("*.csv") if _should_merge_csv(p, out_path))
    all_rows: List[Dict[str, str]] = []
    for p in csvs:
        all_rows.extend(_read_rows(p))

    # Stable ordering: scenario, method, file_bytes
    def _key(row: Dict[str, str]):
        try:
            fb = int(row.get("file_bytes") or "0")
        except Exception:
            fb = 0
        return (row.get("scenario", ""), row.get("method", ""), fb, row.get("file_path", ""))

    all_rows.sort(key=_key)
    all_rows = add_comparison_columns(all_rows)

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in all_rows:
            w.writerow(row)

    print(f"Merged {len(csvs)} files -> {len(all_rows)} rows at {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

