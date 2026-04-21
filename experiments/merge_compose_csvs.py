import argparse
import csv
from pathlib import Path
from typing import Dict, List


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
]


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


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge per-run compose CSVs into one file")
    ap.add_argument("--in-dir", default="results/compose", help="Directory containing per-run CSVs")
    ap.add_argument("--out", default="results/compose/metrics_all.csv")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Merge all CSVs except the output file itself (if rerun).
    csvs = sorted([p for p in in_dir.glob("*.csv") if p.resolve() != out_path.resolve()])
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

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in all_rows:
            w.writerow(row)

    print(f"Merged {len(csvs)} files -> {len(all_rows)} rows at {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

