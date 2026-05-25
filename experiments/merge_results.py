import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def read_rows(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))

def main():
    old_csv = ROOT / "results" / "compose" / "metrics_all_repeated.csv"
    new_csv = ROOT / "results" / "compose_new" / "metrics_all_repeated.csv"
    out_all = ROOT / "results" / "compose" / "metrics_all_repeated.csv"
    out_summary = ROOT / "results" / "compose" / "metrics_summary.csv"

    # read old variable_file_sizes rows
    old_rows = read_rows(old_csv)
    var_rows = [r for r in old_rows if r["scenario"] == "variable_file_sizes"]

    # read new normal/slow/failed rows
    new_rows = read_rows(new_csv)

    # combine
    all_rows = new_rows + var_rows

    if not all_rows:
        return

    # write back all_repeated
    fields = list(all_rows[0].keys())
    with open(out_all, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)

    # compute summary
    from collections import defaultdict
    import numpy as np

    groups = defaultdict(list)
    for r in all_rows:
        key = (r["method"], r["scenario"], r["n"], r["k"], r["m"], r["file_path"], r["file_bytes"])
        groups[key].append(r)

    summary_rows = []
    for key, group in groups.items():
        times = [float(r["distribution_time_s"]) for r in group]
        costs = [float(r["distribution_cost_units"]) for r in group]
        c2e = [int(r["cloud_to_edge_bytes"]) for r in group]
        e2e = [int(r["edge_to_edge_bytes"]) for r in group]

        summary_rows.append({
            "method": key[0],
            "scenario": key[1],
            "n": key[2],
            "k": key[3],
            "m": key[4],
            "file_path": key[5],
            "file_bytes": key[6],
            "mean_time_s": np.mean(times),
            "std_time_s": np.std(times),
            "mean_cost": np.mean(costs),
            "std_cost": np.std(costs),
            "mean_c2e_bytes": np.mean(c2e),
            "mean_e2e_bytes": np.mean(e2e),
            "n_runs": len(group),
        })

    sum_fields = list(summary_rows[0].keys())
    with open(out_summary, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=sum_fields)
        w.writeheader()
        w.writerows(summary_rows)

    print("Merged successfully!")

if __name__ == "__main__":
    main()
