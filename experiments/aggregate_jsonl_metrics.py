import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from common.metrics import Metrics


@dataclass(frozen=True)
class RunSpec:
    scenario: str
    method: str  # "edgehydra" | "edgedis"
    n: int
    k: Optional[int]
    m: Optional[int]
    file_path: str


def _sum_events(paths: Iterable[Path]) -> Dict[str, int]:
    out = {"cloud_to_edge": 0, "edge_to_edge": 0}
    for p in paths:
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            try:
                evt = json.loads(line)
                d = evt.get("direction")
                b = int(evt.get("bytes", 0))
                if d in out:
                    out[d] += b
            except Exception:
                continue
    return out


def _event_time_range_ms(paths: Iterable[Path]) -> Optional[Dict[str, int]]:
    mn: Optional[int] = None
    mx: Optional[int] = None
    for p in paths:
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            try:
                evt = json.loads(line)
                ts = int(evt.get("ts_ms", 0))
                if ts <= 0:
                    continue
                mn = ts if mn is None else min(mn, ts)
                mx = ts if mx is None else max(mx, ts)
            except Exception:
                continue
    if mn is None or mx is None:
        return None
    return {"min": mn, "max": mx}


def _write_csv(out_path: str, rows: List[Metrics]) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
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
            ],
        )
        w.writeheader()
        for r in rows:
            d = r.to_json()
            w.writerow({k: d.get(k) for k in w.fieldnames})


def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate JSONL metrics + write CSV")
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--method", required=True, choices=["edgehydra", "edgedis"])
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--k", type=int, default=0)
    ap.add_argument("--m", type=int, default=0)
    ap.add_argument("--file", required=True)
    ap.add_argument("--jsonl-dir", required=True, help="Directory containing cloud/e*.jsonl")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    k = args.k or None
    m = args.m or None

    spec = RunSpec(
        scenario=args.scenario,
        method=args.method,
        n=args.n,
        k=k,
        m=m,
        file_path=args.file,
    )

    jsonl_dir = Path(args.jsonl_dir)
    # Sum everything in the directory (cloud + all edges).
    jsonls = sorted(jsonl_dir.glob("*.jsonl"))
    sums = _sum_events(jsonls)
    tr = _event_time_range_ms(jsonls)

    fp = Path(spec.file_path)
    metrics = Metrics(
        scenario=spec.scenario,
        method=spec.method,
        n=spec.n,
        k=spec.k,
        m=spec.m,
        file_path=spec.file_path,
        file_bytes=fp.stat().st_size if fp.exists() else 0,
    )
    # IMPORTANT: compose startup time can dominate. Use metric event timestamps.
    if tr is not None:
        metrics.started_at_ms = int(tr["min"])
        metrics.ended_at_ms = int(tr["max"])
    metrics.cloud_to_edge_bytes = sums["cloud_to_edge"]
    metrics.edge_to_edge_bytes = sums["edge_to_edge"]

    _write_csv(args.out, [metrics])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

