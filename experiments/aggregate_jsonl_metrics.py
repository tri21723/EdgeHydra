import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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


def _event_label(evt: Dict[str, Any]) -> str:
    typ = str(evt.get("type", "")).strip()
    direction = str(evt.get("direction", "")).strip()
    if typ:
        return typ
    if direction:
        return direction
    return "unknown"


def _collect_timed_events(paths: Iterable[Path]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in paths:
        if not p.exists():
            continue
        for i, line in enumerate(p.read_text().splitlines(), start=1):
            try:
                evt = json.loads(line)
                ts = int(evt.get("ts_ms", 0))
                if ts <= 0:
                    continue
                out.append(
                    {
                        "ts_ms": ts,
                        "file": p.name,
                        "line": i,
                        "label": _event_label(evt),
                    }
                )
            except Exception:
                continue
    out.sort(key=lambda x: int(x["ts_ms"]))
    return out


def _time_range_from_events(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not events:
        return None
    start = events[0]
    end = events[-1]
    return {
        "min": int(start["ts_ms"]),
        "max": int(end["ts_ms"]),
        "min_source_file": str(start["file"]),
        "min_source_line": int(start["line"]),
        "min_source_label": str(start["label"]),
        "max_source_file": str(end["file"]),
        "max_source_line": int(end["line"]),
        "max_source_label": str(end["label"]),
    }


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
    all_events = _collect_timed_events(jsonls)
    tr_dbg = _time_range_from_events(all_events)
    tr = None
    if tr_dbg is not None:
        tr = {"min": int(tr_dbg["min"]), "max": int(tr_dbg["max"])}

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
    # Debug logging: print event-file counts and exact start/end event sources.
    print(
        f"[aggregate_jsonl_metrics] input scenario={spec.scenario} method={spec.method} "
        f"jsonl_files={len(jsonls)}"
    )
    for p in jsonls:
        line_count = len(p.read_text().splitlines()) if p.exists() else 0
        print(f"[aggregate_jsonl_metrics] file path={p} lines={line_count}")
    print(
        f"[aggregate_jsonl_metrics] events total_events={len(all_events)} "
        f"window_events={len(all_events)}"
    )
    if tr_dbg is not None:
        dist_s = (int(tr_dbg["max"]) - int(tr_dbg["min"])) / 1000.0
        print(
            "[aggregate_jsonl_metrics] timing"
            f" t_start_ms={int(tr_dbg['min'])}"
            f" t_end_ms={int(tr_dbg['max'])}"
            f" start_from={tr_dbg['min_source_file']}:{tr_dbg['min_source_line']}:{tr_dbg['min_source_label']}"
            f" end_from={tr_dbg['max_source_file']}:{tr_dbg['max_source_line']}:{tr_dbg['max_source_label']}"
            f" distribution_time_s={dist_s:.6f}"
        )
    else:
        print("[aggregate_jsonl_metrics] timing no valid ts_ms found")
    metrics.cloud_to_edge_bytes = sums["cloud_to_edge"]
    metrics.edge_to_edge_bytes = sums["edge_to_edge"]

    _write_csv(args.out, [metrics])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

