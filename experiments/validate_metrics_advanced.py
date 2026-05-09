import argparse
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _to_float(v: str) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: str) -> Optional[int]:
    try:
        return int(float(v))
    except Exception:
        return None


def _mib(file_bytes: Optional[int]) -> str:
    if file_bytes is None:
        return "?"
    return f"{file_bytes // (1024 * 1024)}MiB"


def _avg(vals: List[Optional[float]]) -> Optional[float]:
    nums = [x for x in vals if x is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _print_ok(msg: str) -> None:
    print(f"[OK]      {msg}")


def _print_warn(msg: str) -> None:
    print(f"[WARNING] {msg}")


def _print_err(msg: str) -> None:
    print(f"[ERROR]   {msg}")


def _group_by_method_file(rows: List[Dict[str, str]]) -> Dict[Tuple[str, int], List[Dict[str, str]]]:
    out: Dict[Tuple[str, int], List[Dict[str, str]]] = {}
    for r in rows:
        method = (r.get("method") or "").strip().lower()
        fb = _to_int(r.get("file_bytes", ""))
        if not method or fb is None:
            continue
        out.setdefault((method, fb), []).append(r)
    return out


def _group_by_scenario_file(rows: List[Dict[str, str]]) -> Dict[Tuple[str, int], List[Dict[str, str]]]:
    out: Dict[Tuple[str, int], List[Dict[str, str]]] = {}
    for r in rows:
        scenario = (r.get("scenario") or "").strip().lower()
        fb = _to_int(r.get("file_bytes", ""))
        if not scenario or fb is None:
            continue
        out.setdefault((scenario, fb), []).append(r)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Advanced sanity-check for merged metrics CSV")
    ap.add_argument("--csv", default="results/compose/metrics_all.csv")
    args = ap.parse_args()

    path = Path(args.csv)
    if not path.exists():
        _print_err(f"file not found: {path}")
        return 2

    rows = _load_rows(path)
    if not rows:
        _print_err("csv has no data rows")
        return 2

    errors = 0
    warnings = 0

    # Rule 1: TIME ORDERING RULE (warning only).
    grouped_method_file = _group_by_method_file(rows)
    time_order_warns = 0
    for (method, file_bytes), mrows in sorted(grouped_method_file.items()):
        normal = _avg([_to_float(r.get("distribution_time_s", "")) for r in mrows if (r.get("scenario") or "").strip() == "normal"])
        failed = _avg(
            [_to_float(r.get("distribution_time_s", "")) for r in mrows if (r.get("scenario") or "").strip() == "failed_servers"]
        )
        slow = _avg([_to_float(r.get("distribution_time_s", "")) for r in mrows if (r.get("scenario") or "").strip() == "slow_servers"])
        file_tag = _mib(file_bytes)
        if normal is not None and failed is not None and failed < normal * 0.5:
            _print_warn(
                f"failed_servers {method} time ({failed:.3f}s) < normal ({normal:.3f}s) x 0.5 for {file_tag} — suspicious"
            )
            warnings += 1
            time_order_warns += 1
        if normal is not None and slow is not None and slow < normal * 0.8:
            _print_warn(
                f"slow_servers {method} time ({slow:.3f}s) < normal ({normal:.3f}s) x 0.8 for {file_tag} — suspicious"
            )
            warnings += 1
            time_order_warns += 1
    if time_order_warns == 0:
        _print_ok("time ordering checks look reasonable")

    # Rule 2: EDGEHYDRA C2E RATIO RULE (error).
    hydra_rows = [r for r in rows if (r.get("method") or "").strip() == "edgehydra"]
    hydra_ratio_errors = 0
    for r in hydra_rows:
        fb = _to_float(r.get("file_bytes", ""))
        c2e = _to_float(r.get("cloud_to_edge_bytes", ""))
        if fb is None or fb <= 0 or c2e is None:
            continue
        ratio = c2e / fb
        if not (1.30 <= ratio <= 1.35):
            _print_err(
                f"EdgeHydra C2E ratio out of range in {r.get('scenario','?')}/"
                f"{_mib(_to_int(r.get('file_bytes','')))}: {ratio:.4f} (expected 1.30..1.35)"
            )
            errors += 1
            hydra_ratio_errors += 1
    if hydra_ratio_errors == 0:
        _print_ok("C2E ratio correct for all EdgeHydra rows")

    # Rule 3: E2E DUPLICATION RULE (warning).
    dedup_warnings = 0
    for r in hydra_rows:
        c2e = _to_float(r.get("cloud_to_edge_bytes", ""))
        e2e = _to_float(r.get("edge_to_edge_bytes", ""))
        if c2e is None or c2e <= 0 or e2e is None:
            continue
        ratio = e2e / c2e
        if ratio > 4.0:
            _print_warn(
                f"EdgeHydra E2E/C2E ratio high in {r.get('scenario','?')}/{_mib(_to_int(r.get('file_bytes','')))}: {ratio:.2f}"
            )
            warnings += 1
            dedup_warnings += 1
            if ratio >= 5.0:
                _print_warn("deduplication may not be working (ratio >= 5.0)")
                warnings += 1
    if dedup_warnings == 0:
        _print_ok("EdgeHydra E2E duplication ratio is within target bounds")

    # Rule 4: EDGEDIS INTEGRITY RULE (error).
    edgedis_rows = [r for r in rows if (r.get("method") or "").strip() == "edgedis"]
    edgedis_errors = 0
    for r in edgedis_rows:
        fb = _to_int(r.get("file_bytes", ""))
        c2e = _to_int(r.get("cloud_to_edge_bytes", ""))
        if fb is None or c2e is None:
            continue
        if c2e != fb:
            _print_err(
                f"EdgeDis cloud_to_edge_bytes != file_bytes in {r.get('scenario','?')}/{_mib(fb)}: "
                f"c2e={c2e}, file={fb}"
            )
            errors += 1
            edgedis_errors += 1
    if edgedis_errors == 0:
        _print_ok("EdgeDis C2E integrity holds (cloud_to_edge_bytes == file_bytes)")

    # Rule 5: COST CONSISTENCY RULE (error).
    cost_errors = 0
    for r in rows:
        c2e = _to_float(r.get("cloud_to_edge_bytes", ""))
        e2e = _to_float(r.get("edge_to_edge_bytes", ""))
        cost = _to_float(r.get("distribution_cost_units", ""))
        if c2e is None or e2e is None or cost is None:
            continue
        expected = (c2e / 1048576.0) * 1.0 + (e2e / 1048576.0) * (1.0 / 9.0)
        if abs(cost - expected) > 0.01:
            _print_err(
                f"Cost formula mismatch in row: {r.get('scenario','?')}/{r.get('method','?')}/{_mib(_to_int(r.get('file_bytes','')))} "
                f"(csv={cost:.6f}, expected={expected:.6f})"
            )
            errors += 1
            cost_errors += 1
    if cost_errors == 0:
        _print_ok("cost formula consistency checks passed")

    # Rule 6: COMPARATIVE RULE (warning only) for large files in slow/failed.
    grouped_scenario_file = _group_by_scenario_file(rows)
    comparative_warns = 0
    for (scenario, file_bytes), srows in sorted(grouped_scenario_file.items()):
        if scenario not in {"slow_servers", "failed_servers"}:
            continue
        if file_bytes < 24 * 1024 * 1024:
            continue
        hydra_t = _avg(
            [_to_float(r.get("distribution_time_s", "")) for r in srows if (r.get("method") or "").strip() == "edgehydra"]
        )
        dis_t = _avg(
            [_to_float(r.get("distribution_time_s", "")) for r in srows if (r.get("method") or "").strip() == "edgedis"]
        )
        if hydra_t is None or dis_t is None:
            continue
        if hydra_t > dis_t:
            _print_warn(
                f"{scenario} {_mib(file_bytes)}: EdgeHydra time ({hydra_t:.3f}s) > EdgeDis ({dis_t:.3f}s) — expected faster"
            )
            warnings += 1
            comparative_warns += 1
    if comparative_warns == 0:
        _print_ok("comparative large-file checks passed for slow/failed scenarios")

    # Extra sanity check: variable_file_sizes time should not be unrealistically short.
    # Lower bound assumes an optimistic 500 MiB/s effective throughput.
    throughput_floor_errors = 0
    for r in rows:
        if (r.get("scenario") or "").strip() != "variable_file_sizes":
            continue
        fb = _to_float(r.get("file_bytes", ""))
        t = _to_float(r.get("distribution_time_s", ""))
        if fb is None or fb <= 0 or t is None:
            continue
        min_expected_s = fb / (500.0 * 1024.0 * 1024.0)
        if t < min_expected_s:
            _print_err(
                f"time unrealistically short in variable_file_sizes/{r.get('method','?')}/{_mib(_to_int(r.get('file_bytes','')))}: "
                f"time_s={t:.3f} < min_expected_s={min_expected_s:.3f}"
            )
            errors += 1
            throughput_floor_errors += 1
    if throughput_floor_errors == 0:
        _print_ok("variable_file_sizes minimum-time sanity checks passed")

    print("\nAdvanced validation summary:")
    print(f"- rows: {len(rows)}")
    print(f"- warnings: {warnings}")
    print(f"- errors: {errors}")

    # Requested behavior: code 1 if any ERROR, code 0 otherwise.
    return 1 if errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

