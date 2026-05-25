import argparse
import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List

from experiments.summarize_metrics import summarize


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


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FILE = ROOT / "experiments" / "data" / "raw" / "video_1MiB.bin"
DEFAULT_INPUT_DIR = Path("/tmp/edgehydra_inputs")


def _run(cmd: List[str], env: Dict[str, str]) -> None:
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")


def _compose_env(base_env: Dict[str, str]) -> Dict[str, str]:
    env = os.environ.copy()
    env.update(base_env)
    return env


def _clear_fault_env(env: Dict[str, str]) -> None:
    for i in range(4):
        env[f"EDGE{i}_E2E_FAULT_ENABLED"] = "0"
        env[f"EDGE{i}_E2E_FAULT_DELAY_MIN_MS"] = "0"
        env[f"EDGE{i}_E2E_FAULT_DELAY_MAX_MS"] = "0"
        env[f"EDGE{i}_E2E_FAULT_DROP_PROB"] = "0"
        env[f"EDGE{i}_E2E_FAULT_CRASH_AFTER_S"] = "-1"


def _fault_env_for_scenario(scenario: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    _clear_fault_env(env)
    if scenario == "slow_servers":
        for i in (1, 2, 3):
            env[f"EDGE{i}_E2E_FAULT_ENABLED"] = "1"
            env[f"EDGE{i}_E2E_FAULT_DELAY_MIN_MS"] = "20"
            env[f"EDGE{i}_E2E_FAULT_DELAY_MAX_MS"] = "40"
            env[f"EDGE{i}_E2E_FAULT_DROP_PROB"] = "0"
    elif scenario == "failed_servers":
        env["EDGE1_E2E_FAULT_ENABLED"] = "1"
        env["EDGE1_E2E_FAULT_CRASH_AFTER_S"] = "1"
    return env


def _read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})


def _files_for_scenario(scenario: str) -> List[Path]:
    if scenario != "variable_file_sizes":
        # Use 24MiB for fault scenarios to clearly show performance drops
        return [ROOT / "experiments" / "data" / "raw" / "video_24MiB.bin"]
    raws = sorted((ROOT / "experiments" / "data" / "raw").glob("video_*MiB.bin"))
    return [p for p in raws if p.is_file()]


def _run_one_method(
    *,
    scenario: str,
    method: str,
    file_path: Path,
    jsonl_dir: Path,
    out_csv: Path,
    n: int,
    k: int,
    m: int,
    env: Dict[str, str],
) -> None:
    DEFAULT_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    base_name = file_path.name
    target_input = DEFAULT_INPUT_DIR / base_name
    if not file_path.exists():
        raise FileNotFoundError(f"input file not found: {file_path}")
    shutil.copyfile(file_path, target_input)

    run_env = _compose_env(env)
    run_env["SCENARIO"] = scenario
    run_env["HOST_INPUT_DIR"] = str(DEFAULT_INPUT_DIR)
    run_env["FILE_PATH"] = f"/inputs/{base_name}"

    _run(["docker", "compose", "down", "-v"], run_env)
    if method == "edgehydra":
        _run(["docker", "compose", "up", "-d", "--build", "edge0", "edge1", "edge2", "edge3"], run_env)
        _run(["docker", "compose", "run", "--rm", "cloud"], run_env)
        use_k, use_m = k, m
    else:
        _run(["docker", "compose", "up", "-d", "--build", "edgedis0", "edgedis1", "edgedis2", "edgedis3"], run_env)
        _run(["docker", "compose", "run", "--rm", "edgedis_cloud"], run_env)
        use_k, use_m = 0, 0

    _run(
        [
            sys.executable,
            "-m",
            "experiments.aggregate_jsonl_metrics",
            "--scenario",
            scenario,
            "--method",
            method,
            "--n",
            str(n),
            "--k",
            str(use_k),
            "--m",
            str(use_m),
            "--file",
            str(file_path),
            "--jsonl-dir",
            str(jsonl_dir),
            "--out",
            str(out_csv),
        ],
        run_env,
    )


def run_repeated(scenarios: List[str], runs: int, out_dir: Path, n: int, k: int, m: int) -> None:
    tmp_dir = out_dir / ".tmp_repeated"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for run_idx in range(1, runs + 1):
        for scenario in scenarios:
            rows: List[Dict[str, str]] = []
            fault_env = _fault_env_for_scenario(scenario)
            for file_path in _files_for_scenario(scenario):
                for method in ("edgehydra", "edgedis"):
                    jsonl_dir = ROOT / "metrics" / method / scenario
                    jsonl_dir.mkdir(parents=True, exist_ok=True)
                    # Hard-isolate each run/method so bytes/time do not accumulate
                    # across repeated iterations.
                    for pat in ("*.jsonl", "*.json", "netmon_*.json", "node_state_*.json"):
                        for old in jsonl_dir.glob(pat):
                            old.unlink(missing_ok=True)
                    one_csv = tmp_dir / f"one_{run_idx}_{scenario}_{method}_{file_path.name}.csv"
                    _run_one_method(
                        scenario=scenario,
                        method=method,
                        file_path=file_path,
                        jsonl_dir=jsonl_dir,
                        out_csv=one_csv,
                        n=n,
                        k=k,
                        m=m,
                        env=fault_env,
                    )
                    rows.extend(_read_rows(one_csv))
            run_csv = out_dir / f"run_{run_idx}_{scenario}.csv"
            _write_rows(run_csv, rows)
            print(f"Wrote {run_csv} ({len(rows)} rows)")

    # Aggregate logic: merge all run-level rows and write summary.
    run_csvs = sorted(out_dir.glob("run_*.csv"))
    all_rows: List[Dict[str, str]] = []
    for p in run_csvs:
        all_rows.extend(_read_rows(p))
    merged_csv = out_dir / "metrics_all_repeated.csv"
    _write_rows(merged_csv, all_rows)
    print(f"Wrote merged repeated metrics: {merged_csv} ({len(all_rows)} rows)")

    import pandas as pd

    raw_df = pd.read_csv(merged_csv)
    summary_df = summarize(raw_df)
    summary_csv = out_dir / "metrics_summary.csv"
    summary_df.to_csv(summary_csv, index=False)
    print(f"Wrote summary metrics: {summary_csv} ({len(summary_df)} rows)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run compose scenarios repeatedly and summarize results")
    ap.add_argument(
        "--scenarios",
        default="normal,slow_servers,failed_servers,variable_file_sizes",
        help="Comma-separated scenarios",
    )
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--out-dir", default="results/compose")
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--k", type=int, default=3)
    ap.add_argument("--m", type=int, default=4)
    args = ap.parse_args()

    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    allowed = {"normal", "slow_servers", "failed_servers", "variable_file_sizes"}
    unknown = [s for s in scenarios if s not in allowed]
    if unknown:
        raise SystemExit(f"Unknown scenario(s): {unknown}")
    if args.runs <= 0:
        raise SystemExit("--runs must be > 0")

    out_dir = (ROOT / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_repeated(scenarios, args.runs, out_dir, args.n, args.k, args.m)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
