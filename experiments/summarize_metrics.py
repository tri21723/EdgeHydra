import argparse
import re
from pathlib import Path

import pandas as pd


# Repeated compose writes run_<run_index>_<scenario>.csv (not *_metrics.csv).
RUN_METRICS_CSV_RE = re.compile(r"^run_(\d+)_[^.]+\.csv$")

# Never ingest merged outputs / summaries even if naming overlaps.
_METRICS_MERGED_NAMES = frozenset(
    {
        "metrics_all.csv",
        "metrics_all_repeated.csv",
        "metrics_summary.csv",
    }
)


def remove_outliers_iqr(series: pd.Series) -> pd.Series:
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return series[(series >= lower) & (series <= upper)]


def _maybe_drop_ratio_spike(times: pd.Series, candidate_idx: pd.Index) -> tuple[pd.Index, list[float]]:
    """
    When Tukey IQR keeps all points (common with n=3), drop the largest sample once only if it is a
    clear multiplicative spike versus the next-largest value (peak > spike_factor * rest_max).
    This targets system-wide timing blips without flagging ordinary n=3 variance.
    """
    spike_factor = 3.0
    vals = times.loc[candidate_idx].dropna()
    if len(vals) < 3:
        return candidate_idx, []
    sorted_idx = vals.sort_values().index.to_numpy()
    largest_i = sorted_idx[-1]
    rest_idx = sorted_idx[:-1]
    peak = float(times.loc[largest_i])
    rest_max = float(times.loc[pd.Index(rest_idx)].max())
    if rest_max <= 0.0:
        return candidate_idx, []
    if peak > spike_factor * rest_max:
        return pd.Index(rest_idx.tolist()), [peak]
    return candidate_idx, []


def _run_id_from_csv_path(p: Path) -> int:
    m = RUN_METRICS_CSV_RE.match(p.name)
    if not m:
        raise ValueError(f"filename does not match run_<id>_<scenario>.csv: {p.name}")
    return int(m.group(1))


def _read_input_csvs(in_dir: Path) -> pd.DataFrame:
    csvs = sorted(
        p
        for p in in_dir.glob("*.csv")
        if p.name not in _METRICS_MERGED_NAMES and RUN_METRICS_CSV_RE.match(p.name)
    )
    if not csvs:
        return pd.DataFrame()
    frames = []
    for p in csvs:
        df = pd.read_csv(p)
        run_id = _run_id_from_csv_path(p)
        df["run_id"] = run_id
        if "run_csv" not in df.columns:
            df["run_csv"] = p.name
        frames.append(df)
    merged = pd.concat(frames, ignore_index=True)
    dup_subset = ["scenario", "method", "file_bytes", "run_id"]
    dup_mask = merged.duplicated(subset=dup_subset, keep=False)
    if dup_mask.any():
        n_dup = int(dup_mask.sum())
        raise AssertionError(
            f"duplicate ({', '.join(dup_subset)}) rows after loading ({n_dup} rows involved); "
            "check inputs are not merged twice."
        )
    return merged


def _to_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["distribution_time_s"] = _to_numeric(work, "distribution_time_s")
    work["distribution_cost_units"] = _to_numeric(work, "distribution_cost_units")
    work["file_bytes"] = pd.to_numeric(work.get("file_bytes"), errors="coerce").fillna(0).astype(int)

    group_cols = ["method", "scenario", "file_bytes"]

    rows_out = []
    for key_tuple, group in work.groupby(group_cols, dropna=False):
        method, scenario, file_bytes = key_tuple
        g = group.copy()
        times = g["distribution_time_s"]
        valid_time = times.notna()
        # Samples that contribute to timing stats before outlier removal (matches prior `.agg(..., count)`).
        n_runs = int(valid_time.sum())

        if n_runs == 0:
            filtered_idx = g.index[:0]
            outliers_removed = 0
            removed_times: list[float] = []
        elif n_runs <= 2:
            filtered_idx = g.index[valid_time]
            outliers_removed = 0
            removed_times = []
        else:
            t_valid = times[valid_time]
            filt = remove_outliers_iqr(t_valid)
            if len(filt) == 0:
                filt = t_valid
            filtered_idx = filt.index
            kept_idx_set = set(filtered_idx.tolist())
            dropped_idx = [i for i in g.index[valid_time].tolist() if i not in kept_idx_set]
            outliers_removed = len(dropped_idx)
            removed_times = sorted(times.loc[dropped_idx].tolist())

            removal_modes: list[str] = []
            if outliers_removed > 0:
                removal_modes.append("IQR")

            # Tukey fences often retain single spikes when n≈3; catch multiplicative spikes afterward.
            if outliers_removed == 0 and n_runs >= 3:
                filtered_idx2, spike_times = _maybe_drop_ratio_spike(times, filtered_idx)
                if spike_times:
                    filtered_idx = filtered_idx2
                    outliers_removed = len(spike_times)
                    removed_times = sorted(spike_times)
                    removal_modes.append("ratio_vs_rest_max")

            if outliers_removed > 0:
                modes = "+".join(removal_modes)
                print(
                    "WARNING: distribution_time_s outlier removal "
                    f"({modes}) dropped {outliers_removed} sample(s) for "
                    f"scenario={scenario!r} method={method!r} file_bytes={file_bytes}: "
                    f"removed values={removed_times}"
                )

        n_after_outlier_removal = int(len(filtered_idx))

        sub = g.loc[filtered_idx]
        t_sub = sub["distribution_time_s"]
        c_sub = sub["distribution_cost_units"]

        rows_out.append(
            {
                "method": method,
                "scenario": scenario,
                "file_bytes": file_bytes,
                "mean_time_s": float(t_sub.mean()) if len(t_sub) else float("nan"),
                "std_time_s": float(t_sub.std(ddof=1)) if len(t_sub) > 1 else 0.0,
                "min_time_s": float(t_sub.min()) if len(t_sub) else float("nan"),
                "max_time_s": float(t_sub.max()) if len(t_sub) else float("nan"),
                "mean_cost": float(c_sub.mean()) if len(c_sub) else float("nan"),
                "std_cost": float(c_sub.std(ddof=1)) if len(c_sub) > 1 else 0.0,
                "min_cost": float(c_sub.min()) if len(c_sub) else float("nan"),
                "max_cost": float(c_sub.max()) if len(c_sub) else float("nan"),
                "n_runs": n_runs,
                "n_after_outlier_removal": n_after_outlier_removal,
                "outliers_removed": outliers_removed,
                # Back-compat for scripts expecting `runs` == samples used for aggregates.
                "runs": n_after_outlier_removal,
            }
        )

    out = pd.DataFrame(rows_out)
    out["std_time_s"] = out["std_time_s"].fillna(0.0)
    out["std_cost"] = out["std_cost"].fillna(0.0)
    return out.sort_values(["scenario", "method", "file_bytes"]).reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize repeated-run metrics CSV files")
    ap.add_argument("--in-dir", default="results/compose", help="Directory containing run_<id>_*.csv")
    ap.add_argument("--out", default="results/compose/metrics_summary.csv")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw = _read_input_csvs(in_dir)
    if raw.empty:
        print(f"No run_<id>_*.csv repeated-run files found in {in_dir} (metrics_all*.csv excluded)")
        return 1
    summary = summarize(raw)
    summary.to_csv(out_path, index=False)
    print(f"Wrote summary: {out_path} ({len(summary)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
