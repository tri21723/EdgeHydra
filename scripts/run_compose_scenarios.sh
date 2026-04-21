#!/usr/bin/env bash
set -euo pipefail

# Runs 4 evaluation scenarios via docker compose and writes per-run CSVs.
# Output directories:
#   ./metrics/<method>/<scenario>/*
#   ./results/compose/*.csv

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

N="${N:-4}"
K="${K:-3}"
M="${M:-4}"
FILE="${FILE:-experiments/data/raw/video_1MiB.bin}"

mkdir -p metrics/edgehydra metrics/edgedis results/compose

ts_ms() {
  python - <<'PY'
import time
print(int(time.time()*1000))
PY
}

clean_metrics_dir() {
  local dir="$1"
  rm -rf "$dir"
  mkdir -p "$dir"
}

run_one() {
  local scenario="$1"
  local method="$2" # edgehydra|edgedis
  local jsonl_dir="$3"
  local out_csv="$4"
  export SCENARIO="$scenario"
  export FILE_PATH="$FILE"

  if [[ "$method" == "edgehydra" ]]; then
    docker compose up -d --build edge0 edge1 edge2 edge3
    docker compose run --rm cloud
  else
    docker compose up -d --build edgedis0 edgedis1 edgedis2 edgedis3
    docker compose run --rm edgedis_cloud
  fi

  python -m experiments.aggregate_jsonl_metrics \
    --scenario "$scenario" \
    --method "$method" \
    --n "$N" \
    --k "$K" \
    --m "$M" \
    --file "$FILE" \
    --jsonl-dir "$jsonl_dir" \
    --out "$out_csv"
}

echo "== Normal =="
docker compose down -v >/dev/null 2>&1 || true

# Normal: just run with default compose env; we route metrics by moving after run.
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"
run_one "normal" "edgehydra" "metrics/edgehydra/normal" "results/compose/edgehydra_normal.csv"
run_one "normal" "edgedis" "metrics/edgedis/normal" "results/compose/edgedis_normal.csv"

echo "== Slow servers (simulate E2E delay on 3 edges) =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

# Apply env vars for slow edges by exporting before starting edges.
export E2E_FAULT_ENABLED=1
export E2E_FAULT_DELAY_MIN_MS=20
export E2E_FAULT_DELAY_MAX_MS=40
export E2E_FAULT_DROP_PROB=0
run_one "slow_servers" "edgehydra" "metrics/edgehydra/slow_servers" "results/compose/edgehydra_slow.csv"
run_one "slow_servers" "edgedis" "metrics/edgedis/slow_servers" "results/compose/edgedis_slow.csv"
unset E2E_FAULT_ENABLED E2E_FAULT_DELAY_MIN_MS E2E_FAULT_DELAY_MAX_MS E2E_FAULT_DROP_PROB

echo "== Failed servers (simulate crash after 1s) =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

export E2E_FAULT_ENABLED=1
export E2E_FAULT_CRASH_AFTER_S=1
run_one "failed_servers" "edgehydra" "metrics/edgehydra/failed_servers" "results/compose/edgehydra_failed.csv"
run_one "failed_servers" "edgedis" "metrics/edgedis/failed_servers" "results/compose/edgedis_failed.csv"
unset E2E_FAULT_ENABLED E2E_FAULT_CRASH_AFTER_S

echo "== Variable file sizes =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

for fp in experiments/data/raw/video_*MiB.bin; do
  [[ -f "$fp" ]] || continue
  FILE="$fp"
  clean_metrics_dir "metrics/edgehydra"
  clean_metrics_dir "metrics/edgedis"
  run_one "variable_file_sizes" "edgehydra" "metrics/edgehydra/variable_file_sizes" "results/compose/edgehydra_$(basename "$fp").csv"
  run_one "variable_file_sizes" "edgedis" "metrics/edgedis/variable_file_sizes" "results/compose/edgedis_$(basename "$fp").csv"
  docker compose down -v
done

echo "Done. CSVs in results/compose/ ."

