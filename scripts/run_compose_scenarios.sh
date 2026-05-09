#!/usr/bin/env bash
set -euo pipefail

# Runs 4 evaluation scenarios via docker compose and writes per-run CSVs.
# Output directories:
#   ./metrics/<method>/<scenario>/*
#   ./results/compose/*.csv

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# Ensure module-style execution (python -m experiments.* / common.*) can always
# resolve project packages no matter which shell/venv launches this script.
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "ERROR: neither python3 nor python found in PATH"
  exit 1
fi

N="${N:-4}"
K="${K:-3}"
M="${M:-4}"
FILE="${FILE:-experiments/data/raw/video_1MiB.bin}"
HOST_INPUT_DIR="${HOST_INPUT_DIR:-/tmp/edgehydra_inputs}"

mkdir -p metrics/edgehydra metrics/edgedis
# Always start from clean compose-result CSVs to avoid stale mixes.
rm -rf results/compose
mkdir -p results/compose
mkdir -p "$HOST_INPUT_DIR"

ts_ms() {
  "$PYTHON_BIN" - <<'PY'
import time
print(int(time.time()*1000))
PY
}

clear_fault_env() {
  for i in 0 1 2 3; do
    unset "EDGE${i}_E2E_FAULT_ENABLED" || true
    unset "EDGE${i}_E2E_FAULT_DELAY_MIN_MS" || true
    unset "EDGE${i}_E2E_FAULT_DELAY_MAX_MS" || true
    unset "EDGE${i}_E2E_FAULT_DROP_PROB" || true
    unset "EDGE${i}_E2E_FAULT_CRASH_AFTER_S" || true
  done
}

set_slow_profile() {
  # Paper-like scenario: 3 slow servers (e1,e2,e3), e0 normal.
  clear_fault_env
  for i in 1 2 3; do
    export "EDGE${i}_E2E_FAULT_ENABLED=1"
    export "EDGE${i}_E2E_FAULT_DELAY_MIN_MS=20"
    export "EDGE${i}_E2E_FAULT_DELAY_MAX_MS=40"
    export "EDGE${i}_E2E_FAULT_DROP_PROB=0"
    export "EDGE${i}_E2E_FAULT_CRASH_AFTER_S=-1"
  done
}

set_failed_profile() {
  # Paper-like scenario: 1 crashed server (default: e1), others normal.
  clear_fault_env
  local crash_idx="${1:-1}"
  export "EDGE${crash_idx}_E2E_FAULT_ENABLED=1"
  export "EDGE${crash_idx}_E2E_FAULT_CRASH_AFTER_S=1"
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
  local base
  base="$(basename "$FILE")"
  mkdir -p "$jsonl_dir"
  # Hard-isolate each run: remove stale telemetry files before compose up.
  rm -f "$jsonl_dir"/*.jsonl "$jsonl_dir"/*.json "$jsonl_dir"/netmon_*.json "$jsonl_dir"/node_state_*.json || true

  # Copy input file to Linux filesystem path to avoid WSL/OneDrive Errno 22
  # when containers read large files from /mnt/c bind-mount.
  # If source cannot be read, regenerate by filename pattern video_<MiB>MiB.bin.
  if ! cp -f "$FILE" "$HOST_INPUT_DIR/$base"; then
    if [[ "$base" =~ ^video_([0-9]+)MiB\.bin$ ]]; then
      local size_mib="${BASH_REMATCH[1]}"
      echo "WARN: cannot read $FILE; regenerating $base (${size_mib}MiB) in $HOST_INPUT_DIR"
      "$PYTHON_BIN" -m common.fake_video one --out "$HOST_INPUT_DIR/$base" --size-mib "$size_mib"
    else
      echo "ERROR: cannot read input file and cannot infer size from name: $FILE"
      exit 1
    fi
  fi
  export SCENARIO="$scenario"
  export FILE_PATH="/inputs/$base"
  export HOST_INPUT_DIR="$HOST_INPUT_DIR"

  if [[ "$method" == "edgehydra" ]]; then
    docker compose up -d --build edge0 edge1 edge2 edge3
    docker compose run --rm cloud
  else
    docker compose up -d --build edgedis0 edgedis1 edgedis2 edgedis3
    docker compose run --rm edgedis_cloud
  fi

  "$PYTHON_BIN" -m experiments.aggregate_jsonl_metrics \
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
clear_fault_env

# Normal: just run with default compose env; we route metrics by moving after run.
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"
run_one "normal" "edgehydra" "metrics/edgehydra/normal" "results/compose/edgehydra_normal.csv"
run_one "normal" "edgedis" "metrics/edgedis/normal" "results/compose/edgedis_normal.csv"

echo "== Slow servers (simulate E2E delay on 3 edges) =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

set_slow_profile
run_one "slow_servers" "edgehydra" "metrics/edgehydra/slow_servers" "results/compose/edgehydra_slow.csv"
run_one "slow_servers" "edgedis" "metrics/edgedis/slow_servers" "results/compose/edgedis_slow.csv"
clear_fault_env

echo "== Failed servers (simulate crash after 1s) =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

set_failed_profile 1
run_one "failed_servers" "edgehydra" "metrics/edgehydra/failed_servers" "results/compose/edgehydra_failed.csv"
run_one "failed_servers" "edgedis" "metrics/edgedis/failed_servers" "results/compose/edgedis_failed.csv"
clear_fault_env

echo "== Variable file sizes =="
docker compose down -v
clean_metrics_dir "metrics/edgehydra"
clean_metrics_dir "metrics/edgedis"

for fp in experiments/data/raw/video_*MiB.bin; do
  [[ -f "$fp" ]] || continue
  FILE="$fp"
  clear_fault_env
  clean_metrics_dir "metrics/edgehydra"
  clean_metrics_dir "metrics/edgedis"
  run_one "variable_file_sizes" "edgehydra" "metrics/edgehydra/variable_file_sizes" "results/compose/edgehydra_$(basename "$fp").csv"
  run_one "variable_file_sizes" "edgedis" "metrics/edgedis/variable_file_sizes" "results/compose/edgedis_$(basename "$fp").csv"
  docker compose down -v
done

echo "Done. CSVs in results/compose/ ."

