# EdgeHydra — Fault-tolerant edge data distribution (IS211)

**Đồ án môn:** *Cơ sở dữ liệu phân tán (IS211)* — so sánh hai giao thức phân phối dữ liệu tại edge: **EdgeHydra** (mã hóa xóa + lịch thích ứng) và **EdgeDis** (cắt lát đơn giản).

This repository implements a **research-style simulation** of edge file distribution using **Python/asyncio**, optional **Docker Compose** multi-container runs, and a small **FastAPI** dashboard. Metrics are emitted as **JSONL** and summarized to **CSV**.

---

## Contents

- [Overview](#overview)
- [Repository layout](#repository-layout)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start — local simulation](#quick-start--local-simulation)
- [Docker Compose — EdgeHydra \& EdgeDis](#docker-compose--edgehydra--edgedis)
- [Automated evaluation (4 scenarios)](#automated-evaluation-4-scenarios)
- [Repeated runs \& summarization](#repeated-runs--summarization)
- [Metrics \& validation](#metrics--validation)
- [Web dashboard](#web-dashboard)
- [Fault injection (environment variables)](#fault-injection-environment-variables)
- [Documentation](#documentation)
- [License \& course use](#license--course-use)

---

## Overview

| Component | Role |
|-----------|------|
| **EdgeHydra** | Erasure coding (**zfec**, typically **k = 3**, **m = 4**), TCP transfers, UDP heartbeats, leaderless supplement; adaptive peer ranking (RTT / EWMA) for forwarding. |
| **EdgeDis** | Baseline: naive slicing to *n* edges, no erasure recovery. |
| **Cloud** | Stage-1 distributor: encodes (Hydra) or slices (Dis) and pushes to edges. |
| **Experiments** | Local runner, Compose automation, JSONL aggregation → CSV, merge/summarize helpers. |
| **Webapp** | Trigger compose pipeline and inspect `metrics_all.csv` (charts/tables). |

**Scenarios supported in scripts:** `normal`, `slow_servers`, `failed_servers`, `variable_file_sizes` (multiple `video_*MiB.bin` inputs).

---

## Repository layout

```
.
├── cloud/                 # Cloud distributors (EdgeHydra + EdgeDis clients)
├── edge/                  # Edge servers (Hydra + EdgeDis nodes)
├── common/                # Erasure coding, transport, metrics, fault simulator, etc.
├── experiments/           # Runners, aggregation, merge, validation, repeated compose
├── webapp/                # FastAPI UI
├── scripts/
│   └── run_compose_scenarios.sh   # Single-pass 4-scenario Compose benchmark
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── PROJECT_STATUS_AND_NEXT_STEPS.md   # Detailed handover / roadmap
└── README.md
```

**Generated paths (not tracked by default — see `.gitignore`):**

- `metrics/` — JSONL and related telemetry from Compose runs  
- `results/` — CSV outputs, optional `results/report/*.md` experiment write-ups  
- `experiments/data/recovered*`, `edge_cache/`, `raw/*.bin` samples, etc.

Clone the repo and **re-run experiments** to regenerate artifacts, or remove `results/` / `metrics/` from `.gitignore` if your course requires committing them.

---

## Requirements

- **Python 3.10+** (3.11/3.12 recommended)
- **Docker** and **Docker Compose** plugin (for multi-container benchmarks)
- **Linux** or **WSL2** recommended for Compose volume behaviour; large binds from synced folders (e.g. OneDrive) can be slow — scripts may copy inputs to `/tmp/edgehydra_inputs`

---

## Installation

```bash
git clone <your-fork-or-repo-url>.git
cd Project

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

Set `PYTHONPATH` to the repo root when running modules (Compose scripts export it automatically):

```bash
export PYTHONPATH="$(pwd)"
```

---

## Quick start — local simulation

Runs EdgeHydra/EdgeDis as **local processes** (no Docker), useful for development:

```bash
python -m experiments.run_experiments \
  --n 4 \
  --k 3 \
  --file experiments/data/raw/video_1MiB.bin \
  --out /tmp/metrics.csv

head /tmp/metrics.csv
```

> Note: synthetic `video_*.bin` assets may need to be generated (see `common.fake_video`, `experiments/prepare_dataset.py`) if not present locally.

---

## Docker Compose — EdgeHydra & EdgeDis

### EdgeHydra (4 edges + cloud job)

```bash
docker compose up -d --build edge0 edge1 edge2 edge3
docker compose run --rm cloud
# Recovered payloads (host paths depend on compose volume mounts):
ls -la experiments/data/recovered
docker compose down -v
```

### EdgeDis baseline

```bash
docker compose up -d --build edgedis0 edgedis1 edgedis2 edgedis3
docker compose run --rm edgedis_cloud
ls -la experiments/data/recovered_edgedis
docker compose down -v
```

Compose writes metric JSONL under **`./metrics/`** on the host (by service/scenario routing in your env — see automation scripts).

---

## Automated evaluation (4 scenarios)

**Single sweep** — runs all scenarios and writes **per-run CSVs** under `results/compose/` (requires Docker):

```bash
chmod +x scripts/run_compose_scenarios.sh
./scripts/run_compose_scenarios.sh
ls results/compose
```

**Merge** single-compose artifacts only (`edgehydra_*.csv`, `edgedis_*..csv`, …) into one table (**does not** merge `run_*.csv` or prior merged summaries):

```bash
python -m experiments.merge_compose_csvs \
  --in-dir results/compose \
  --out results/compose/metrics_all.csv
```

**Validate** merged **data** CSV (expects full scenario grid, not the summary statistics file):

```bash
python3 -m experiments.validate_metrics --csv results/compose/metrics_all.csv
```

---

## Repeated runs & summarization

**Three (or more) independent repeats**, one CSV per run index and scenario, plus merged + summary outputs:

```bash
python3 -m experiments.run_repeated_compose \
  --runs 3 \
  --out-dir results/compose \
  --scenarios normal,slow_servers,failed_servers,variable_file_sizes
```

Outputs typically include:

- `run_<i>_<scenario>.csv`
- `metrics_all_repeated.csv`
- `metrics_summary.csv` (written at end of script via `pandas`)

To regenerate the **summary** with the standalone pipeline (recommended for consistent outlier/IQR behaviour):

```bash
python3 -m experiments.summarize_metrics \
  --in-dir results/compose \
  --out results/compose/metrics_summary.csv

python3 -m experiments.validate_metrics --csv results/compose/metrics_summary.csv
```

`validate_metrics` detects summary vs raw CSVs via column schema.

---

## Metrics & validation

- **`distribution_time_s`**: derived in `experiments/aggregate_jsonl_metrics.py` from **min/max `ts_ms`** across JSONL events (avoids swallowing Compose startup noise).
- **`distribution_cost_units`**: from `common/metrics.py` — cloud-to-edge MiB × **1.0** plus edge-to-edge MiB × **(1/9)** (cost model documented in code).

Helpers:

| Module | Purpose |
|--------|---------|
| `experiments/aggregate_jsonl_metrics.py` | Sum directions from JSONL, write one-row CSV |
| `experiments/merge_compose_csvs.py` | Merge baseline compose CSVs; adds comparison columns |
| `experiments/summarize_metrics.py` | Aggregate repeated `run_*.csv` → mean/std/outlier-aware summary |
| `experiments/validate_metrics.py` | Sanity checks on merged or summary CSV |

---

## Web dashboard

```bash
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn webapp.main:app --host 0.0.0.0 --port 8080 --reload
```

Open **[http://localhost:8080](http://localhost:8080)** — uses `results/compose/metrics_all.csv` by default (generate via script + merge first).

---

## Fault injection (environment variables)

Used by Compose / edges (see `common/fault_simulator.py`, `experiments/run_repeated_compose.py`, bash script profiles). Examples:

| Pattern | Meaning |
|---------|---------|
| `EDGE{i}_E2E_FAULT_ENABLED` | Enable fault path on edge *i* |
| `EDGE{i}_E2E_FAULT_DELAY_MIN_MS` / `_MAX_MS` | Random delay bracket |
| `EDGE{i}_E2E_FAULT_DROP_PROB` | Drop probability |
| `EDGE{i}_E2E_FAULT_CRASH_AFTER_S` | Crash after N seconds |

Exact semantics are documented in source and `PROJECT_STATUS_AND_NEXT_STEPS.md`.

---

## Documentation

| File | Content |
|------|---------|
| `PROJECT_STATUS_AND_NEXT_STEPS.md` | Implementation vs paper, validated behaviour, backlog |
| `PROJECT_REVIEW_ROADMAP_AND_IMPROVEMENTS.md` | Review notes / improvements |
| `results/report/experiment_report.md` | *(Generated locally)* Full experiment narrative + tables |
| `results/report/report_tables_only.md` | *(Generated locally)* Tables only for slides |

Reports under `results/` are **ignored by git** unless you commit them intentionally.

---

## License & course use

This project is maintained for **course / academic use** at the authors’ discretion. Add a `LICENSE` file if you open-source publicly.

**Suggested citation (adapt to your instructor’s format):**  
*EdgeHydra — IS211 Distributed Databases course project,* [GitHub repository URL].

---

### Acknowledgements

- **zfec** for erasure coding (`requirements.txt`).
- Compose-based evaluation pattern inspired by containerized edge testbeds.

Pull requests welcome for typo fixes and clearer reproduction steps; breaking protocol changes should stay aligned with the course specification.
