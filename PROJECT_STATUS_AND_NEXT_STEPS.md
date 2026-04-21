# EdgeHydra Project Handover (Detailed)

This document summarizes:
- what has been implemented,
- what is currently validated,
- what is still inconsistent with the paper,
- and what to do next (with practical steps).

---

## 1) Project Goal and Scope

Target system:
- EdgeHydra simulation in Python (asyncio, TCP/UDP communication)
- Erasure coding (zfec)
- Baseline EdgeDis for comparison
- Fault injection scenarios
- Experimental metrics (time and cost)
- Docker/Docker Compose execution for distributed-style runs

Expected evaluation scenarios:
1. Normal
2. Slow servers
3. Failed servers
4. Variable file sizes

---

## 2) What Has Been Implemented

### 2.1 Core structure and modules

Implemented module layout:
- `cloud/`
- `edge/`
- `common/`
- `experiments/`

Key files:
- `common/erasure_coding.py`
- `common/fake_video.py`
- `common/transport.py`
- `common/fault_simulator.py`
- `common/metrics.py`
- `common/network_monitor.py`
- `cloud/cloud_client.py`
- `cloud/edgedis_client.py`
- `edge/edge_server.py`
- `edge/edgedis_node.py`
- `experiments/prepare_dataset.py`
- `experiments/run_experiments.py`

### 2.2 EdgeHydra protocol (4 stages)

Implemented in `edge/edge_server.py` and `cloud/cloud_client.py`:
- Stage 1: Cloud encodes and sends coded blocks (`mesbd`) to edges.
- Stage 2: Edges cache and forward blocks (`mesbt`).
- Stage 3: Leaderless supplement:
  - UDP heartbeat (`meshb`) every 100ms,
  - Missing-block request/response (`mesbrq`/`mesbs`).
- Stage 4: Reconstruction when enough blocks (`k`) and broadcast stop-signal (`mesdr_bcast`).

### 2.3 Baseline EdgeDis

Implemented:
- Cloud slices file into `n` parts (no erasure coding): `cloud/edgedis_client.py`
- EdgeDis node behavior with coordinator logic: `edge/edgedis_node.py`
- Reconstruction flow in `experiments/data/recovered_edgedis`

### 2.4 Fault simulation

Implemented in `common/fault_simulator.py`:
- Delay injection
- Drop injection
- Crash-after-time
- Config by env variables:
  - `*_ENABLED`
  - `*_DELAY_MIN_MS`
  - `*_DELAY_MAX_MS`
  - `*_DROP_PROB`
  - `*_CRASH_AFTER_S`

### 2.5 Adaptive scheduling (initial version)

Implemented:
- Lightweight monitor (`common/network_monitor.py`) using RTT probe (`mesping`/`mespong`)
- Peer ranking by RTT and success rate
- Stage-2 forwarding in EdgeHydra prefers better-ranked peers

### 2.6 Metrics and experiment pipeline

Implemented:
- Metrics model and cost formula in `common/metrics.py`
- JSONL sink via `METRICS_SINK` for cross-process/container logging
- Local runner scenarios in `experiments/run_experiments.py`
- Aggregate and merge helpers:
  - `experiments/aggregate_jsonl_metrics.py`
  - `experiments/merge_compose_csvs.py`

### 2.7 Docker / Compose support

Implemented files:
- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `requirements.txt`
- `README.md`

Compose runs for:
- EdgeHydra (`edge0..edge3`, `cloud`)
- EdgeDis (`edgedis0..edgedis3`, `edgedis_cloud`)

Automation:
- `scripts/run_compose_scenarios.sh`

---

## 3) What Has Been Validated

### 3.1 Functional correctness (core)

Validated in current setup:
- Edge nodes reconstruct output files in:
  - `experiments/data/recovered/`
  - `experiments/data/recovered_edgedis/`
- Hash equality checks (raw vs recovered) have been verified in terminal tests.

### 3.2 Scenario execution

Validated:
- Local runner (`experiments/run_experiments.py`) executes all major scenarios.
- Compose-based runs generate per-run CSVs and merged CSV:
  - `results/compose/*.csv`
  - `results/compose/metrics_all.csv`

### 3.3 Operational robustness fixes already done

Fixed during debugging:
- Port conflict retry and process early-exit handling
- Metrics directory creation timing issues
- Race conditions with filesystem operations
- WSL/OneDrive cleanup permission issues (best-effort rmtree)
- Compose file path parameterization for cloud run (`FILE_PATH`)
- Scenario-scoped metrics sinks (`SCENARIO` in sink path)

---

## 4) Known Issues / Gaps vs Paper Expectations

These are the important mismatches to resolve before final report claims:

1. Distribution time trends were previously distorted by Compose startup overhead.
2. `edge_to_edge_bytes` may be too high due to duplicate forwarding/supplement behavior.
3. Some runs showed `edge_to_edge_bytes = 0` in scenarios where Stage 2 should always generate E2E traffic.
4. EdgeHydra can appear slower or costlier than expected in some scenarios.
5. Slow/Failed scenario mapping in Compose still needs strict per-node alignment with paper-like setup (e.g., exactly 3 slow nodes, exactly 1 crashed node).

---

## 5) Cost Formula (Paper Alignment)

Reference formula:

`cost = cloud_to_edge_MiB * 1.0 + edge_to_edge_MiB * (1/9)`

Current `common/metrics.py` logic already follows this form:
- cloud-to-edge: full unit price
- edge-to-edge: 1/9 unit price

Important: if bytes are overcounted by duplicated traffic, cost will still be inflated even with a correct formula.

---

## 6) Priority Next Steps (Action Plan)

### Priority A - Make scenario injection faithful to paper setup

Goal:
- Normal: no artificial faults
- Slow: only selected 3 nodes have delay
- Failed: only selected 1 node crashes

Tasks:
- Refactor `docker-compose.yml` fault env to be per-service, not one global blanket effect.
- Update `scripts/run_compose_scenarios.sh` to apply fault profiles per node.
- Explicitly document which node IDs are slowed/crashed in each scenario.

### Priority B - Reduce duplicate traffic (especially E2E)

Goal:
- Bring E2E bytes and cost closer to paper trends.

Tasks (EdgeHydra):
- Reduce Stage-2 broadcast fanout (selected-forward strategy)
- Stronger early-stop policy when peer has reconstructed
- Avoid retransmitting already-acknowledged blocks

Tasks (EdgeDis):
- Limit coordinator supplement duplication
- Avoid repeated sends of same missing block to same node

### Priority C - Make timing metric purely transfer-driven

Goal:
- Ensure `distribution_time_s` scales with file size and network/fault settings.

Tasks:
- Keep using event timestamp range (`ts_ms`) from JSONL, not container lifecycle wall time.
- Confirm all relevant transfer events emit `ts_ms`.
- Add validation checks to reject runs with suspiciously flat timing curves.

### Priority D - Statistical stability for report

Goal:
- Reliable conclusions, not one-off runs.

Tasks:
- Run each scenario multiple times (recommended: 5-10)
- Compute mean/std for:
  - `distribution_time_s`
  - `distribution_cost_units`
  - bytes counters
- Export one final summary table for report.

---

## 7) Recommended Work Breakdown for Team Collaboration

### Engineer A (Protocol/Network behavior)
- EdgeHydra Stage-2/Stage-3 duplication reduction
- Stop-condition correctness
- Adaptive scheduling refinement

### Engineer B (Experiment pipeline)
- Compose scenario profile correctness
- Runner automation and reproducibility
- CSV/summary generation and validation checks

### Engineer C (Analysis/report)
- Multi-run statistics
- Plotting and trend interpretation
- Comparison text against paper claims and caveats

---

## 8) Suggested Validation Checklist (Before Final Submission)

Run this checklist and mark all pass:

1. Functional
- [ ] Recovered files exist for all required nodes
- [ ] SHA-256 raw == recovered for sampled files/scenarios

2. Metrics integrity
- [ ] Cloud bytes increase with file size
- [ ] E2E bytes are non-zero when Stage-2 forwarding is active
- [ ] No mixed-scenario pollution in metrics directory

3. Scenario correctness
- [ ] Normal has no injected delay/drop/crash
- [ ] Slow affects exactly intended nodes
- [ ] Failed crashes exactly intended node(s)

4. Trend sanity
- [ ] Distribution time increases with file size
- [ ] Cost trend is explainable by traffic pattern
- [ ] EdgeHydra behavior under failures is consistent with design goals

5. Report readiness
- [ ] Multi-run avg/std computed
- [ ] Final combined CSV generated
- [ ] README has reproducible commands

---

## 9) Files Added/Updated Recently (for teammate orientation)

Added:
- `common/network_monitor.py`
- `requirements.txt`
- `.dockerignore`
- `Dockerfile`
- `docker-compose.yml` (expanded)
- `experiments/aggregate_jsonl_metrics.py`
- `experiments/merge_compose_csvs.py`
- `scripts/run_compose_scenarios.sh`
- `PROJECT_STATUS_AND_NEXT_STEPS.md` (this file)

Updated:
- `edge/edge_server.py` (adaptive forwarding, monitor integration)
- `experiments/run_experiments.py` (stability/cleanup/retry-related fixes)
- `README.md` (compose usage + automation notes)

---

## 10) Quick Commands for the Next Session

Re-run compose scenarios:

```bash
cd "/mnt/c/Users/Admin/OneDrive/Desktop/IS211_CSDLPT/Project"
./scripts/run_compose_scenarios.sh
python -m experiments.merge_compose_csvs --in-dir results/compose --out results/compose/metrics_all.csv
nl -ba results/compose/metrics_all.csv | sed -n '1,30p'
```

Bring compose down fully:

```bash
docker compose down -v
```

---

## 11) Current Overall Status

Project is in a strong near-complete state for implementation breadth (all major phases are present), but still needs calibration/tuning to align experimental outcomes with paper-level claims.

In short:
- Implementation coverage: high
- Functional execution: validated
- Paper-faithful metrics behavior: partially complete, requires targeted refinement

