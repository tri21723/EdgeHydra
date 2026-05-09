# EdgeHydra Project Review, Roadmap, and Improvement Plan

Last updated: 2026-04-25

This document summarizes the current state of the project after the latest implementation and demo work. It is intended as a handover document for teammates and as a checklist for the next development sessions.

---

## 1. Project Overview

The project implements a simulation of EdgeHydra, a fault-tolerant edge data distribution protocol based on erasure coding. It also includes an EdgeDis baseline for comparison, an experiment pipeline, Docker Compose deployment, fault injection, metrics collection, and a web demo application.

Main goals:

- Simulate Cloud-to-Edge and Edge-to-Edge data distribution.
- Implement EdgeHydra's four-stage protocol.
- Implement EdgeDis as a baseline.
- Support fault scenarios: normal, slow servers, failed servers, and variable file sizes.
- Measure distribution time and distribution cost.
- Provide a visual demo UI for presentation.

Main tech stack:

- Python 3 with `asyncio`
- TCP socket transfer for data messages
- UDP heartbeat for leaderless supplement
- `zfec` for erasure coding
- Docker / Docker Compose for multi-container simulation
- FastAPI + static HTML/JS for the demo web UI

---

## 2. Current Project Structure

Important directories and files:

- `common/`
  - `erasure_coding.py`: encode/decode logic using `zfec`
  - `fake_video.py`: dummy binary/video file generator
  - `transport.py`: TCP frame protocol
  - `fault_simulator.py`: delay/drop/crash simulation
  - `metrics.py`: metrics model, cost formula, JSONL sink
  - `network_monitor.py`: adaptive scheduling monitor
- `cloud/`
  - `cloud_client.py`: EdgeHydra cloud sender
  - `edgedis_client.py`: EdgeDis cloud sender
- `edge/`
  - `edge_server.py`: EdgeHydra edge node
  - `edgedis_node.py`: EdgeDis edge node and coordinator behavior
- `experiments/`
  - `run_experiments.py`: local runner
  - `aggregate_jsonl_metrics.py`: aggregate JSONL events into CSV
  - `merge_compose_csvs.py`: merge per-run compose CSVs
  - `validate_metrics.py`: sanity-check merged metrics
  - `prepare_dataset.py`: dataset preparation helper
- `scripts/`
  - `run_compose_scenarios.sh`: automated Compose scenario runner
- `webapp/`
  - `main.py`: FastAPI backend for demo execution/status
  - `static/index.html`: visual demo UI
- Root files:
  - `docker-compose.yml`
  - `Dockerfile`
  - `requirements.txt`
  - `.dockerignore`
  - `README.md`
  - `PROJECT_STATUS_AND_NEXT_STEPS.md`
  - `PROJECT_REVIEW_ROADMAP_AND_IMPROVEMENTS.md` (this file)

---

## 3. What Has Been Implemented

### 3.1 Phase 1 - Project Setup and Utilities

Completed:

- Created the core project structure: `cloud/`, `edge/`, `common/`, `experiments/`, `scripts/`, `webapp/`.
- Implemented fake video generation in `common/fake_video.py`.
- Implemented erasure coding in `common/erasure_coding.py`.
- Added `requirements.txt` with:
  - `zfec==1.6.0.0`
  - `fastapi`
  - `uvicorn[standard]`
- Added Docker-related files:
  - `Dockerfile`
  - `.dockerignore`
  - `docker-compose.yml`

### 3.2 Phase 2 - Core EdgeHydra Protocol

Implemented in `cloud/cloud_client.py` and `edge/edge_server.py`.

Implemented EdgeHydra stages:

1. Stage 1 - Cloud distribution
   - Cloud reads an input file.
   - Cloud erasure-encodes the file using `(k=3, m=4)` by default.
   - Cloud sends coded blocks to edge nodes over TCP using `mesbd`.
   - Cloud-to-edge traffic is recorded through `record_cloud_to_edge`.

2. Stage 2 - Edge forwarding
   - Edge nodes receive coded blocks.
   - Blocks are cached per node.
   - Edges forward blocks to selected peers using `mesbt`.
   - Edge-to-edge traffic is recorded through `record_edge_to_edge`.

3. Stage 3 - Leaderless supplement
   - UDP heartbeat message `meshb` is broadcast every 100ms.
   - Nodes track peer block possession.
   - Missing blocks are requested using `mesbrq`.
   - Missing blocks are supplied using `mesbs`.

4. Stage 4 - Reconstruction and stop signal
   - A node reconstructs once it has enough blocks (`k`).
   - Reconstruction writes recovered files under `experiments/data/recovered/<edge_id>/`.
   - Reconstructed nodes broadcast `mesdr_bcast` to stop unnecessary transfers.

### 3.3 Phase 3 - EdgeDis Baseline and Fault Simulation

Implemented in `cloud/edgedis_client.py` and `edge/edgedis_node.py`.

EdgeDis behavior:

- Cloud slices a file into `n=4` blocks without erasure coding.
- Edge nodes forward blocks.
- Coordinator logic supplies missing blocks.
- Reconstruction requires all `n` slices.
- Recovered files are written under `experiments/data/recovered_edgedis/<edge_id>/`.

Fault simulation:

- Implemented in `common/fault_simulator.py`.
- Supports:
  - artificial delay
  - random drop
  - crash-after-time
- Fault settings are controlled with environment variables:
  - `E2E_FAULT_ENABLED`
  - `E2E_FAULT_DELAY_MIN_MS`
  - `E2E_FAULT_DELAY_MAX_MS`
  - `E2E_FAULT_DROP_PROB`
  - `E2E_FAULT_CRASH_AFTER_S`
- Docker Compose maps these per edge:
  - `EDGE0_E2E_FAULT_*`
  - `EDGE1_E2E_FAULT_*`
  - `EDGE2_E2E_FAULT_*`
  - `EDGE3_E2E_FAULT_*`

### 3.4 Phase 4 - Adaptive Scheduling

Implemented in `common/network_monitor.py` and integrated in `edge/edge_server.py`.

Implemented behavior:

- Edge nodes run a `NetworkStateMonitor`.
- Monitor probes peers with `mesping` / `mespong`.
- Each peer link maintains:
  - EWMA RTT
  - sent probe count
  - successful probe count
  - success rate
  - last successful response timestamp
- Peer ranking is based on:
  - lower RTT
  - higher success rate
  - penalty for failed/timeout probes
- EdgeHydra Stage 2 forwarding uses `rank_peers()`.
- Forward fanout is controlled by `target_fanout`.

Recent enhancement:

- Real adaptive runtime telemetry is exported from edge containers:
  - `metrics/edgehydra/<scenario>/netmon_e0.json`
  - `metrics/edgehydra/<scenario>/netmon_e1.json`
  - `metrics/edgehydra/<scenario>/netmon_e2.json`
  - `metrics/edgehydra/<scenario>/netmon_e3.json`
- Web UI now reads real `netmon_*` files instead of showing only estimates.
- The Adaptive panel displays:
  - probe interval
  - probe timeout
  - EWMA alpha
  - target fanout
  - real ranked peers
  - source node
  - peer RTT / success rate

### 3.5 Phase 5 - Metrics and Experiment Pipeline

Implemented:

- `common/metrics.py`
  - `Metrics` dataclass
  - cloud-to-edge bytes
  - edge-to-edge bytes
  - distribution time
  - distribution cost
- Paper-aligned cost formula:

```text
cost = cloud_to_edge_MiB * 1.0 + edge_to_edge_MiB * (1/9)
```

- Cross-process metrics sink using JSONL:
  - `METRICS_SINK=/metrics/<method>/<scenario>/<component>.jsonl`
- Aggregation:
  - `experiments/aggregate_jsonl_metrics.py`
  - sums C2E/E2E bytes
  - computes distribution time from min/max event `ts_ms`
- Merge:
  - `experiments/merge_compose_csvs.py`
- Validation:
  - `experiments/validate_metrics.py`

Experiment scenarios currently supported:

- `normal`
- `slow_servers`
- `failed_servers`
- `variable_file_sizes`

Compose automation:

- `scripts/run_compose_scenarios.sh`
- Runs EdgeHydra and EdgeDis for each scenario.
- Writes per-run CSVs to `results/compose/`.
- Merged output is expected at `results/compose/metrics_all.csv`.

### 3.6 Docker Compose Simulation

Implemented in `docker-compose.yml`.

Services:

- EdgeHydra:
  - `edge0`
  - `edge1`
  - `edge2`
  - `edge3`
  - `cloud`
- EdgeDis:
  - `edgedis0`
  - `edgedis1`
  - `edgedis2`
  - `edgedis3`
  - `edgedis_cloud`

Important Compose features:

- Per-service metrics sink.
- Per-edge fault environment variables.
- Separate volumes for edge caches.
- Input file mounted from `${HOST_INPUT_DIR}` into `/inputs`.
- Cloud work directories moved to `/tmp/cloud_work` and `/tmp/cloud_work_edgedis` to reduce Windows/OneDrive/WSL I/O issues.

### 3.7 Web Demo Application

Implemented in `webapp/main.py` and `webapp/static/index.html`.

Core features:

- FastAPI backend.
- Single-page live demo UI.
- Can run one demo scenario from the browser.
- Supports:
  - `EdgeHydra`
  - `EdgeDis`
  - `normal`
  - `slow`
  - `failed`
  - configurable file size in MiB

Backend demo behavior:

- Starts demo runs as background jobs.
- Runs Docker Compose services.
- Generates/copies input file to `/tmp/edgehydra_inputs`.
- Applies fault profile.
- Aggregates output metrics.
- Exposes live status endpoint:
  - phase
  - stage label
  - node status
  - reconstructed count
  - traffic bytes
  - fault plan
  - adaptive telemetry

UI visualization:

- Topology diagram with Cloud and 4 Edge nodes.
- Node colors:
  - receiving/running
  - reconstructed
  - failed
  - slow
- Realtime metrics:
  - current stage
  - reconstructed nodes
  - Cloud-to-Edge bytes
  - Edge-to-Edge bytes
- Fault banner.
- Event timeline.
- Random slow/failed edge display.
- Adaptive Scheduling panel for EdgeHydra only.
- Real block count per node:
  - `x / k blocks`
  - backed by exported node state, not fake UI inference.

Recent web demo fixes:

- Fixed `Optional` import issue in `webapp/main.py`.
- Fixed old recovered files causing false reconstructed status.
- Added finalizing phase to avoid showing `Completed` too early.
- Added real block-state export:
  - `node_state_e0.json`
  - `node_state_e1.json`
  - `node_state_e2.json`
  - `node_state_e3.json`
- Added real adaptive telemetry export:
  - `netmon_e0.json`
  - `netmon_e1.json`
  - `netmon_e2.json`
  - `netmon_e3.json`
- Randomized demo fault target:
  - `slow`: random 2-3 edges
  - `failed`: random 1 edge

---

## 4. What Has Been Validated So Far

Validated through terminal and demo runs:

- Fake video generation works.
- Erasure encode/decode works with `zfec`.
- EdgeHydra can reconstruct files.
- EdgeDis can reconstruct files.
- Docker Compose can run EdgeHydra and EdgeDis.
- Metrics JSONL files are generated.
- CSV aggregation works.
- `metrics_all.csv` can be validated with `experiments.validate_metrics`.
- Web demo can launch runs and display live status.
- Adaptive panel now shows real runtime values from `NetworkStateMonitor`.
- Failed/slow scenarios can be visualized in the UI.
- Real block counts are now available through exported node state.

---

## 5. Important Current Limitations

### 5.1 Demo completion semantics still need refinement

The current demo distinguishes:

- Cloud distribution/aggregation completion
- Edge reconstruction completion

This was improved by adding a `finalizing` phase and a grace period. However, for very large files such as `144MiB`, some nodes may still show partial block state when the job completes.

Reason:

- EdgeHydra only requires `k=3` blocks to reconstruct.
- The system does not necessarily require all 4 nodes to reconstruct before considering the distribution run useful.
- For demo clarity, users often expect `4/4 reconstructed`, but EdgeHydra's fault-tolerant design can still be considered successful if enough nodes recover depending on the experiment definition.

Needed decision:

- Should a demo run be considered complete when any node reconstructs?
- When all non-failed nodes reconstruct?
- When all 4 nodes reconstruct?
- Or when cloud transfer is done and metrics are aggregated?

This decision should be made explicit in both code and UI.

### 5.2 EdgeHydra default is fixed at `k=3, m=4`

Current Compose config uses:

- `k=3`
- `m=4`

Therefore:

- Cloud creates 4 coded shards.
- Each EdgeHydra node needs any 3 shards to reconstruct.
- UI correctly shows `x/3 blocks`.

Potential improvement:

- Add demo controls for `k` and `m`.
- Or document clearly that the current demo is fixed `(k=3, m=4)`.

### 5.3 Adaptive Scheduling panel shows one source node

Current UI chooses `e0` as the primary source node if available. This is useful for presentation, but it does not show all per-node rankings at once.

Current display:

- Source node: `e0`
- Ranked peers from `e0`'s monitor
- Peer RTT/ok-rate from `e0`

Potential improvement:

- Add dropdown to select source node.
- Add table for all edge nodes:
  - source
  - peer
  - RTT
  - ok rate
  - rank

### 5.4 Slow/failed profiles differ between web demo and batch experiment script

Batch script (`scripts/run_compose_scenarios.sh`):

- `slow_servers`: fixed `e1,e2,e3` delay 20-40ms
- `failed_servers`: fixed `e1` crash after 1s

Web demo:

- `slow`: random 2-3 edges with stronger delay/drop
- `failed`: random 1 edge crash after 0.35s

This is intentional for visual demonstration, but it should be documented as two modes:

- Paper-like batch mode
- Visual demo mode

### 5.5 Metrics may still require calibration for paper-level claims

Known possible mismatches:

- Edge-to-edge bytes may be high due to forwarding and supplementation duplication.
- Distribution time may be affected by system load and probe timeouts.
- Compose/Docker overhead must not be included in transfer time.
- For large files, probe success rate can drop due to CPU/network load, even in normal mode.

---

## 6. What Should Be Done Next

### Priority 1 - Define Correct Completion Criteria

Decide and implement one clear rule for demo/experiment completion.

Recommended options:

1. Experiment metric completion:
   - complete when all required transfer events are done and metrics are aggregated.
2. EdgeHydra protocol completion:
   - complete when at least one node reconstructs and stop signals propagate.
3. Demo visualization completion:
   - complete when all non-failed nodes reconstruct.
4. Strict educational completion:
   - complete when all 4 nodes reconstruct.

Recommended for demo:

- Normal/slow: wait for all 4 nodes if possible.
- Failed: wait for all non-failed nodes.
- Timeout after a configurable grace period.
- UI label should say exactly which rule was used.

Implementation tasks:

- Track failed nodes from `fault_plan`.
- Compute expected reconstruct target:
  - normal: 4
  - slow: 4
  - failed: `4 - failed_count`
- Replace fixed grace-only logic with:
  - wait until target reconstructed
  - or timeout
- Show:
  - `Completed: 3/3 expected nodes reconstructed`
  - or `Partial: 3/4 reconstructed, timeout reached`

### Priority 2 - Improve Real Block Progress

Already implemented:

- node exports `received_count`
- UI shows `x/k blocks`

Next improvements:

- Show exact block IDs next to each node:
  - `blocks: [0, 2, 3]`
- Add tooltip or expandable row for each node.
- Add event when a new block is received:
  - `e2 received block 3`
- Detect block progress delta per poll and add it to timeline.

### Priority 3 - Improve Adaptive Scheduling Visualization

Already implemented:

- real `netmon_*` export
- real RTT/ok rate
- real rank

Next improvements:

- Add source-node selector:
  - `e0`, `e1`, `e2`, `e3`
- Show all source nodes in a table.
- Highlight degraded peer links.
- Add link color intensity based on RTT/ok rate:
  - green: good
  - yellow: slow
  - red: bad/failing
- Add event timeline messages:
  - `adaptive rank changed: e3 > e2 > e1`
  - `e1 ok-rate dropped below 50%`

### Priority 4 - Align Demo Fault Profiles with Paper and Presentation Needs

Current state:

- Batch mode is paper-like.
- Web demo is visual and random.

Recommended:

- Add UI selector:
  - `Paper fixed`
  - `Random visual`
- For paper fixed:
  - slow: `e1,e2,e3`
  - failed: `e1`
- For random visual:
  - slow: random 2-3 edges
  - failed: random 1 edge

This gives both reproducibility and a visually interesting demo.

### Priority 5 - Improve Large File Behavior

Observed issue:

- With `normal 144MiB`, one edge can stay at `2/3 blocks` while phase says complete or partial complete.

Likely causes:

- Demo job finalization timeout too short.
- Large file forwarding takes longer.
- Controlled fanout may not reach all nodes before stop signals.
- Some edge nodes may not need to reconstruct for the protocol to be considered successful.

Possible improvements:

- Increase finalizing grace period for large files.
- Scale finalizing timeout by file size.
- Keep containers alive until target reconstruction count is reached.
- Show partial success explicitly instead of implying failure.
- Add retry/supplement pressure during finalizing phase.

### Priority 6 - Reduce E2E Traffic Duplication

Still important for paper alignment.

Tasks:

- Review Stage-2 forwarding.
- Ensure peers that already have a block are skipped.
- Ensure peers that already reconstructed are skipped.
- Avoid repeated supplement requests for the same block.
- Add per-block/per-peer send ledger for Stage 3.
- Compare EdgeHydra E2E bytes against EdgeDis in all scenarios.

### Priority 7 - Improve Statistical Experiment Quality

Current pipeline runs one sample per scenario/file.

Recommended:

- Run each scenario 5-10 times.
- Compute:
  - mean
  - standard deviation
  - min/max
- Output:
  - `results/compose/metrics_summary.csv`
  - optional charts for report

Potential new script:

- `experiments/run_repeated_compose.py`
- `experiments/summarize_metrics.py`

### Priority 8 - Documentation and Report Readiness

Update documentation:

- `README.md`
  - current run command should mention `8081` if that is the active port
  - explain web demo vs batch experiment difference
  - explain `k=3,m=4`
  - explain adaptive panel
- Add a short `DEMO_GUIDE.md`
  - normal run script
  - slow run script
  - failed run script
  - what to say when presenting each screen
- Add a `METRICS_GUIDE.md`
  - cost formula
  - time formula
  - where JSONL files are stored
  - what each CSV column means

---

## 7. Detailed Improvement Checklist

### Protocol correctness

- [ ] Confirm every EdgeHydra node writes correct recovered file.
- [ ] Confirm SHA-256 raw == recovered for sampled runs.
- [ ] Confirm `mesdr_bcast` actually stops unnecessary forwarding.
- [ ] Confirm leaderless supplement requests do not storm.
- [ ] Confirm crashed edge is avoided by adaptive forwarding.

### Adaptive scheduling

- [x] Implement RTT probes.
- [x] Implement EWMA RTT.
- [x] Implement success rate.
- [x] Use peer ranking in forwarding.
- [x] Export real netmon stats.
- [x] Show real adaptive panel in web UI.
- [ ] Show all source-node rankings.
- [ ] Add rank-change events.
- [ ] Tune probe timeout for large files.

### Metrics

- [x] Record C2E bytes.
- [x] Record E2E bytes.
- [x] Aggregate JSONL to CSV.
- [x] Use event timestamp range for transfer time.
- [x] Use paper cost formula.
- [ ] Add repeated-run statistics.
- [ ] Add confidence/variance reporting.
- [ ] Add validation for suspiciously flat timing.

### Fault scenarios

- [x] Delay injection.
- [x] Drop injection.
- [x] Crash-after-time injection.
- [x] Per-edge Compose fault environment.
- [x] Web demo random slow/failed target.
- [ ] UI mode for fixed paper faults vs random demo faults.
- [ ] Record fault plan into result CSV metadata.
- [ ] Show exact injected delay/drop/crash values in UI.

### Web demo

- [x] FastAPI backend.
- [x] Visual topology.
- [x] Event timeline.
- [x] Fault banner.
- [x] Real block progress.
- [x] Real adaptive panel.
- [x] Random fault visualization.
- [ ] Source-node selector for adaptive panel.
- [ ] Show exact block IDs.
- [ ] Show transfer pulses when byte delta increases.
- [ ] Better completion semantics.
- [ ] Export demo result screenshot/report.

### Docker / environment

- [x] Compose multi-container setup.
- [x] WSL/OneDrive input workaround via `/tmp/edgehydra_inputs`.
- [x] `/tmp/cloud_work` for cloud working directory.
- [ ] Ensure commands in `README.md` consistently use `python3` or activated venv.
- [ ] Add troubleshooting section for occupied ports.
- [ ] Add troubleshooting section for `externally-managed-environment`.

---

## 8. Recommended Next Development Order

Recommended order for the next sessions:

1. Fix demo completion rule.
   - Most visible current issue.
   - Prevents confusing screens like `Completed` while nodes are still `2/3`.

2. Add block-ID display.
   - Makes `x/k blocks` educational.
   - Helps explain erasure coding.

3. Add adaptive source-node selector.
   - Makes adaptive panel more credible.
   - Shows that each edge has its own view of the network.

4. Add fixed/random fault mode selector.
   - Separates paper experiment from visual demo.

5. Add repeated-run statistics.
   - Important for final report and paper comparison.

6. Tune traffic duplication and timing.
   - Required for stronger paper-aligned claims.

---

## 9. Suggested Commands

Start web demo:

```bash
cd "/mnt/c/Users/Admin/OneDrive/Desktop/IS211_CSDLPT/Project"
source .venv/bin/activate
python3 -m uvicorn webapp.main:app --host 0.0.0.0 --port 8081 --reload
```

Run batch Compose scenarios:

```bash
cd "/mnt/c/Users/Admin/OneDrive/Desktop/IS211_CSDLPT/Project"
source .venv/bin/activate
chmod +x scripts/run_compose_scenarios.sh
./scripts/run_compose_scenarios.sh
python3 -m experiments.merge_compose_csvs --in-dir results/compose --out results/compose/metrics_all.csv
python3 -m experiments.validate_metrics --csv results/compose/metrics_all.csv
```

Stop containers:

```bash
docker compose down -v
```

Inspect demo telemetry files:

```bash
ls -la metrics/edgehydra/*/
ls -la metrics/edgedis/*/
```

Expected live files during web demo:

```text
node_state_e0.json
node_state_e1.json
node_state_e2.json
node_state_e3.json
netmon_e0.json
netmon_e1.json
netmon_e2.json
netmon_e3.json
```

---

## 10. Current Overall Status

Implementation status:

- Core EdgeHydra protocol: mostly complete
- EdgeDis baseline: complete enough for comparison
- Fault injection: implemented and visible in demo
- Adaptive scheduling: implemented and now observable with real telemetry
- Metrics pipeline: implemented
- Docker Compose simulation: implemented
- Web demo: functional and increasingly presentation-ready

Remaining work:

- Clarify and enforce completion semantics.
- Improve large-file demo behavior.
- Improve paper-faithful metric trends.
- Reduce duplicate E2E traffic.
- Add repeated-run statistics.
- Improve documentation for final presentation/report.

Summary:

The project is now beyond a terminal-only simulator. It has a working distributed simulation, metrics pipeline, adaptive scheduling telemetry, and an interactive web demo. The main remaining work is not basic implementation, but correctness polish, metric calibration, and presentation/report readiness.

