## EdgeHydra Simulation (Python/asyncio)

This repo contains a process-based simulation of EdgeHydra (and baseline EdgeDis) using:
- TCP for block/file transfer
- UDP for heartbeats (leaderless supplement)
- Erasure coding via `zfec`

### Local (venv) quick run

```bash
cd "/mnt/c/Users/Admin/OneDrive/Desktop/IS211_CSDLPT/Project"
source .venv/bin/activate

python -m experiments.run_experiments --n 4 --k 3 --file experiments/data/raw/video_1MiB.bin --out /tmp/metrics.csv
nl -ba /tmp/metrics.csv | sed -n '1,12p'
```

### Docker Compose (multi-container)

Prereqs: Docker + Compose plugin.

Metrics: containers write JSONL events under `./metrics/` on the host.

1) Start 4 edge nodes:

```bash
docker compose up -d --build edge0 edge1 edge2 edge3
```

2) Run cloud Stage-1 distributor (one-shot container):

```bash
docker compose run --rm cloud
```

3) Verify recovered outputs (written under `experiments/data/recovered/` on the host):

```bash
ls -la experiments/data/recovered
```

4) EdgeDis baseline (multi-container):

```bash
docker compose up -d --build edgedis0 edgedis1 edgedis2 edgedis3
docker compose run --rm edgedis_cloud
ls -la experiments/data/recovered_edgedis
```

4) Stop everything:

```bash
docker compose down -v
```

### Automated 4-scenario run (Compose)

Runs Normal / Slow / Failed / Variable File Sizes via Compose, and writes per-run CSVs under `results/compose/`.

```bash
chmod +x scripts/run_compose_scenarios.sh
./scripts/run_compose_scenarios.sh
ls -la results/compose
```

Merge per-run compose CSVs into one:

```bash
python -m experiments.merge_compose_csvs --in-dir results/compose --out results/compose/metrics_all.csv
nl -ba results/compose/metrics_all.csv | sed -n '1,20p'
```

