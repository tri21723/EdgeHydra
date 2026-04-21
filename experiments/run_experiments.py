import argparse
import asyncio
import contextlib
import csv
from dataclasses import dataclass
from pathlib import Path
import json
import time
import random
from typing import Dict, List, Optional, Tuple

from common.fault_simulator import FaultConfig
from common.metrics import Metrics, MetricsRecorder, set_active
from cloud.cloud_client import stage1_distribute
from cloud.edgedis_client import distribute_edgedis


def _pick_ids(n: int, count: int) -> List[int]:
    return list(range(min(n, count)))


def _rmtree_best_effort(path: str) -> None:
    """
    Best-effort cleanup for experiment outputs.
    On WSL/Windows mounts it is common to hit permission issues (e.g. files created
    by a different uid or readonly bit). We attempt chmod and retry.
    """
    import os
    import shutil
    import stat

    p = Path(path)
    if not p.exists():
        return

    def _onerror(func, pth, exc_info) -> None:
        with contextlib.suppress(Exception):
            os.chmod(pth, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        with contextlib.suppress(Exception):
            func(pth)

    with contextlib.suppress(Exception):
        shutil.rmtree(p, onerror=_onerror)


@dataclass(frozen=True)
class Ports:
    base_tcp: int
    base_udp: int


def _edgehydra_ports(n: int, ports: Ports) -> Tuple[List[str], List[str]]:
    # returns (tcp_endpoints, udp_endpoints)
    tcp = [f"127.0.0.1:{ports.base_tcp+i}" for i in range(n)]
    udp = [f"127.0.0.1:{ports.base_udp+i}" for i in range(n)]
    return tcp, udp


async def _run_edgehydra_once(
    *,
    scenario: str,
    file_path: str,
    n: int,
    k: int,
    ports: Ports,
    slow_ids: List[int],
    crash_ids: List[int],
) -> Metrics:
    # Start edges as subprocesses to allow per-node env config.
    import os
    import subprocess

    procs: List[subprocess.Popen] = []
    tcp_eps, udp_eps = _edgehydra_ports(n, ports)
    edges_arg = ",".join([f"e{i}@{tcp_eps[i]}" for i in range(n)])

    run_id = f"eh-{scenario}-{Path(file_path).stem}-{int(time.time()*1000)}"
    cache_root = "/tmp/edge_cache_eh"
    work_dir = "/tmp/cloud_work_eh"
    metrics_dir = f"/tmp/metrics-{run_id}"
    _rmtree_best_effort(cache_root)
    _rmtree_best_effort(work_dir)
    _rmtree_best_effort(metrics_dir)
    _rmtree_best_effort("experiments/data/recovered")
    Path(metrics_dir).mkdir(parents=True, exist_ok=True)
    (Path(metrics_dir) / "RUNNING").write_text(run_id)
    logs_dir = Path(metrics_dir) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n):
        env = os.environ.copy()
        env["METRICS_SINK"] = f"{metrics_dir}/edge_{i}.jsonl"
        # Slow servers: delay edge-to-edge
        if i in slow_ids:
            env.update(
                {
                    "E2E_FAULT_ENABLED": "1",
                    "E2E_FAULT_DELAY_MIN_MS": "20",
                    "E2E_FAULT_DELAY_MAX_MS": "40",
                    "E2E_FAULT_DROP_PROB": "0",
                }
            )
        # Failed servers: crash
        if i in crash_ids:
            env.update({"E2E_FAULT_ENABLED": "1", "E2E_FAULT_CRASH_AFTER_S": "1"})

        peer_parts = []
        for j in range(n):
            if j == i:
                continue
            peer_parts.append(f"e{j}@127.0.0.1:{ports.base_tcp+j}:{ports.base_udp+j}")
        peers_arg = ",".join(peer_parts)

        cmd = [
            "python",
            "-m",
            "edge.edge_server",
            "--id",
            f"e{i}",
            "--port",
            str(ports.base_tcp + i),
            "--udp-port",
            str(ports.base_udp + i),
            "--cache-dir",
            cache_root,
            "--peers",
            peers_arg,
        ]
        lf = (logs_dir / f"edge_{i}.log").open("wb")
        procs.append(subprocess.Popen(cmd, env=env, stdout=lf, stderr=lf))

    await asyncio.sleep(0.8)
    # Fail fast if any edge couldn't start (e.g., port already in use)
    for p in procs:
        if p.poll() is not None:
            for pp in procs:
                with contextlib.suppress(Exception):
                    pp.terminate()
            for pp in procs:
                with contextlib.suppress(Exception):
                    pp.wait(timeout=1)
            raise RuntimeError(f"edgehydra edge process exited early (likely port conflict). metrics_dir={metrics_dir}")

    m = n
    metrics = Metrics(scenario=scenario, method="edgehydra", n=n, k=k, m=m, file_path=file_path, file_bytes=Path(file_path).stat().st_size)
    rec = MetricsRecorder(metrics)
    set_active(rec)
    metrics.start()
    try:
        os.environ["METRICS_SINK"] = f"{metrics_dir}/cloud.jsonl"
        await stage1_distribute(
            input_file=file_path,
            work_dir=work_dir,
            edges=[type("E", (), {"edge_id": f"e{i}", "host": "127.0.0.1", "port": ports.base_tcp + i}) for i in range(n)],  # simple struct
            k=k,
            m=m,
            codec="zfec",
            timeout_s=10.0,
        )
        # Give edge-to-edge forwarding/supplement time to run before teardown.
        await asyncio.sleep(0.6)
    finally:
        metrics.stop()
        set_active(None)
        os.environ.pop("METRICS_SINK", None)
        for p in procs:
            with contextlib.suppress(Exception):
                p.terminate()
        for p in procs:
            with contextlib.suppress(Exception):
                p.wait(timeout=1)

    # Aggregate edge-to-edge bytes from jsonl sinks
    e2e = 0
    c2e = 0
    for p in Path(metrics_dir).glob("*.jsonl"):
        for line in p.read_text().splitlines():
            try:
                evt = json.loads(line)
                if evt.get("direction") == "edge_to_edge":
                    e2e += int(evt.get("bytes", 0))
                elif evt.get("direction") == "cloud_to_edge":
                    c2e += int(evt.get("bytes", 0))
            except Exception:
                continue
    # cloud_to_edge is already in-recorder; prefer summed if present
    if c2e:
        metrics.cloud_to_edge_bytes = c2e
    metrics.edge_to_edge_bytes = e2e
    return metrics


async def _run_edgedis_once(
    *,
    scenario: str,
    file_path: str,
    n: int,
    ports: int,
    slow_ids: List[int],
    crash_ids: List[int],
) -> Metrics:
    import os
    import subprocess

    procs: List[subprocess.Popen] = []
    cache_root = "/tmp/edge_cache_ed"
    work_dir = "/tmp/cloud_work_ed"
    run_id = f"ed-{scenario}-{Path(file_path).stem}-{int(time.time()*1000)}"
    metrics_dir = f"/tmp/metrics-{run_id}"
    _rmtree_best_effort(cache_root)
    _rmtree_best_effort(work_dir)
    _rmtree_best_effort(metrics_dir)
    _rmtree_best_effort("experiments/data/recovered_edgedis")
    Path(metrics_dir).mkdir(parents=True, exist_ok=True)
    (Path(metrics_dir) / "RUNNING").write_text(run_id)
    logs_dir = Path(metrics_dir) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    edges_arg = ",".join([f"e{i}@127.0.0.1:{ports+i}" for i in range(n)])
    coordinator_id = "e0"

    for i in range(n):
        env = os.environ.copy()
        env["METRICS_SINK"] = f"{metrics_dir}/edge_{i}.jsonl"
        if i in slow_ids:
            env.update(
                {
                    "E2E_FAULT_ENABLED": "1",
                    "E2E_FAULT_DELAY_MIN_MS": "20",
                    "E2E_FAULT_DELAY_MAX_MS": "40",
                    "E2E_FAULT_DROP_PROB": "0",
                }
            )
        if i in crash_ids:
            env.update({"E2E_FAULT_ENABLED": "1", "E2E_FAULT_CRASH_AFTER_S": "1"})

        peer_parts = []
        for j in range(n):
            if j == i:
                continue
            peer_parts.append(f"e{j}@127.0.0.1:{ports+j}")
        peers_arg = ",".join(peer_parts)

        cmd = [
            "python",
            "-m",
            "edge.edgedis_node",
            "--id",
            f"e{i}",
            "--port",
            str(ports + i),
            "--cache-dir",
            cache_root,
            "--coordinator-id",
            coordinator_id,
            "--peers",
            peers_arg,
        ]
        lf = (logs_dir / f"edge_{i}.log").open("wb")
        procs.append(subprocess.Popen(cmd, env=env, stdout=lf, stderr=lf))

    await asyncio.sleep(0.8)
    for p in procs:
        if p.poll() is not None:
            for pp in procs:
                with contextlib.suppress(Exception):
                    pp.terminate()
            for pp in procs:
                with contextlib.suppress(Exception):
                    pp.wait(timeout=1)
            raise RuntimeError(f"edgedis edge process exited early (likely port conflict). metrics_dir={metrics_dir}")

    metrics = Metrics(scenario=scenario, method="edgedis", n=n, file_path=file_path, file_bytes=Path(file_path).stat().st_size)
    rec = MetricsRecorder(metrics)
    set_active(rec)
    metrics.start()
    try:
        os.environ["METRICS_SINK"] = f"{metrics_dir}/cloud.jsonl"
        await distribute_edgedis(input_file=file_path, work_dir=work_dir, edges=[type("E", (), {"edge_id": f"e{i}", "host": "127.0.0.1", "port": ports + i}) for i in range(n)], timeout_s=10.0)
        # EdgeDis forwarding runs asynchronously inside edges; allow it to complete.
        await asyncio.sleep(0.6)
    finally:
        metrics.stop()
        set_active(None)
        os.environ.pop("METRICS_SINK", None)
        for p in procs:
            with contextlib.suppress(Exception):
                p.terminate()
        for p in procs:
            with contextlib.suppress(Exception):
                p.wait(timeout=1)

    e2e = 0
    c2e = 0
    for p in Path(metrics_dir).glob("*.jsonl"):
        for line in p.read_text().splitlines():
            try:
                evt = json.loads(line)
                if evt.get("direction") == "edge_to_edge":
                    e2e += int(evt.get("bytes", 0))
                elif evt.get("direction") == "cloud_to_edge":
                    c2e += int(evt.get("bytes", 0))
            except Exception:
                continue
    if c2e:
        metrics.cloud_to_edge_bytes = c2e
    metrics.edge_to_edge_bytes = e2e
    return metrics


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
    parser = argparse.ArgumentParser(description="Run EdgeHydra vs EdgeDis experiments and log metrics")
    parser.add_argument("--n", type=int, default=4)
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--file", default="experiments/data/raw/video_1MiB.bin")
    parser.add_argument(
        "--variable-sizes",
        action="store_true",
        help="Also run variable_file_sizes scenario on all experiments/data/raw/video_*MiB.bin files",
    )
    parser.add_argument("--out", default="experiments/results/metrics.csv")
    args = parser.parse_args()

    n = args.n
    k = args.k
    file_path = args.file

    async def _run() -> List[Metrics]:
        rows: List[Metrics] = []

        # Retry wrapper to handle transient port conflicts
        async def _retry(coro_factory, *, attempts: int = 5):
            last_err: Optional[Exception] = None
            for _ in range(attempts):
                try:
                    return await coro_factory()
                except RuntimeError as e:
                    last_err = e
                    await asyncio.sleep(0.2)
                    continue
            assert last_err is not None
            raise last_err

        def _pick_bases() -> Tuple[int, int, int]:
            # NOTE: this is called per retry-attempt so that port conflicts can
            # be resolved by choosing a fresh port range.
            seed = int(time.time() * 1000) & 0xFFFFFFFF
            rng = random.Random(seed)
            base_tcp = rng.randrange(20000, 30000, 100)
            base_udp = rng.randrange(30000, 40000, 100)
            base_ed = rng.randrange(40000, 50000, 100)
            return base_tcp, base_udp, base_ed

        async def _run_pair(
            *,
            scenario: str,
            file_path_override: Optional[str] = None,
            offset: int,
            slow_ids: List[int],
            crash_ids: List[int],
        ) -> None:
            base_tcp, base_udp, base_ed = _pick_bases()
            fp = file_path_override or file_path
            rows.append(
                await _run_edgehydra_once(
                    scenario=scenario,
                    file_path=fp,
                    n=n,
                    k=k,
                    ports=Ports(base_tcp + offset, base_udp + offset),
                    slow_ids=slow_ids,
                    crash_ids=crash_ids,
                )
            )
            rows.append(
                await _run_edgedis_once(
                    scenario=scenario,
                    file_path=fp,
                    n=n,
                    ports=base_ed + offset,
                    slow_ids=slow_ids,
                    crash_ids=crash_ids,
                )
            )

        # Normal
        await _retry(lambda: _run_pair(scenario="normal", offset=0, slow_ids=[], crash_ids=[]))

        # Slow servers (3 slow)
        slow = _pick_ids(n, min(3, n))
        await _retry(lambda: _run_pair(scenario="slow_servers", offset=100, slow_ids=slow, crash_ids=[]))

        # Failed servers (1 crash)
        crash = _pick_ids(n, min(1, n))
        await _retry(lambda: _run_pair(scenario="failed_servers", offset=200, slow_ids=[], crash_ids=crash))

        if args.variable_sizes:
            raw_dir = Path("experiments/data/raw")
            for fp in sorted(raw_dir.glob("video_*MiB.bin")):
                await _retry(
                    lambda fp=fp: _run_pair(
                        scenario="variable_file_sizes",
                        file_path_override=str(fp),
                        offset=300,
                        slow_ids=[],
                        crash_ids=[],
                    )
                )
        return rows

    rows = asyncio.run(_run())
    _write_csv(args.out, rows)
    print(f"Wrote {len(rows)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

