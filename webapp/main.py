import asyncio
import csv
import json
import os
import random
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[1]
RESULTS_CSV = ROOT / "results" / "compose" / "metrics_all.csv"
INPUTS_DIR = Path("/tmp/edgehydra_inputs")
DEMO_CSV = ROOT / "results" / "compose" / "demo_last.csv"

app = FastAPI(title="EdgeHydra Demo Dashboard")
app.mount("/static", StaticFiles(directory=str(ROOT / "webapp" / "static")), name="static")


def _read_metrics_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


async def _run_cmd(cmd: List[str], env: Dict[str, str] = None) -> Dict[str, Any]:
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(ROOT),
        env=run_env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    text = out.decode("utf-8", errors="replace")
    return {"code": proc.returncode, "output": text}


def _base_fault_env() -> Dict[str, str]:
    return {
        "EDGE0_E2E_FAULT_ENABLED": "0",
        "EDGE1_E2E_FAULT_ENABLED": "0",
        "EDGE2_E2E_FAULT_ENABLED": "0",
        "EDGE3_E2E_FAULT_ENABLED": "0",
        "EDGE0_E2E_FAULT_DELAY_MIN_MS": "0",
        "EDGE0_E2E_FAULT_DELAY_MAX_MS": "0",
        "EDGE1_E2E_FAULT_DELAY_MIN_MS": "0",
        "EDGE1_E2E_FAULT_DELAY_MAX_MS": "0",
        "EDGE2_E2E_FAULT_DELAY_MIN_MS": "0",
        "EDGE2_E2E_FAULT_DELAY_MAX_MS": "0",
        "EDGE3_E2E_FAULT_DELAY_MIN_MS": "0",
        "EDGE3_E2E_FAULT_DELAY_MAX_MS": "0",
        "EDGE0_E2E_FAULT_CRASH_AFTER_S": "-1",
        "EDGE1_E2E_FAULT_CRASH_AFTER_S": "-1",
        "EDGE2_E2E_FAULT_CRASH_AFTER_S": "-1",
        "EDGE3_E2E_FAULT_CRASH_AFTER_S": "-1",
        "EDGE0_E2E_FAULT_DROP_PROB": "0",
        "EDGE1_E2E_FAULT_DROP_PROB": "0",
        "EDGE2_E2E_FAULT_DROP_PROB": "0",
        "EDGE3_E2E_FAULT_DROP_PROB": "0",
    }


def _build_fault_plan(profile: str, fault_mode: str) -> Dict[str, Any]:
    base = _base_fault_env()
    plan: Dict[str, Any] = {"profile": profile, "fault_mode": fault_mode, "slow_edges": [], "failed_edges": []}
    edge_ids = ["0", "1", "2", "3"]
    if profile == "normal":
        return {"env": base, "plan": plan}
    if fault_mode == "paper":
        if profile == "slow":
            for eid in ("1", "2", "3"):
                base[f"EDGE{eid}_E2E_FAULT_ENABLED"] = "1"
                base[f"EDGE{eid}_E2E_FAULT_DELAY_MIN_MS"] = "20"
                base[f"EDGE{eid}_E2E_FAULT_DELAY_MAX_MS"] = "40"
            plan["slow_edges"] = ["e1", "e2", "e3"]
        if profile == "failed":
            base["EDGE1_E2E_FAULT_ENABLED"] = "1"
            base["EDGE1_E2E_FAULT_CRASH_AFTER_S"] = "1.0"
            plan["failed_edges"] = ["e1"]
        return {"env": base, "plan": plan}
    # random visual mode
    if profile == "slow":
        cnt = random.randint(2, 3)
        selected = random.sample(edge_ids, k=cnt)
        for eid in selected:
            base[f"EDGE{eid}_E2E_FAULT_ENABLED"] = "1"
            base[f"EDGE{eid}_E2E_FAULT_DELAY_MIN_MS"] = "50"
            base[f"EDGE{eid}_E2E_FAULT_DELAY_MAX_MS"] = "150"
            base[f"EDGE{eid}_E2E_FAULT_DROP_PROB"] = "0.1"
        plan["slow_edges"] = [f"e{x}" for x in selected]
    if profile == "failed":
        failed = random.choice(edge_ids)
        base[f"EDGE{failed}_E2E_FAULT_ENABLED"] = "1"
        base[f"EDGE{failed}_E2E_FAULT_CRASH_AFTER_S"] = "0.35"
        plan["failed_edges"] = [f"e{failed}"]
    return {"env": base, "plan": plan}


def _fault_env(profile: str, fault_mode: str = "random") -> Dict[str, Any]:
    base = {
        "env": {},
        "plan": {"profile": profile, "fault_mode": fault_mode, "slow_edges": [], "failed_edges": []},
    }
    # Backward-compatible wrapper, delegated to clean helper.
    base = _build_fault_plan(profile, fault_mode)
    return base


class DemoRunRequest(BaseModel):
    method: str = "edgehydra"  # edgehydra | edgedis
    file_size_mib: int = 24
    fault_profile: str = "normal"  # normal | slow | failed
    fault_mode: str = "random"  # paper | random


DEMO_JOBS: Dict[str, Dict[str, Any]] = {}


def _expected_reconstruct_target(job: Dict[str, Any]) -> int:
    method = str(job.get("method", "edgehydra"))
    total_nodes = len(_services_for_method(method)["edges"])
    plan = job.get("fault_plan", {}) or {}
    profile = str(plan.get("profile", "normal"))
    if profile == "failed":
        failed_nodes = plan.get("failed_edges", []) or []
        return max(0, total_nodes - len(failed_nodes))
    return total_nodes


def _services_for_method(method: str) -> Dict[str, Any]:
    if method == "edgehydra":
        return {
            "edges": ["edge0", "edge1", "edge2", "edge3"],
            "cloud": "cloud",
            "recover_root": ROOT / "experiments" / "data" / "recovered",
            "k_target": 3,
        }
    return {
        "edges": ["edgedis0", "edgedis1", "edgedis2", "edgedis3"],
        "cloud": "edgedis_cloud",
        "recover_root": ROOT / "experiments" / "data" / "recovered_edgedis",
        "k_target": 4,
    }


def _sum_jsonl_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for line in path.read_text().splitlines():
        try:
            evt = json.loads(line)
            total += int(evt.get("bytes", 0))
        except Exception:
            continue
    return total


def _sum_jsonl_direction(path: Path, direction: str) -> int:
    if not path.exists():
        return 0
    total = 0
    for line in path.read_text().splitlines():
        try:
            evt = json.loads(line)
            if str(evt.get("direction", "")) != direction:
                continue
            total += int(evt.get("bytes", 0))
        except Exception:
            continue
    return total


def _sum_dir_direction(dir_path: Path, direction: str) -> int:
    if not dir_path.exists():
        return 0
    total = 0
    for p in sorted(dir_path.glob("*.jsonl")):
        total += _sum_jsonl_direction(p, direction)
    return total


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text())
    except Exception:
        return None


def _parse_compose_ps_json(output: str) -> Dict[str, str]:
    states: Dict[str, str] = {}
    text = output.strip()
    if not text:
        return states
    try:
        data = json.loads(text)
        if isinstance(data, list):
            for row in data:
                name = str(row.get("Service") or row.get("Name") or "").strip()
                st = str(row.get("State") or row.get("Status") or "unknown").strip()
                if name:
                    states[name] = st
            return states
    except Exception:
        pass
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("["):
            continue
        try:
            row = json.loads(line)
            name = str(row.get("Service") or row.get("Name") or "").strip()
            st = str(row.get("State") or row.get("Status") or "unknown").strip()
            if name:
                states[name] = st
        except Exception:
            continue
    return states


async def _live_status(job: Dict[str, Any]) -> Dict[str, Any]:
    method = str(job["method"])
    scenario = str(job["scenario"])
    svc = _services_for_method(method)
    metrics_dir = ROOT / "metrics" / method / scenario
    ps = await _run_cmd(["docker", "compose", "ps", "--format", "json"], env=job.get("env"))
    state_map = _parse_compose_ps_json(ps["output"]) if ps["code"] == 0 else {}

    started_at = float(job.get("started_at", time.time()))
    elapsed_s = max(0.0, time.time() - started_at)
    env = job.get("env", {}) or {}
    nodes: List[Dict[str, Any]] = []
    node_state_ready_count = 0
    for idx, edge_service in enumerate(svc["edges"]):
        node_id = f"e{idx}"
        traffic = _sum_jsonl_direction(metrics_dir / f"{node_id}.jsonl", "edge_to_edge")
        state_file = metrics_dir / f"node_state_{node_id}.json"
        state_snap = _read_json(state_file) or {}
        has_state = bool(state_snap)
        if has_state:
            node_state_ready_count += 1
        recovered_dir = svc["recover_root"] / node_id
        reconstructed = False
        if recovered_dir.exists():
            for fp in recovered_dir.glob("*.bin"):
                try:
                    # Consider only files produced in this demo run.
                    if fp.stat().st_mtime >= started_at - 0.5:
                        reconstructed = True
                        break
                except Exception:
                    continue
        enabled = str(env.get(f"EDGE{idx}_E2E_FAULT_ENABLED", "0")) in {"1", "true", "True"}
        crash_after_s = float(env.get(f"EDGE{idx}_E2E_FAULT_CRASH_AFTER_S", "-1") or -1)
        delay_max_ms = int(env.get(f"EDGE{idx}_E2E_FAULT_DELAY_MAX_MS", "0") or 0)
        drop_prob = float(env.get(f"EDGE{idx}_E2E_FAULT_DROP_PROB", "0") or 0)
        fault_state = "none"
        if enabled and crash_after_s > 0 and elapsed_s >= crash_after_s:
            fault_state = "failed_injected"
        elif enabled and (delay_max_ms > 0 or drop_prob > 0):
            fault_state = "slow_injected"
        received_block_ids = state_snap.get("received_block_ids", state_snap.get("received_blocks", []))
        nodes.append(
            {
                "id": node_id,
                "service": edge_service,
                "status": state_map.get(edge_service, "unknown"),
                "traffic_bytes": traffic,
                "reconstructed": bool(state_snap.get("reconstructed", False)) or reconstructed,
                "k_target": int(svc["k_target"]),
                "k_reached": min(int(svc["k_target"]), int(state_snap.get("received_count", 0))),
                "received_count": int(state_snap.get("received_count", 0)),
                "received_block_ids": received_block_ids,
                "crashed": bool(state_snap.get("crashed", False)),
                "node_state_available": has_state,
                "fault_state": fault_state,
            }
        )

    cloud_bytes = _sum_dir_direction(metrics_dir, "cloud_to_edge")
    edge_bytes = _sum_dir_direction(metrics_dir, "edge_to_edge")
    reconstructed_count = len([n for n in nodes if n["reconstructed"]])
    expected_target = int(job.get("expected_target", _expected_reconstruct_target(job)))
    completion_reason = str(job.get("completion_reason", ""))
    phase = str(job.get("phase", "unknown"))
    stage_label = "Setup"
    if phase in {"run_cloud"} and reconstructed_count == 0:
        stage_label = "Stage 1-3 active"
    elif reconstructed_count > 0 and reconstructed_count < len(nodes):
        stage_label = "Stage 4 reconstructing"
    elif reconstructed_count == len(nodes) and reconstructed_count > 0:
        stage_label = "Stage 4 completed"
    if phase == "done":
        if completion_reason == "target_reached":
            stage_label = "Completed"
        elif completion_reason == "timeout":
            stage_label = "Partial completion (timeout)"
        else:
            stage_label = "Completed (cloud), partial edge reconstruction"
    if phase == "finalizing":
        stage_label = "Finalizing edge reconstruction"
    if phase == "error":
        stage_label = "Failed"

    adaptive: Dict[str, Any] = {"enabled": False}
    if method == "edgehydra":
        snaps: List[Dict[str, Any]] = []
        for idx in range(4):
            s = _read_json(metrics_dir / f"netmon_e{idx}.json")
            if s:
                snaps.append(s)
        if snaps:
            all_sources: Dict[str, Any] = {}
            for s in snaps:
                sid = str(s.get("self_id", "")).strip()
                if sid:
                    all_sources[sid] = s
            # Show e0 snapshot first (stable presenter node), fallback to latest.
            primary = None
            for s in snaps:
                if str(s.get("self_id")) == "e0":
                    primary = s
                    break
            if primary is None:
                primary = sorted(snaps, key=lambda x: int(x.get("ts_ms", 0)), reverse=True)[0]
            adaptive = {
                "enabled": True,
                "probe_interval_s": primary.get("probe_interval_s", 0.5),
                "probe_timeout_s": primary.get("probe_timeout_s", 0.25),
                "ewma_alpha": primary.get("ewma_alpha", 0.25),
                "target_fanout": 2,
                "ranked_peers_real": primary.get("ranked_peers", []),
                "peers_real": primary.get("peers", []),
                "source_node": primary.get("self_id", "unknown"),
                "all_sources": all_sources,
            }
        else:
            adaptive = {"enabled": False}

    return {
        "phase": phase,
        "stage_label": stage_label,
        "nodes": nodes,
        "cloud_bytes": cloud_bytes,
        "edge_bytes": edge_bytes,
        "reconstructed_count": reconstructed_count,
        "expected_target": expected_target,
        "completion_reason": completion_reason,
        "node_state_available": node_state_ready_count > 0,
        "node_state_ready_count": node_state_ready_count,
        "adaptive": adaptive,
    }


async def _run_demo_job(job_id: str, req: DemoRunRequest) -> None:
    job = DEMO_JOBS[job_id]
    job["phase"] = "prepare"
    try:
        INPUTS_DIR.mkdir(parents=True, exist_ok=True)
        file_name = f"demo_{req.file_size_mib}MiB.bin"
        input_path = INPUTS_DIR / file_name
        if not input_path.exists():
            gen = await _run_cmd(
                [
                    sys.executable,
                    "-m",
                    "common.fake_video",
                    "one",
                    "--out",
                    str(input_path),
                    "--size-mib",
                    str(req.file_size_mib),
                ]
            )
            if gen["code"] != 0:
                raise RuntimeError(f"generate_input failed:\n{gen['output'][-2000:]}")

        method = req.method
        scenario = job["scenario"]
        metrics_dir = ROOT / "metrics" / method / scenario
        metrics_dir.mkdir(parents=True, exist_ok=True)
        job["expected_target"] = _expected_reconstruct_target(job)
        job["completion_reason"] = ""
        env = {
            "HOST_INPUT_DIR": str(INPUTS_DIR),
            "FILE_PATH": f"/inputs/{file_name}",
            "SCENARIO": scenario,
        }
        fault = job.get("fault_data") or _fault_env(req.fault_profile, req.fault_mode)
        env.update(fault["env"])
        job["env"] = env
        job["fault_plan"] = fault["plan"]

        job["phase"] = "compose_down"
        down = await _run_cmd(["docker", "compose", "down", "-v"], env=env)
        if down["code"] != 0:
            raise RuntimeError(f"compose down failed:\n{down['output'][-3000:]}")

        svc = _services_for_method(method)
        # Remove old recovered files so the UI does not think nodes reconstructed instantly.
        for idx in range(4):
            rec_dir = svc["recover_root"] / f"e{idx}"
            if rec_dir.exists():
                shutil.rmtree(rec_dir, ignore_errors=True)
        job["phase"] = "compose_up"
        up = await _run_cmd(["docker", "compose", "up", "-d", "--build", *svc["edges"]], env=env)
        if up["code"] != 0:
            raise RuntimeError(f"compose up failed:\n{up['output'][-4000:]}")

        job["phase"] = "run_cloud"
        run_cloud = await _run_cmd(["docker", "compose", "run", "--rm", svc["cloud"]], env=env)
        job["cloud_log_tail"] = run_cloud["output"][-6000:]
        if run_cloud["code"] != 0:
            raise RuntimeError(f"cloud run failed:\n{run_cloud['output'][-5000:]}")

        job["phase"] = "aggregate"
        agg = await _run_cmd(
            [
                sys.executable,
                "-m",
                "experiments.aggregate_jsonl_metrics",
                "--scenario",
                scenario,
                "--method",
                method,
                "--n",
                "4",
                "--k",
                "3" if method == "edgehydra" else "0",
                "--m",
                "4" if method == "edgehydra" else "0",
                "--file",
                str(input_path),
                "--jsonl-dir",
                str(metrics_dir),
                "--out",
                str(DEMO_CSV),
            ]
        )
        if agg["code"] != 0:
            raise RuntimeError(f"aggregate failed:\n{agg['output'][-3000:]}")

        rows = _read_metrics_csv(DEMO_CSV)
        job["result"] = rows[0] if rows else {}
        # Finalize by waiting for expected reconstruct target, with timeout.
        job["phase"] = "finalizing"
        expected_target = int(job.get("expected_target", 4))
        base_timeout_s = 60.0
        size_scale_s = max(0.0, float(req.file_size_mib - 60)) * 0.35
        timeout_s = base_timeout_s + size_scale_s
        end_t = time.time() + timeout_s
        # Backward-compat fallback: if node_state export is unavailable,
        # keep old grace-period behavior.
        fallback_grace_s = 8.0
        used_fallback = False
        no_state_polls = 0

        while time.time() < end_t:
            live = await _live_status(job)
            rec = int(live.get("reconstructed_count", 0))
            has_state = bool(live.get("node_state_available", False))
            if has_state and rec >= expected_target:
                job["completion_reason"] = "target_reached"
                break
            if not has_state:
                no_state_polls += 1
            else:
                no_state_polls = 0
            if no_state_polls >= 4:
                # No node_state after ~2s => use old grace-period behavior.
                used_fallback = True
                await asyncio.sleep(fallback_grace_s)
                break
            await asyncio.sleep(0.5)
        if not job.get("completion_reason"):
            if used_fallback:
                job["completion_reason"] = "timeout"
            else:
                live = await _live_status(job)
                rec = int(live.get("reconstructed_count", 0))
                job["completion_reason"] = "target_reached" if rec >= expected_target else "timeout"
        job["phase"] = "done"
        job["ok"] = True
    except Exception as e:
        job["phase"] = "error"
        job["ok"] = False
        job["error"] = str(e)
    finally:
        env = job.get("env", {})
        await _run_cmd(["docker", "compose", "down", "-v"], env=env)
        job["ended_at"] = time.time()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(ROOT / "webapp" / "static" / "index.html")


@app.get("/api/metrics")
async def get_metrics() -> Dict[str, Any]:
    rows = _read_metrics_csv(RESULTS_CSV)
    return {"rows": rows, "count": len(rows), "csv_path": str(RESULTS_CSV)}


@app.post("/api/run-compose")
async def run_compose_pipeline() -> Dict[str, Any]:
    """
    Runs the full compose scenario pipeline and merges metrics.
    This can take a few minutes.
    """
    script = ROOT / "scripts" / "run_compose_scenarios.sh"
    if not script.exists():
        raise HTTPException(status_code=404, detail="run_compose_scenarios.sh not found")

    proc1 = await asyncio.create_subprocess_exec(
        "bash",
        str(script),
        cwd=str(ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out1, _ = await proc1.communicate()
    text1 = out1.decode("utf-8", errors="replace")
    if proc1.returncode != 0:
        return {"ok": False, "step": "run_compose_scenarios", "returncode": proc1.returncode, "output": text1[-6000:]}

    proc2 = await asyncio.create_subprocess_exec(
        "python3",
        "-m",
        "experiments.merge_compose_csvs",
        "--in-dir",
        "results/compose",
        "--out",
        "results/compose/metrics_all.csv",
        cwd=str(ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out2, _ = await proc2.communicate()
    text2 = out2.decode("utf-8", errors="replace")
    if proc2.returncode != 0:
        return {"ok": False, "step": "merge_compose_csvs", "returncode": proc2.returncode, "output": text2[-6000:]}

    rows = _read_metrics_csv(RESULTS_CSV)
    return {"ok": True, "rows": len(rows), "run_output_tail": text1[-3000:], "merge_output": text2[-1500:]}


@app.post("/api/demo/run")
async def run_demo_once(req: DemoRunRequest) -> Dict[str, Any]:
    if req.method not in {"edgehydra", "edgedis"}:
        raise HTTPException(status_code=400, detail="method must be edgehydra or edgedis")
    if req.fault_profile not in {"normal", "slow", "failed"}:
        raise HTTPException(status_code=400, detail="fault_profile must be normal/slow/failed")
    if req.fault_mode not in {"paper", "random"}:
        raise HTTPException(status_code=400, detail="fault_mode must be paper/random")
    if req.file_size_mib <= 0 or req.file_size_mib > 144:
        raise HTTPException(status_code=400, detail="file_size_mib must be in range 1..144")

    active = [j for j in DEMO_JOBS.values() if j.get("phase") not in {"done", "error"}]
    if active:
        raise HTTPException(status_code=409, detail="A demo run is already in progress")
    scenario = f"demo_{req.method}_{req.fault_profile}_{req.file_size_mib}mib_{int(time.time())}"
    job_id = uuid.uuid4().hex[:10]
    fault = _fault_env(req.fault_profile, req.fault_mode)
    DEMO_JOBS[job_id] = {
        "id": job_id,
        "method": req.method,
        "fault_profile": req.fault_profile,
        "fault_mode": req.fault_mode,
        "file_size_mib": req.file_size_mib,
        "scenario": scenario,
        "phase": "queued",
        "ok": False,
        "result": {},
        "cloud_log_tail": "",
        "started_at": time.time(),
        "fault_plan": fault["plan"],
        "fault_data": fault,
    }
    asyncio.create_task(_run_demo_job(job_id, req))
    return {
        "ok": True,
        "job_id": job_id,
        "scenario": scenario,
        "fault_plan": DEMO_JOBS[job_id]["fault_plan"],
        "fault_mode": req.fault_mode,
    }


@app.get("/api/demo/status/{job_id}")
async def get_demo_status(job_id: str) -> Dict[str, Any]:
    job = DEMO_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    live = await _live_status(job)
    out = {
        "ok": True,
        "job_id": job_id,
        "phase": job.get("phase"),
        "scenario": job.get("scenario"),
        "error": job.get("error", ""),
        "result": job.get("result", {}),
        "cloud_log_tail": job.get("cloud_log_tail", ""),
        "fault_plan": job.get("fault_plan", {}),
        "fault_mode": str(job.get("fault_mode", "random")),
        "expected_target": int(job.get("expected_target", _expected_reconstruct_target(job))),
        "completion_reason": str(job.get("completion_reason", "")),
        "reconstructed_count": int(live.get("reconstructed_count", 0)),
        "elapsed_s": round(time.time() - float(job.get("started_at", time.time())), 2),
        "live": live,
    }
    return out


@app.post("/api/validate")
async def validate_metrics() -> Dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        "python3",
        "-m",
        "experiments.validate_metrics",
        "--csv",
        "results/compose/metrics_all.csv",
        cwd=str(ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    text = out.decode("utf-8", errors="replace")
    return {"ok": proc.returncode == 0, "returncode": proc.returncode, "output": text}

